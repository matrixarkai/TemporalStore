use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

pub mod golden;
pub mod reports;

mod constants;
mod object_manager;
mod set_index_serde;
mod slot_store;
mod state;

use self::constants::*;
use self::reports::*;
use self::state::*;
use crate::cache::{CacheEntryInfo, CacheGcReport, CacheKey, MultiLayerCache};
use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, Config, GetConfigResponse, GetInfoResponse, GetStatsResponse,
    LoadShardRequest, LoadShardResponse, MembershipUpdateRequest, ObjectManagerStats,
    PartitionInfoStats, ScanStreamRequest, ScanStreamResponse, SetConfigRequest, ShardInfo,
    ShardStats, StreamKind, StreamReadRequest, StreamReadResponse, StreamRecord,
    UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::oplog::LocalOplogStore;
use crate::page_store::{LocalPageStore, PageAddress, PageStoreError, PageStoreOptions};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ContextAuditRef,
    ContextChildRef, ContextCompressionEvent, ContextEmbedding, ContextEntity, ContextEvent,
    ContextExtractedEventIndexes, ContextIndexRef, ContextNode, ContextPackAudit, ContextSummary,
    ContextSummaryDirtyMarker, ContextTraversedNode, ContextWire, ExecuteRequest, ExecuteResponse,
    FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy, InternalContextIndex,
    IpsSnapshotReport, IpsStats, RiskFamily, RiskFolType, SequenceFeatureRow, SequenceQuerySpec,
    ShardId, Status, StringSetCondition,
};

#[derive(Debug, Clone)]
pub struct TemporalEngine {
    shards: Arc<RwLock<HashMap<ShardId, ShardState>>>,
    cache: MultiLayerCache,
    page_store: LocalPageStore,
    oplog_store: LocalOplogStore,
    index_log_store: LocalIndexLogStore,
    index_dir: PathBuf,
    configs: Arc<RwLock<HashMap<ShardId, Config>>>,
    infos: Arc<RwLock<HashMap<ShardId, ShardInfo>>>,
    admissions: Arc<RwLock<HashMap<AdmissionScope, AdmissionState>>>,
}

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
        let oplog_store = LocalOplogStore::new(index_dir.join("oplogs"));
        let index_log_store = LocalIndexLogStore::new(index_dir.join("indexlogs"));
        Self {
            shards: Arc::default(),
            cache,
            page_store,
            oplog_store,
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

    pub fn oplog_store(&self) -> LocalOplogStore {
        self.oplog_store.clone()
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
        let state = self.load_index(request.shard_id).unwrap_or_default();
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

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, None)
    }

    pub fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        self.execute_with_storage_override(request, Some(false))
    }

    fn execute_with_storage_override(
        &self,
        request: ExecuteRequest,
        async_storage_override: Option<bool>,
    ) -> ExecuteResponse {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            return ExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                response: CommandResponse::Empty,
            };
        };
        let command = request.command;
        if self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false)
            && is_write_command(&command)
        {
            return ExecuteResponse {
                status: Status::error("readonly_shard", "readonly shard rejects write command"),
                response: CommandResponse::Empty,
            };
        }
        let mut config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        if let Some(async_storage) = async_storage_override {
            config.async_storage = async_storage;
        }
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let write_command = is_write_command(&command);
        if let Err(status) = self.check_admission(request.shard_id, write_command, &config, &info) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        if write_command
            && config
                .maxmemory_bytes
                .map(|limit| self.page_store.stats().bytes_written >= limit)
                .unwrap_or(false)
        {
            return ExecuteResponse {
                status: Status::error(
                    "storage_quota_exceeded",
                    "shard maxmemory_bytes limit has been reached",
                ),
                response: CommandResponse::Empty,
            };
        }
        if let Err(status) = validate_command_preconditions(
            &self.cache,
            &self.page_store,
            request.shard_id,
            shard,
            &command,
        ) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        let outcome = execute_on_shard(
            &self.cache,
            &self.page_store,
            config.feature_max_size,
            config.async_storage,
            request.shard_id,
            info.as_ref()
                .map(|info| info.start_routing_slot)
                .unwrap_or_default(),
            info.as_ref()
                .map(|info| info.end_routing_slot)
                .unwrap_or(u32::MAX),
            shard,
            command.clone(),
        );
        if outcome.mutated {
            for object_key in command_object_keys(&command) {
                shard.dirty_objects.insert(object_key);
            }
            if !command_updates_slot_index_directly(&command) || shard.slot_index.slots.is_empty() {
                rebuild_slot_first_index(
                    shard,
                    info.as_ref()
                        .map(|info| info.start_routing_slot)
                        .unwrap_or_default(),
                    info.as_ref()
                        .map(|info| info.end_routing_slot)
                        .unwrap_or(u32::MAX),
                );
            }
            if write_command && !config.async_storage {
                let _ = self.oplog_store.append(request.shard_id, command);
            }
            if !config.async_storage {
                let index_bytes = serialize_index(shard);
                let _ = self
                    .index_log_store
                    .append_json(request.shard_id, &index_bytes);
                let _ = self.persist_index_bytes(request.shard_id, &index_bytes);
            }
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.execute(ExecuteRequest {
            shard_id: request.shard_id,
            command: request.command,
        });
        CheckedExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    fn check_admission(
        &self,
        shard_id: ShardId,
        write_command: bool,
        config: &Config,
        info: &Option<ShardInfo>,
    ) -> Result<(), Status> {
        let limits = admission_limits(shard_id, write_command, config, info);
        if limits.is_empty() {
            return Ok(());
        }
        let now_sec = now_epoch_seconds();
        let mut admissions = self.admissions.write().expect("admission lock poisoned");
        for limit in &limits {
            if limit.limit == 0 {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} is zero", limit.label),
                ));
            }
            let admission = admissions.entry(limit.scope.clone()).or_default();
            reset_admission_window(admission, now_sec);
            let count = admission_count(admission, write_command);
            if *count >= limit.limit {
                return Err(Status::error(
                    "admission_rejected",
                    format!("{} limit exceeded", limit.label),
                ));
            }
        }
        for limit in limits {
            let admission = admissions.entry(limit.scope).or_default();
            reset_admission_window(admission, now_sec);
            *admission_count(admission, write_command) += 1;
        }
        Ok(())
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        if !self.is_shard_loaded(request.shard_id) {
            return Status::error("shard_not_found", "shard is not loaded");
        }
        let mut configs = self.configs.write().expect("config lock poisoned");
        let current = configs.get(&request.shard_id).cloned().unwrap_or_default();
        if request.config.version < current.version {
            return Status::error("failed_precondition", "legacy config version");
        }
        if request.config.version == current.version {
            return Status::ok();
        }
        configs.insert(request.shard_id, request.config);
        Status::ok()
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
        if !self.is_shard_loaded(shard_id) {
            return GetConfigResponse {
                status: Status::error("shard_not_found", "shard is not loaded"),
                config: Config::default(),
            };
        }
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&shard_id)
            .cloned()
            .unwrap_or_default();
        GetConfigResponse {
            status: Status::ok(),
            config,
        }
    }

    fn is_shard_loaded(&self, shard_id: ShardId) -> bool {
        self.infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .map(|info| info.loaded)
            .unwrap_or(false)
    }

    pub fn get_info(&self, shard_id: ShardId) -> GetInfoResponse {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        GetInfoResponse {
            status: if info.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            info,
        }
    }

    pub fn update_membership(&self, request: MembershipUpdateRequest) -> Status {
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            if request.membership_version < info.membership_version {
                return Status::error("failed_precondition", "legacy membership info");
            }
            let global_update = request.membership_version > info.membership_version;
            if !global_update
                && request.replica_membership_version == info.replica_membership_version
            {
                return Status::ok();
            }
            if request.replica_membership_version < info.replica_membership_version {
                return Status::error("failed_precondition", "legacy membership unit info");
            }
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            info.membership_version = request.membership_version;
            info.replica_membership_version = request.replica_membership_version;
            info.membership_valid = info
                .local_node_id
                .map(|node_id| info.replica_node_ids.contains(&node_id))
                .unwrap_or(true);
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let stats = self.shard_stats(shard_id);
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn rust_storage_observation(&self, shard_id: ShardId) -> Option<RustStorageObservation> {
        self.shard_stats(shard_id)
            .map(|stats| RustStorageObservation {
                shard_id,
                observed_memory_hit: stats.cache.memory_hits > 0,
                observed_block_cache_hit: stats.cache.disk_hits > 0,
                observed_local_file_read: stats.page_store.reads > 0,
                observed_cache_invalidation: stats.cache.invalidations > 0,
                observed_memory_eviction: stats.cache.memory_evictions > 0,
                cache_memory_bytes: stats.cache.memory_bytes,
                cache_disk_bytes: stats.cache.disk_bytes,
                local_page_bytes_written: stats.page_store.bytes_written,
                local_page_bytes_read: stats.page_store.bytes_read,
                cache: stats.cache,
                page_store: stats.page_store,
            })
    }

    pub fn loaded_shard_stats(&self) -> Vec<ShardStats> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.shard_stats(shard_id))
            .collect()
    }

    pub fn loaded_shard_ids(&self) -> Vec<ShardId> {
        let mut shard_ids = self
            .shards
            .read()
            .expect("engine lock poisoned")
            .keys()
            .copied()
            .collect::<Vec<_>>();
        shard_ids.sort_unstable();
        shard_ids
    }

    pub fn slot_storage_summaries(&self, shard_id: ShardId) -> Vec<SlotStorageSummary> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return Vec::new();
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        let summaries = slot_storage_summaries(shard, start, end);
        if let Some(manifest) = latest_slot_dump_manifest_at(&self.index_dir, shard_id) {
            merge_last_dump_sequence(summaries, &manifest)
        } else {
            summaries
        }
    }

    pub fn storage_physical_index_report(&self, shard_id: ShardId) -> StoragePhysicalIndexReport {
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return StoragePhysicalIndexReport {
                shard_id,
                slot_first: true,
                ..StoragePhysicalIndexReport::default()
            };
        };
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        let summaries = slot_storage_summaries(shard, start, end);
        let summaries =
            if let Some(manifest) = latest_slot_dump_manifest_at(&self.index_dir, shard_id) {
                merge_last_dump_sequence(summaries, &manifest)
            } else {
                summaries
            };
        storage_physical_index_report(shard_id, shard, summaries)
    }

    pub fn routing_slot_for_key(&self, shard_id: ShardId, key: &str) -> u32 {
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&shard_id)
            .cloned();
        let start = info
            .as_ref()
            .map(|info| info.start_routing_slot)
            .unwrap_or_default();
        let end = info
            .as_ref()
            .map(|info| info.end_routing_slot)
            .unwrap_or(u32::MAX);
        page_routing_slot(key, start, end)
    }

    pub fn create_slot_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> Result<SlotDumpManifest, Status> {
        let selected_slots = selected_slots.into_iter().collect::<BTreeSet<_>>();
        let summaries = self.slot_storage_summaries(shard_id);
        if summaries.is_empty()
            && !self
                .shards
                .read()
                .expect("engine lock poisoned")
                .contains_key(&shard_id)
        {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        }
        let mut slot_summaries = summaries
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect::<Vec<_>>();
        slot_summaries.sort_by_key(|summary| summary.routing_slot);
        let mut page_segment_ids = slot_summaries
            .iter()
            .flat_map(|summary| summary.page_segment_ids.iter().copied())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        page_segment_ids.sort_unstable();
        let oplog_sequence = self.oplog_store.stats(shard_id).last_sequence;
        let index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let index_bytes = self
            .export_index_bytes(shard_id)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        let index_sha256 = sha256_hex_bytes(&index_bytes);
        let dump_index_state = serde_json::from_slice::<ShardState>(&index_bytes)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        let created_unix_ms = now_ms();
        let manifest_id = format!("{shard_id}-{index_log_sequence}-{created_unix_ms}");
        let parent_manifest_id = latest_slot_dump_manifest_at(&self.index_dir, shard_id)
            .map(|manifest| manifest.manifest_id);
        let object_lifecycle = storage_object_lifecycle_report_for_slots(
            shard_id,
            &dump_index_state,
            &selected_slots,
            |key| self.routing_slot_for_key(shard_id, key),
        );
        let mut manifest = SlotDumpManifest {
            version: 3,
            shard_id,
            manifest_id,
            manifest_kind: "slot_dump".to_string(),
            dump_generation_id: String::new(),
            source_manifest_ids: Vec::new(),
            parent_manifest_id,
            load_version_handoff: None,
            created_unix_ms,
            slot_ids: slot_summaries
                .iter()
                .map(|summary| summary.routing_slot)
                .collect(),
            page_segment_ids,
            oplog_sequence,
            index_log_sequence,
            live_page_refs: slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            logical_bytes: slot_summaries
                .iter()
                .map(|summary| summary.logical_bytes)
                .sum(),
            physical_bytes: slot_summaries
                .iter()
                .map(|summary| summary.physical_bytes)
                .sum(),
            slot_summaries,
            object_lifecycle,
            index_bytes,
            index_sha256,
            checksum: String::new(),
        };
        manifest.dump_generation_id = slot_dump_generation_id(&manifest);
        manifest.checksum = slot_dump_manifest_checksum(&manifest)?;
        self.persist_slot_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        Ok(manifest)
    }

    pub fn create_merged_slot_dump_manifest(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
        source_manifest_ids: impl IntoIterator<Item = String>,
        next_load_version: Option<u64>,
    ) -> Result<SlotDumpManifest, Status> {
        let mut manifest = self.create_slot_dump_manifest(shard_id, selected_slots)?;
        manifest.manifest_kind = "merged_slot_dump".to_string();
        manifest.source_manifest_ids = source_manifest_ids.into_iter().collect::<Vec<_>>();
        manifest.source_manifest_ids.sort();
        manifest.source_manifest_ids.dedup();
        if let Some(next_load_version) = next_load_version {
            let previous_load_version = self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&shard_id)
                .map(|info| info.load_version)
                .unwrap_or_default();
            manifest.load_version_handoff = Some(SlotDumpLoadVersionHandoff {
                previous_load_version,
                next_load_version,
                applied: false,
            });
        }
        manifest.checksum = slot_dump_manifest_checksum(&manifest)?;
        self.persist_slot_dump_manifest(&manifest)
            .map_err(|err| Status::error("slot_dump_failed", err.to_string()))?;
        Ok(manifest)
    }

    pub fn list_slot_dump_manifests(&self, shard_id: ShardId) -> Vec<SlotDumpManifest> {
        list_slot_dump_manifests_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn interrupted_slot_dump_installs(&self, shard_id: ShardId) -> Vec<SlotDumpInstallMarker> {
        interrupted_slot_dump_installs_at(&self.index_dir, shard_id).unwrap_or_default()
    }

    pub fn slot_dump_manifest_prune_plan(&self, shard_id: ShardId) -> SlotDumpManifestPrunePlan {
        self.slot_dump_manifest_prune_plan_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn slot_dump_manifest_prune_plan_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
    ) -> SlotDumpManifestPrunePlan {
        self.slot_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn slot_dump_manifest_prune_plan_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = SlotDumpRaftSnapshotRef>,
    ) -> SlotDumpManifestPrunePlan {
        let follower_cursors = follower_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        slot_dump_manifest_prune_plan_at(
            &self.index_dir,
            shard_id,
            &follower_cursors,
            &raft_snapshot_refs,
        )
        .unwrap_or_else(|err| SlotDumpManifestPrunePlan {
            shard_id,
            reasons: vec![format!("slot_dump_prune_plan_failed:{err}")],
            ..SlotDumpManifestPrunePlan::default()
        })
    }

    pub fn slot_dump_install_roll_forward_reports(
        &self,
        shard_id: ShardId,
    ) -> Vec<SlotDumpInstallRollForwardReport> {
        self.interrupted_slot_dump_installs(shard_id)
            .into_iter()
            .map(|marker| self.slot_dump_install_roll_forward_report(&marker))
            .collect()
    }

    pub fn roll_forward_slot_dump_installs(
        &self,
        shard_id: ShardId,
    ) -> Vec<SlotDumpInstallRollForwardReport> {
        self.interrupted_slot_dump_installs(shard_id)
            .into_iter()
            .map(|marker| {
                let mut report = self.slot_dump_install_roll_forward_report(&marker);
                if report.can_retry_install {
                    match slot_dump_manifest_at(
                        &self.index_dir,
                        marker.shard_id,
                        &marker.manifest_id,
                    )
                    .ok()
                    .flatten()
                    .map(|manifest| self.install_slot_dump_manifest(&manifest))
                    {
                        Some(Ok(())) => {
                            report.completed_install = true;
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_slot_dump_install_markers(
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
                    match self.persist_slot_dump_install_marker_by_fields(
                        marker.shard_id,
                        &marker.manifest_id,
                        "commit",
                        marker.oplog_sequence,
                        marker.index_log_sequence,
                    ) {
                        Ok(()) => {
                            report.completed_commit = true;
                            report.obsolete_marker_files_removed =
                                remove_obsolete_slot_dump_install_markers(
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

    pub fn apply_slot_dump_manifest_prune(&self, shard_id: ShardId) -> SlotDumpManifestPruneReport {
        self.apply_slot_dump_manifest_prune_with_follower_cursors(shard_id, Vec::new())
    }

    pub fn apply_slot_dump_manifest_prune_with_follower_cursors(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
    ) -> SlotDumpManifestPruneReport {
        self.apply_slot_dump_manifest_prune_with_retention_refs(
            shard_id,
            follower_cursors,
            Vec::new(),
        )
    }

    pub fn apply_slot_dump_manifest_prune_with_retention_refs(
        &self,
        shard_id: ShardId,
        follower_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = SlotDumpRaftSnapshotRef>,
    ) -> SlotDumpManifestPruneReport {
        let plan = self.slot_dump_manifest_prune_plan_with_retention_refs(
            shard_id,
            follower_cursors,
            raft_snapshot_refs,
        );
        let mut removed_manifest_ids = Vec::new();
        for manifest_id in &plan.prunable_manifest_ids {
            let path = slot_dump_manifest_path(&self.index_dir, shard_id, manifest_id);
            if fs::remove_file(path).is_ok() {
                removed_manifest_ids.push(manifest_id.clone());
            }
        }
        let mut removed_marker_files = 0usize;
        if let Ok(marker_files) = slot_dump_install_marker_files_at(&self.index_dir, shard_id) {
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
        SlotDumpManifestPruneReport {
            shard_id,
            plan,
            removed_manifest_ids,
            removed_marker_files,
        }
    }

    fn slot_dump_install_roll_forward_report(
        &self,
        marker: &SlotDumpInstallMarker,
    ) -> SlotDumpInstallRollForwardReport {
        if marker.phase != "install" && marker.phase != "prepare" {
            return SlotDumpInstallRollForwardReport {
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
            slot_dump_manifest_at(&self.index_dir, marker.shard_id, &marker.manifest_id)
                .ok()
                .flatten()
        else {
            return SlotDumpInstallRollForwardReport {
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
        let reason = match self.validate_slot_dump_manifest(&manifest) {
            Ok(()) if marker.phase == "install" => "commit_ready".to_string(),
            Ok(()) => "install_retry_ready".to_string(),
            Err(status) => format!("manifest_invalid:{}", status.code),
        };
        SlotDumpInstallRollForwardReport {
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

    pub fn validate_slot_dump_manifest(&self, manifest: &SlotDumpManifest) -> Result<(), Status> {
        let expected = slot_dump_manifest_checksum(manifest)
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
        let existing_segments = self
            .page_store
            .segment_ids()
            .map_err(|err| Status::error("slot_dump_invalid", err.to_string()))?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing = manifest
            .page_segment_ids
            .iter()
            .copied()
            .filter(|id| !existing_segments.contains(id))
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Err(Status::error(
                "slot_dump_missing_page_segments",
                format!("slot dump references missing page segments: {missing:?}"),
            ));
        }
        let restored = serde_json::from_slice::<ShardState>(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        if manifest_slots.len() != manifest.slot_ids.len()
            || manifest.slot_ids != manifest_slots.iter().copied().collect::<Vec<_>>()
        {
            return Err(Status::error(
                "slot_dump_slot_ids_not_canonical",
                "slot dump manifest slot ids must be sorted and unique",
            ));
        }
        let canonical_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if manifest.page_segment_ids != canonical_page_segment_ids {
            return Err(Status::error(
                "slot_dump_page_segment_ids_not_canonical",
                "slot dump manifest page segment ids must be sorted and unique",
            ));
        }
        let live_page_entries = collect_live_page_entries(&restored)
            .into_iter()
            .filter(|entry| {
                let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
                    self.routing_slot_for_key(manifest.shard_id, &entry.object_key)
                });
                manifest_slots.is_empty() || manifest_slots.contains(&routing_slot)
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
        let expected_slot_summaries =
            slot_dump_manifest_comparable_summaries(&restored, &manifest_slots);
        let actual_slot_summaries = comparable_slot_dump_summaries(manifest.slot_summaries.clone());
        if actual_slot_summaries != expected_slot_summaries {
            return Err(Status::error(
                "slot_dump_slot_summary_mismatch",
                format!(
                    "slot dump slot summaries do not match restored index page ownership: manifest={actual_slot_summaries:?} restored={expected_slot_summaries:?}"
                ),
            ));
        }
        let expected_logical_bytes = expected_slot_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let expected_physical_bytes = expected_slot_summaries
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
            let expected_object_lifecycle = storage_object_lifecycle_report_for_slots(
                manifest.shard_id,
                &restored,
                &manifest_slots,
                |key| self.routing_slot_for_key(manifest.shard_id, key),
            );
            if manifest.object_lifecycle != expected_object_lifecycle {
                return Err(Status::error(
                    "slot_dump_object_lifecycle_mismatch",
                    "slot dump object lifecycle metadata does not match restored index",
                ));
            }
        }
        let referenced_page_segment_ids = live_page_entries
            .iter()
            .map(|entry| entry.address.page_segment_id)
            .collect::<BTreeSet<_>>();
        let manifest_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if referenced_page_segment_ids != manifest_page_segment_ids {
            return Err(Status::error(
                "slot_dump_page_segment_mismatch",
                format!(
                    "slot dump page segment ids {manifest_page_segment_ids:?} do not match live refs {referenced_page_segment_ids:?}"
                ),
            ));
        }
        if !manifest.dump_generation_id.is_empty()
            && manifest.dump_generation_id != slot_dump_generation_id(manifest)
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

    pub fn slot_dump_install_preflight_report(
        &self,
        manifest: &SlotDumpManifest,
    ) -> SlotDumpInstallPreflightReport {
        let current_oplog_sequence = self.oplog_store.stats(manifest.shard_id).last_sequence;
        let current_index_log_sequence =
            self.index_log_store.stats(manifest.shard_id).last_sequence;
        let existing_segments = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let missing_page_segment_ids = manifest
            .page_segment_ids
            .iter()
            .copied()
            .filter(|id| !existing_segments.contains(id))
            .collect::<Vec<_>>();
        let corrupt_page_segment_ids = self
            .page_store
            .segment_reports()
            .unwrap_or_default()
            .into_iter()
            .filter(|report| {
                report.has_corruption && manifest.page_segment_ids.contains(&report.page_segment_id)
            })
            .map(|report| report.page_segment_id)
            .collect::<Vec<_>>();
        let stale_manifest = current_index_log_sequence > manifest.index_log_sequence;
        let mut blockers = Vec::new();
        if stale_manifest {
            blockers.push("stale_manifest_sequence".to_string());
        }
        if !missing_page_segment_ids.is_empty() {
            blockers.push("missing_page_segments".to_string());
        }
        if !corrupt_page_segment_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }

        let mut unreadable_page_ref_count = 0usize;
        let mut unreadable_page_bytes = 0u64;
        let mut restored_index = None;
        if !manifest.index_bytes.is_empty() && missing_page_segment_ids.is_empty() {
            if let Ok(restored) = serde_json::from_slice::<ShardState>(&manifest.index_bytes) {
                let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
                for entry in collect_live_page_entries(&restored) {
                    let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
                        self.routing_slot_for_key(manifest.shard_id, &entry.object_key)
                    });
                    if manifest_slots.is_empty() || manifest_slots.contains(&routing_slot) {
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
            .map(|restored| self.slot_dump_stale_conflict_report(manifest, restored))
            .unwrap_or_default();
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

        SlotDumpInstallPreflightReport {
            shard_id: manifest.shard_id,
            manifest_id: manifest.manifest_id.clone(),
            install_safe: blockers.is_empty(),
            blockers,
            current_oplog_sequence,
            current_index_log_sequence,
            manifest_oplog_sequence: manifest.oplog_sequence,
            manifest_index_log_sequence: manifest.index_log_sequence,
            missing_page_segment_ids,
            corrupt_page_segment_ids,
            unreadable_page_ref_count,
            unreadable_page_bytes,
            stale_manifest,
            stale_object_conflict_count: stale_object_conflicts.len(),
            stale_page_conflict_count: stale_page_conflicts.len(),
            stale_object_conflicts,
            stale_page_conflicts,
        }
    }

    fn slot_dump_stale_conflict_report(
        &self,
        manifest: &SlotDumpManifest,
        restored: &ShardState,
    ) -> (Vec<String>, Vec<String>) {
        let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        let manifest_entries =
            slot_dump_entries_by_key(manifest.shard_id, restored, &manifest_slots, |key| {
                self.routing_slot_for_key(manifest.shard_id, key)
            });
        let current_entries = {
            let shards = self.shards.read().expect("engine lock poisoned");
            let Some(current) = shards.get(&manifest.shard_id) else {
                return (Vec::new(), Vec::new());
            };
            slot_dump_entries_by_key(manifest.shard_id, current, &manifest_slots, |key| {
                self.routing_slot_for_key(manifest.shard_id, key)
            })
        };
        let mut stale_object_conflicts = Vec::new();
        let mut stale_page_conflicts = Vec::new();
        for key in manifest_entries.keys().chain(current_entries.keys()) {
            match (manifest_entries.get(key), current_entries.get(key)) {
                (Some(manifest_address), Some(current_address)) => {
                    if manifest_address != current_address {
                        stale_page_conflicts.push(key.clone());
                    }
                }
                (Some(_), None) | (None, Some(_)) => stale_object_conflicts.push(key.clone()),
                (None, None) => {}
            }
        }
        stale_object_conflicts.sort();
        stale_object_conflicts.dedup();
        stale_page_conflicts.sort();
        stale_page_conflicts.dedup();
        (stale_object_conflicts, stale_page_conflicts)
    }

    pub fn slot_dump_fault_matrix_report(&self, shard_id: ShardId) -> SlotDumpFaultMatrixReport {
        let manifest = match self.create_slot_dump_manifest(shard_id, Vec::new()) {
            Ok(manifest) => manifest,
            Err(status) => {
                let scenario = SlotDumpFaultScenarioReport {
                    scenario: "create_manifest".to_string(),
                    passed: false,
                    expected_code: "ok".to_string(),
                    actual_code: status.code,
                    blockers: Vec::new(),
                    install_safe: false,
                };
                return SlotDumpFaultMatrixReport {
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
            .validate_slot_dump_manifest(&checksum_mismatch)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(slot_dump_fault_scenario(
            "checksum_mismatch",
            "slot_dump_checksum_mismatch",
            checksum_code,
            Vec::new(),
            false,
        ));

        let mut partial = manifest.clone();
        partial.index_bytes.clear();
        partial.checksum =
            slot_dump_manifest_checksum(&partial).unwrap_or_else(|err| err.code.clone());
        let partial_code = self
            .install_slot_dump_manifest(&partial)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(slot_dump_fault_scenario(
            "partial_manifest",
            "slot_dump_partial_manifest",
            partial_code,
            Vec::new(),
            false,
        ));

        let mut missing = manifest.clone();
        let missing_segment_id = missing
            .page_segment_ids
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            .saturating_add(1_000_000);
        missing.page_segment_ids.push(missing_segment_id);
        missing.page_segment_ids.sort_unstable();
        missing.checksum =
            slot_dump_manifest_checksum(&missing).unwrap_or_else(|err| err.code.clone());
        let missing_preflight = self.slot_dump_install_preflight_report(&missing);
        let missing_code = self
            .validate_slot_dump_manifest(&missing)
            .err()
            .map(|status| status.code)
            .unwrap_or_else(|| "ok".to_string());
        scenarios.push(slot_dump_fault_scenario(
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
            let stale_preflight = self.slot_dump_install_preflight_report(&manifest);
            stale_install_safe = stale_preflight.install_safe;
            stale_blockers = stale_preflight.blockers;
            stale_code = self
                .install_slot_dump_manifest(&manifest)
                .err()
                .map(|status| status.code)
                .unwrap_or_else(|| "ok".to_string());
        }
        scenarios.push(slot_dump_fault_scenario(
            "stale_manifest",
            "slot_dump_stale_manifest",
            stale_code,
            stale_blockers,
            stale_install_safe,
        ));

        let restart_code;
        let mut restart_blockers = Vec::new();
        let mut restart_install_safe = false;
        match self.create_slot_dump_manifest(shard_id, Vec::new()) {
            Ok(restart_manifest) => {
                match self.persist_slot_dump_install_marker_by_fields(
                    restart_manifest.shard_id,
                    &restart_manifest.manifest_id,
                    "prepare",
                    restart_manifest.oplog_sequence,
                    restart_manifest.index_log_sequence,
                ) {
                    Ok(()) => {
                        let before = self.slot_dump_install_roll_forward_reports(shard_id);
                        let applied = self.roll_forward_slot_dump_installs(shard_id);
                        let interrupted_after = self.interrupted_slot_dump_installs(shard_id);
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
        scenarios.push(slot_dump_fault_scenario(
            "restart_during_install_roll_forward",
            "slot_dump_restart_roll_forward",
            restart_code,
            restart_blockers,
            restart_install_safe,
        ));

        let mut corrupt_code = "not_run".to_string();
        let mut corrupt_blockers = Vec::new();
        let mut corrupt_install_safe = false;
        if let Some(segment_id) = manifest.page_segment_ids.first().copied() {
            match self.page_store.read_segment(segment_id) {
                Ok(mut segment) if !segment.is_empty() => {
                    if let Some(last) = segment.last_mut() {
                        *last ^= 0xff;
                    }
                    match self.page_store.install_segment(segment_id, &segment) {
                        Ok(()) => {
                            let corrupt_preflight =
                                self.slot_dump_install_preflight_report(&manifest);
                            corrupt_install_safe = corrupt_preflight.install_safe;
                            corrupt_blockers = corrupt_preflight.blockers;
                            corrupt_code = if corrupt_blockers
                                .iter()
                                .any(|blocker| blocker == "corrupt_page_segments")
                            {
                                "corrupt_page_segments".to_string()
                            } else {
                                self.validate_slot_dump_manifest(&manifest)
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
        scenarios.push(slot_dump_fault_scenario(
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
        SlotDumpFaultMatrixReport {
            shard_id,
            manifest_id: manifest.manifest_id,
            production_ready_slice: failed_scenarios.is_empty(),
            scenario_count: scenarios.len(),
            passed_count,
            failed_scenarios,
            scenarios,
        }
    }

    pub fn install_slot_dump_manifest(&self, manifest: &SlotDumpManifest) -> Result<(), Status> {
        self.validate_slot_dump_manifest(manifest)?;
        let preflight = self.slot_dump_install_preflight_report(manifest);
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
        self.validate_slot_dump_generation_for_install(manifest)?;
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
        let restored = serde_json::from_slice::<ShardState>(&manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_invalid_index", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "prepare")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_index_bytes(manifest.shard_id, &manifest.index_bytes)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "install")
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
        self.persist_slot_dump_manifest(manifest)
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        self.persist_slot_dump_install_marker(manifest, "commit")
            .map_err(|err| Status::error("slot_dump_install_failed", err.to_string()))?;
        Ok(())
    }

    pub fn install_merged_slot_dump_manifest(
        &self,
        manifest: &SlotDumpManifest,
    ) -> SlotDumpMergedInstallReport {
        let preflight = self.slot_dump_install_preflight_report(manifest);
        let rollback_marker_written = self
            .persist_slot_dump_install_marker(manifest, "rollback")
            .is_ok();
        if !preflight.install_safe {
            return SlotDumpMergedInstallReport {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                source_manifest_ids: manifest.source_manifest_ids.clone(),
                slot_ids: manifest.slot_ids.clone(),
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
        match self.install_slot_dump_manifest(manifest) {
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
                SlotDumpMergedInstallReport {
                    shard_id: manifest.shard_id,
                    manifest_id: manifest.manifest_id.clone(),
                    source_manifest_ids: manifest.source_manifest_ids.clone(),
                    slot_ids: manifest.slot_ids.clone(),
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
            Err(status) => SlotDumpMergedInstallReport {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                source_manifest_ids: manifest.source_manifest_ids.clone(),
                slot_ids: manifest.slot_ids.clone(),
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
        let current_oplog_sequence = self.oplog_store.stats(request.shard_id).last_sequence;
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
        StorageLifecycleReport {
            shard_id: request.shard_id,
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
        }
    }

    pub fn storage_wal_reclaim_plan(
        &self,
        shard_id: ShardId,
        follower_replay_cursors: impl IntoIterator<Item = SlotDumpFollowerReplayCursor>,
        raft_snapshot_refs: impl IntoIterator<Item = SlotDumpRaftSnapshotRef>,
    ) -> StorageWalReclaimPlan {
        let follower_replay_cursors = follower_replay_cursors.into_iter().collect::<Vec<_>>();
        let raft_snapshot_refs = raft_snapshot_refs.into_iter().collect::<Vec<_>>();
        let current_oplog_sequence = self.oplog_store.stats(shard_id).last_sequence;
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

        let mut follower_cursor_block_count = 0usize;
        for cursor in follower_replay_cursors
            .iter()
            .filter(|cursor| cursor.shard_id == shard_id)
        {
            follower_cursor_block_count = follower_cursor_block_count.saturating_add(1);
            durable_oplog_frontier = durable_oplog_frontier.min(cursor.oplog_sequence);
            durable_index_log_frontier = durable_index_log_frontier.min(cursor.index_log_sequence);
            blocker_reasons.push(format!(
                "follower_cursor_retains_logs:{}",
                cursor.follower_id
            ));
        }

        let mut raft_snapshot_block_count = 0usize;
        for snapshot in raft_snapshot_refs
            .iter()
            .filter(|snapshot| snapshot.shard_id == shard_id)
        {
            raft_snapshot_block_count = raft_snapshot_block_count.saturating_add(1);
            durable_oplog_frontier = durable_oplog_frontier.min(snapshot.oplog_sequence);
            durable_index_log_frontier =
                durable_index_log_frontier.min(snapshot.index_log_sequence);
            blocker_reasons.push(format!(
                "raft_snapshot_retains_logs:{}",
                snapshot.snapshot_id
            ));
        }

        if durable_oplog_frontier == u64::MAX {
            durable_oplog_frontier = 0;
        }
        if durable_index_log_frontier == u64::MAX {
            durable_index_log_frontier = 0;
        }
        let safe_to_reclaim = missing_slot_generations.is_empty()
            && covered_slot_count == slot_summaries.len()
            && durable_oplog_frontier > 0
            && durable_index_log_frontier > 0;
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
            .oplog_store
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

    fn storage_index_gc_report(
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

    pub fn run_storage_manager_cycle(
        &self,
        request: StorageManagerCycleRequest,
    ) -> StorageManagerCycleReport {
        let cycle_started_unix_ms = now_ms();
        let cxx_stage_order = [
            "prepare",
            "reclaim_oplog",
            "expire",
            "evict",
            "reclaim_page",
            "index_gc",
            "compact",
            "reap_metrics",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let plan_request = StorageLifecycleRequest {
            shard_id: request.shard_id,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: request.max_dump_slots_per_round,
            min_undumped_oplog_records: if request.enable_oplog_reclaim {
                request.min_undumped_oplog_records
            } else {
                u64::MAX
            },
            purge_delayed_destroy: request.enable_page_reclaim,
            prune_slot_dump_manifests: request.enable_index_gc,
            roll_forward_slot_dump_installs: request.enable_index_gc,
            follower_replay_cursors: request.follower_replay_cursors.clone(),
            page_gc_shared_store_cursors: request.page_gc_shared_store_cursors.clone(),
            page_gc_raft_snapshot_refs: request.raft_snapshot_refs.clone(),
            page_gc_checkpoint_floor_segment_id: request.page_gc_checkpoint_floor_segment_id,
            page_gc_raft_install_floor_segment_id: request.page_gc_raft_install_floor_segment_id,
            page_gc_delayed_destroy_grace_ms: request.page_gc_delayed_destroy_grace_ms,
            invalidate_cache: false,
            warm_cache: request.warm_cache,
        };
        let plan = self.storage_lifecycle_plan(plan_request.clone());
        let page_gc_dependency_plan = self.storage_page_gc_dependency_plan(
            request.shard_id,
            plan.reclaim_candidates
                .iter()
                .map(|candidate| candidate.page_segment_id),
            request.page_gc_shared_store_cursors.clone(),
            request.raft_snapshot_refs.clone(),
            request.page_gc_checkpoint_floor_segment_id,
            request.page_gc_raft_install_floor_segment_id,
            request.page_gc_delayed_destroy_grace_ms,
        );
        let slot_logical_bytes = plan
            .slot_summaries
            .iter()
            .map(|summary| summary.logical_bytes)
            .sum::<u64>();
        let slot_physical_bytes = plan
            .slot_summaries
            .iter()
            .map(|summary| summary.physical_bytes)
            .sum::<u64>();
        let reclaim_live_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.live_physical_bytes)
            .sum::<u64>();
        let reclaim_stale_bytes = plan
            .reclaim_candidates
            .iter()
            .map(|candidate| candidate.stale_physical_bytes)
            .sum::<u64>();
        let reclaim_candidate_count = plan.reclaim_candidates.len();
        let reclaim_skipped_count = plan
            .stale_page_segment_ids
            .len()
            .saturating_sub(reclaim_candidate_count);
        let log_pressure = self.storage_log_compatibility_report(request.shard_id);
        let cache_pressure = self.storage_cache_inspection_report(request.shard_id);
        let page_segment_total_bytes = reclaim_live_bytes.saturating_add(reclaim_stale_bytes);
        let page_segment_stale_density_basis_points = if page_segment_total_bytes == 0 {
            0
        } else {
            reclaim_stale_bytes.saturating_mul(10_000) / page_segment_total_bytes
        };
        let delayed_destroy_segment_count = plan.delayed_destroy_page_segment_ids.len();
        let delayed_destroy_bytes = plan
            .reclaim_candidates
            .iter()
            .filter(|candidate| candidate.reason == "delayed_destroy")
            .map(|candidate| candidate.physical_bytes)
            .sum::<u64>();
        let expired_slot_object_scan_debt = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| shard.expires_at_ms.len())
            .unwrap_or_default();
        let compaction_utility = self
            .shards
            .read()
            .expect("shards lock poisoned")
            .get(&request.shard_id)
            .map(|shard| compaction_utility_report(&self.page_store, shard))
            .unwrap_or_default();
        let compaction_debt_model_count = compaction_utility
            .model_policies
            .iter()
            .filter(|policy| {
                policy.stale_page_estimate > 0
                    || policy.stale_density_basis_points > 0
                    || policy.tombstone_density_basis_points > 0
            })
            .count()
            .max(usize::from(page_segment_stale_density_basis_points > 0));
        let compaction_debt_score = compaction_utility
            .model_policies
            .iter()
            .map(|policy| {
                policy
                    .stale_page_estimate
                    .saturating_add(policy.stale_density_basis_points)
                    .saturating_add(policy.tombstone_density_basis_points)
            })
            .sum::<u64>()
            .saturating_add(compaction_utility.stale_page_estimate)
            .saturating_add(reclaim_stale_bytes)
            .saturating_add(page_segment_stale_density_basis_points);
        let retention_prune_plan = self.slot_dump_manifest_prune_plan_with_retention_refs(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let manifest_retention_blockers = retention_prune_plan
            .follower_blocks
            .len()
            .saturating_add(retention_prune_plan.raft_snapshot_blocks.len());
        let memory_cache_pressure_score = cache_pressure
            .stats
            .memory_bytes
            .saturating_add(cache_pressure.stats.pinned_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_bytes)
            .saturating_add(cache_pressure.stats.async_writeback_queue_depth);
        let total_pressure_score = plan
            .dirty_slots
            .len()
            .saturating_add(expired_slot_object_scan_debt)
            .saturating_add(delayed_destroy_segment_count)
            .saturating_add(compaction_debt_model_count) as u64
            + plan.undumped_oplog_records
            + log_pressure.oplog_bytes
            + log_pressure.index_log_bytes
            + reclaim_stale_bytes
            + cache_pressure.stats.disk_bytes
            + memory_cache_pressure_score
            + delayed_destroy_bytes
            + manifest_retention_blockers as u64
            + compaction_debt_score;
        let pressure_signals = StorageManagerPressureSignals {
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_wal_records: plan.undumped_oplog_records,
            wal_bytes: log_pressure.oplog_bytes,
            index_log_bytes: log_pressure.index_log_bytes,
            stale_page_bytes: reclaim_stale_bytes,
            live_page_bytes: reclaim_live_bytes,
            page_segment_stale_density_basis_points,
            memory_cache_bytes: cache_pressure.stats.memory_bytes,
            disk_cache_bytes: cache_pressure.stats.disk_bytes,
            memory_cache_pressure_score,
            expired_slot_object_scan_debt,
            delayed_destroy_segment_count,
            delayed_destroy_bytes,
            follower_cursor_retention_blockers: retention_prune_plan.follower_blocks.len(),
            raft_snapshot_retention_blockers: retention_prune_plan.raft_snapshot_blocks.len(),
            compaction_debt_model_count,
            compaction_debt_score,
            total_pressure_score,
        };
        let mut stages = Vec::new();
        let mut errors = Vec::new();
        stages.push(StorageManagerStageReport {
            stage: "prepare".to_string(),
            enabled: request.enable_prepare,
            applied: request.enable_prepare && !request.dry_run,
            skipped: !request.enable_prepare,
            reason: if request.enable_prepare {
                "prepared storage lifecycle pressure view and index/page-store metadata".to_string()
            } else {
                "prepare disabled".to_string()
            },
            selected_page_segment_ids: plan.live_page_segment_ids.clone(),
            pressure_signal:
                "dirty_slots+wal_bytes+index_log_bytes+stale_density+cache_pressure+expire_debt+delayed_destroy+retention_blockers+model_compaction_debt"
                    .to_string(),
            pressure_score: pressure_signals.total_pressure_score,
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.total_pressure_score > 0,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count,
            before_bytes: slot_physical_bytes,
            after_bytes: slot_physical_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_oplog_records: plan.undumped_oplog_records,
            metrics_slot_count: plan.slot_summaries.len(),
            metrics_page_ref_count: plan
                .slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            ..StorageManagerStageReport::default()
        });

        let mut expiry_report = None;
        if request.enable_expire && !request.dry_run {
            match self.sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: request.shard_id,
                hot_cursor: request.expire_hot_cursor.clone(),
                cold_cursor: request.expire_cold_cursor.clone(),
                max_hot_slots_per_round: request.max_expire_hot_slots_per_round,
                max_cold_slots_per_round: request.max_expire_cold_slots_per_round,
                load_cold_slots: request.load_cold_slots_for_expire,
            }) {
                Ok(report) => expiry_report = Some(report),
                Err(err) => errors.push(format!("expire: {}", err.message)),
            }
        }

        if request.enable_page_reclaim
            && !request.dry_run
            && !plan.reclaim_candidates.is_empty()
            && page_gc_dependency_plan.safe_to_reclaim
        {
            let retain_from_page_segment_id = plan
                .reclaim_candidates
                .iter()
                .map(|candidate| candidate.page_segment_id)
                .max()
                .unwrap_or_default()
                .saturating_add(1);
            if let Err(err) = self.page_store.gc_segments_before_with_live_refs_utility(
                retain_from_page_segment_id,
                plan.live_page_segment_ids.clone(),
                plan.reclaim_candidates.len(),
                true,
            ) {
                errors.push(format!("reclaim_page: {err}"));
            }
        }

        let lifecycle_report = if request.dry_run {
            None
        } else {
            let mut lifecycle_request = plan_request;
            lifecycle_request.purge_delayed_destroy =
                lifecycle_request.purge_delayed_destroy && page_gc_dependency_plan.safe_to_reclaim;
            Some(self.apply_storage_lifecycle(lifecycle_request))
        };
        let eviction_report = if request.enable_evict {
            Some(if request.dry_run {
                StorageEvictionReport {
                    shard_id: request.shard_id,
                    mode: if request.eviction_delete_drop {
                        "delete_drop"
                    } else {
                        "evict_cache"
                    }
                    .to_string(),
                    pressure_before: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    pressure_after: pressure_signals
                        .memory_cache_pressure_score
                        .saturating_add(pressure_signals.disk_cache_bytes),
                    memory_pressure_threshold: request.eviction_memory_pressure_threshold,
                    batch_limit: request.eviction_batch_limit,
                    dump_before_evict: request.eviction_dump_before_evict,
                    skipped_reason: "dry_run".to_string(),
                    ..StorageEvictionReport::default()
                }
            } else {
                self.apply_storage_eviction(
                    request.shard_id,
                    request.eviction_memory_pressure_threshold,
                    request.eviction_batch_limit,
                    request.eviction_dump_before_evict,
                    request.eviction_delete_drop,
                )
            })
        } else {
            None
        };
        let wal_reclaim_plan = self.storage_wal_reclaim_plan(
            request.shard_id,
            request.follower_replay_cursors.clone(),
            request.raft_snapshot_refs.clone(),
        );
        let wal_reclaim_report = if request.enable_oplog_reclaim {
            Some(if request.dry_run {
                StorageWalReclaimReport {
                    plan: wal_reclaim_plan.clone(),
                    applied: false,
                    ..StorageWalReclaimReport::default()
                }
            } else {
                self.apply_storage_wal_reclaim(wal_reclaim_plan.clone())
            })
        } else {
            None
        };
        let index_gc_report = Some(self.storage_index_gc_report(
            &plan,
            &wal_reclaim_plan,
            lifecycle_report.as_ref(),
            &request,
        ));

        stages.push(StorageManagerStageReport {
            stage: "reclaim_oplog".to_string(),
            enabled: request.enable_oplog_reclaim,
            applied: wal_reclaim_report
                .as_ref()
                .map(|report| report.applied)
                .unwrap_or(false),
            skipped: !request.enable_oplog_reclaim
                || plan.dump_delayed
                || wal_reclaim_report
                    .as_ref()
                    .map(|report| !report.plan.safe_to_reclaim)
                    .unwrap_or(true),
            reason: if !request.enable_oplog_reclaim {
                "oplog reclaim disabled".to_string()
            } else if plan.dump_delayed {
                "dirty slot dump delayed until the configured undumped log threshold is reached"
                    .to_string()
            } else if wal_reclaim_report
                .as_ref()
                .map(|report| !report.plan.safe_to_reclaim)
                .unwrap_or(true)
            {
                format!(
                    "WAL/index-log reclaim blocked until durable slot generations and retention cursors allow it: {}",
                    wal_reclaim_report
                        .as_ref()
                        .map(|report| report.plan.blocker_reasons.join(","))
                        .unwrap_or_default()
                )
            } else {
                "reclaimed WAL/index-log through the slot-generation durable dump frontier"
                    .to_string()
            },
            selected_slots: plan.selected_dump_slots.clone(),
            pressure_signal:
                "durable_slot_generation_frontier+follower_snapshot_retention+wal_bytes+index_log_bytes"
                    .to_string(),
            pressure_score: pressure_signals
                .undumped_wal_records
                .saturating_add(pressure_signals.wal_bytes)
                .saturating_add(pressure_signals.index_log_bytes),
            pressure_threshold: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_oplog_sequence)
                .unwrap_or(request.min_undumped_oplog_records),
            pressure_triggered: request.enable_oplog_reclaim
                && wal_reclaim_report
                    .as_ref()
                    .map(|report| report.plan.safe_to_reclaim)
                    .unwrap_or(false),
            candidate_count: plan.dirty_slots.len(),
            skipped_count: plan
                .dirty_slots
                .len()
                .saturating_sub(plan.selected_dump_slots.len()),
            before_bytes: slot_logical_bytes,
            after_bytes: slot_logical_bytes,
            live_bytes: slot_logical_bytes,
            dirty_slot_count: plan.dirty_slots.len(),
            undumped_oplog_records: plan.undumped_oplog_records,
            dumped_slot_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.dump_manifest.as_ref())
                .map(|manifest| manifest.slot_ids.len())
                .unwrap_or_default(),
            wal_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.oplog_records_removed)
                .unwrap_or_default(),
            index_log_records_removed: wal_reclaim_report
                .as_ref()
                .map(|report| report.index_log_records_removed)
                .unwrap_or_default(),
            retain_from_wal_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_oplog_sequence)
                .unwrap_or_default(),
            retain_from_index_log_sequence: wal_reclaim_report
                .as_ref()
                .map(|report| report.plan.retain_from_index_log_sequence)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "expire".to_string(),
            enabled: request.enable_expire,
            applied: request.enable_expire && !request.dry_run,
            skipped: !request.enable_expire,
            reason: if request.enable_expire {
                "swept expired logical records and persisted index updates".to_string()
            } else {
                "expire disabled".to_string()
            },
            pressure_signal: "expired_hot_slots+cold_slots+scan_cursors+load_on_expire_debt"
                .to_string(),
            pressure_score: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed as u64)
                .unwrap_or(pressure_signals.expired_slot_object_scan_debt as u64),
            pressure_threshold: 1,
            pressure_triggered: pressure_signals.expired_slot_object_scan_debt > 0
                || expiry_report
                    .as_ref()
                    .map(|report| report.expired_records_removed > 0)
                    .unwrap_or(false),
            before_bytes: slot_logical_bytes,
            after_bytes: slot_logical_bytes,
            expired_records_removed: expiry_report
                .as_ref()
                .map(|report| report.expired_records_removed)
                .unwrap_or_default(),
            candidate_count: expiry_report
                .as_ref()
                .map(|report| report.scanned_records)
                .unwrap_or(pressure_signals.expired_slot_object_scan_debt),
            skipped_count: expiry_report
                .as_ref()
                .map(|report| report.skipped_records)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "evict".to_string(),
            enabled: request.enable_evict,
            applied: eviction_report
                .as_ref()
                .map(|report| {
                    report.cache_entries_removed > 0
                        || report.cache_disk_bytes_removed > 0
                        || report.dropped_object_count > 0
                })
                .unwrap_or(false),
            skipped: !request.enable_evict
                || eviction_report
                    .as_ref()
                    .map(|report| !report.pressure_gate_open)
                    .unwrap_or(true),
            reason: if !request.enable_evict {
                "evict disabled".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| !report.pressure_gate_open)
                .unwrap_or(false)
            {
                "eviction skipped because memory/cache pressure is below threshold".to_string()
            } else if eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false)
            {
                "eviction entered cooldown because pressure did not decrease".to_string()
            } else {
                "evicted weighted slot/object victims under memory/cache pressure".to_string()
            },
            pressure_signal: "weighted_slot_object_eviction+memory_pressure_gate+batch_limit"
                .to_string(),
            pressure_score: pressure_signals
                .memory_cache_pressure_score
                .saturating_add(pressure_signals.disk_cache_bytes)
                .saturating_add(
                    eviction_report
                        .as_ref()
                        .map(|report| {
                            report.cache_entries_removed as u64 + report.cache_disk_bytes_removed
                        })
                        .unwrap_or_default(),
                ),
            pressure_threshold: request.eviction_memory_pressure_threshold,
            pressure_triggered: pressure_signals.memory_cache_pressure_score > 0
                || pressure_signals.disk_cache_bytes > 0
                || eviction_report
                    .as_ref()
                    .map(|report| {
                        report.cache_entries_removed > 0 || report.cache_disk_bytes_removed > 0
                    })
                    .unwrap_or(false),
            before_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            after_bytes: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            cache_entries_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_entries_removed)
                .unwrap_or_default(),
            cache_disk_bytes_removed: eviction_report
                .as_ref()
                .map(|report| report.cache_disk_bytes_removed)
                .unwrap_or_default(),
            selected_slots: eviction_report
                .as_ref()
                .map(|report| {
                    report
                        .selected_victims
                        .iter()
                        .map(|victim| victim.routing_slot)
                        .collect()
                })
                .unwrap_or_default(),
            candidate_count: eviction_report
                .as_ref()
                .map(|report| report.selected_victims.len())
                .unwrap_or_default(),
            eviction_pressure_before: eviction_report
                .as_ref()
                .map(|report| report.pressure_before)
                .unwrap_or_default(),
            eviction_pressure_after: eviction_report
                .as_ref()
                .map(|report| report.pressure_after)
                .unwrap_or_default(),
            eviction_cooldown: eviction_report
                .as_ref()
                .map(|report| report.cooldown)
                .unwrap_or(false),
            dropped_object_count: eviction_report
                .as_ref()
                .map(|report| report.dropped_object_count)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "reclaim_page".to_string(),
            enabled: request.enable_page_reclaim,
            applied: request.enable_page_reclaim
                && !request.dry_run
                && page_gc_dependency_plan.safe_to_reclaim
                && lifecycle_report
                    .as_ref()
                    .map(|report| !report.delayed_destroy_purged_segments.is_empty())
                    .unwrap_or(false),
            skipped: !request.enable_page_reclaim
                || plan.reclaim_candidates.is_empty()
                || !page_gc_dependency_plan.safe_to_reclaim,
            reason: if !request.enable_page_reclaim {
                "page reclaim disabled".to_string()
            } else if plan.reclaim_candidates.is_empty() {
                "no stale or delayed-destroy page segments are reclaimable".to_string()
            } else if !page_gc_dependency_plan.safe_to_reclaim {
                format!(
                    "page GC refused because retained dependencies remain: {}",
                    page_gc_dependency_plan.blocker_reasons.join(",")
                )
            } else {
                "reclaimed delayed-destroy page segments selected by stale-byte pressure"
                    .to_string()
            },
            pressure_signal:
                "stale_page_bytes+delayed_destroy_backlog+stale_density+dependency_retention"
                    .to_string(),
            pressure_score: pressure_signals
                .stale_page_bytes
                .saturating_add(pressure_signals.delayed_destroy_bytes)
                .saturating_add(pressure_signals.page_segment_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: request.enable_page_reclaim
                && !plan.reclaim_candidates.is_empty()
                && page_gc_dependency_plan.safe_to_reclaim,
            candidate_count: reclaim_candidate_count,
            skipped_count: reclaim_skipped_count
                .saturating_add(page_gc_dependency_plan.blocked_page_segment_ids.len()),
            before_bytes: reclaim_live_bytes + reclaim_stale_bytes,
            after_bytes: reclaim_live_bytes,
            live_bytes: reclaim_live_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_page_segment_ids: page_gc_dependency_plan.reclaimable_page_segment_ids.clone(),
            page_segments_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_segments.len())
                .unwrap_or_default(),
            page_bytes_reclaimed: lifecycle_report
                .as_ref()
                .map(|report| report.delayed_destroy_purged_bytes)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        stages.push(StorageManagerStageReport {
            stage: "index_gc".to_string(),
            enabled: request.enable_index_gc,
            applied: request.enable_index_gc
                && !request.dry_run
                && (lifecycle_report
                    .as_ref()
                    .and_then(|report| report.manifest_prune_report.as_ref())
                    .map(|report| {
                        !report.removed_manifest_ids.is_empty() || report.removed_marker_files > 0
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.applied)
                        .unwrap_or(false)),
            skipped: !request.enable_index_gc,
            reason: if request.enable_index_gc {
                "pruned obsolete manifests, rolled forward safe install markers, and applied thresholded index-log GC"
                    .to_string()
            } else {
                "index GC disabled".to_string()
            },
            pressure_signal: "obsolete_manifests+install_markers+index_log_bytes+usage_ratio+max_entries"
                .to_string(),
            pressure_score: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default() as u64
                + pressure_signals.follower_cursor_retention_blockers as u64
                + pressure_signals.raft_snapshot_retention_blockers as u64
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .bytes_before
                            .saturating_add(report.usage_ratio_basis_points)
                    })
                    .unwrap_or_default(),
            pressure_threshold: index_gc_report
                .as_ref()
                .map(|report| {
                    report
                        .bytes_threshold
                        .max(report.usage_ratio_trigger_basis_points)
                })
                .unwrap_or(1),
            pressure_triggered: request.enable_index_gc
                && (lifecycle_report
                    .as_ref()
                    .map(|report| {
                        report.manifest_prune_report.is_some()
                            || !report.install_roll_forward_reports.is_empty()
                    })
                    .unwrap_or(false)
                    || index_gc_report
                        .as_ref()
                        .map(|report| report.threshold_triggered || report.usage_ratio_triggered)
                        .unwrap_or(false)),
            candidate_count: lifecycle_report
                .as_ref()
                .map(|report| {
                    report
                        .manifest_prune_report
                        .as_ref()
                        .map(|prune| prune.removed_manifest_ids.len() + prune.removed_marker_files)
                        .unwrap_or_default()
                        + report.install_roll_forward_reports.len()
                })
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| report.removable_records_before_budget)
                    .unwrap_or_default(),
            skipped_count: lifecycle_report
                .as_ref()
                .map(|report| report.manifest_prune_plan.blocked_manifest_ids.len())
                .unwrap_or_default()
                + index_gc_report
                    .as_ref()
                    .map(|report| {
                        report
                            .removable_records_before_budget
                            .saturating_sub(report.records_removed)
                    })
                    .unwrap_or_default(),
            before_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_before)
                .unwrap_or_default(),
            after_bytes: index_gc_report
                .as_ref()
                .map(|report| report.bytes_after)
                .unwrap_or_default(),
            manifest_pruned_count: lifecycle_report
                .as_ref()
                .and_then(|report| report.manifest_prune_report.as_ref())
                .map(|report| report.removed_manifest_ids.len() + report.removed_marker_files)
                .unwrap_or_default(),
            install_roll_forward_count: lifecycle_report
                .as_ref()
                .map(|report| report.install_roll_forward_reports.len())
                .unwrap_or_default(),
            index_log_records_removed: index_gc_report
                .as_ref()
                .map(|report| report.records_removed)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        let mut merged_dump_load_policy = self.storage_merged_dump_load_policy_report(
            request.shard_id,
            request.dry_run,
            &plan,
            lifecycle_report.as_ref(),
            None,
        );

        let should_compact = request.enable_page_compaction
            && !request.dry_run
            && (!plan.reclaim_candidates.is_empty() || plan.live_page_segment_ids.len() > 1);
        let compaction_report = if should_compact {
            match self.compact_shard_pages(request.shard_id) {
                Ok(report) => Some(report),
                Err(err) => {
                    errors.push(format!("compact: {}", err.message));
                    None
                }
            }
        } else {
            None
        };
        stages.push(StorageManagerStageReport {
            stage: "compact".to_string(),
            enabled: request.enable_page_compaction,
            applied: compaction_report.is_some(),
            skipped: !request.enable_page_compaction
                || request.dry_run
                || (plan.reclaim_candidates.is_empty() && plan.live_page_segment_ids.len() <= 1),
            reason: if !request.enable_page_compaction {
                "page compaction disabled".to_string()
            } else if request.dry_run {
                "dry run reports compaction pressure without rewriting pages".to_string()
            } else if plan.reclaim_candidates.is_empty() && plan.live_page_segment_ids.len() <= 1 {
                "compaction skipped because page density does not show stale-segment pressure"
                    .to_string()
            } else {
                "rewrote live model page references into a fresh compacted segment".to_string()
            },
            pressure_signal: "model_layout_compaction_debt+stale_segment_density".to_string(),
            pressure_score: pressure_signals
                .compaction_debt_score
                .saturating_add(pressure_signals.page_segment_stale_density_basis_points),
            pressure_threshold: 1,
            pressure_triggered: should_compact,
            candidate_count: plan.stale_page_segment_ids.len(),
            skipped_count: plan.stale_page_segment_ids.len().saturating_sub(
                compaction_report
                    .as_ref()
                    .map(|report| report.stale_page_segment_ids.len())
                    .unwrap_or_default(),
            ),
            before_bytes: slot_physical_bytes,
            after_bytes: compaction_report
                .as_ref()
                .map(|_| slot_logical_bytes)
                .unwrap_or(slot_physical_bytes),
            live_bytes: slot_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            selected_page_segment_ids: compaction_report
                .as_ref()
                .map(|report| report.stale_page_segment_ids.clone())
                .unwrap_or_default(),
            compacted_page_segment_id: compaction_report
                .as_ref()
                .map(|report| report.compacted_page_segment_id),
            rewritten_page_refs: compaction_report
                .as_ref()
                .map(|report| report.rewritten_page_refs)
                .unwrap_or_default(),
            ..StorageManagerStageReport::default()
        });

        merged_dump_load_policy.compaction_policy_applied =
            request.dry_run || plan.reclaim_candidates.is_empty() || compaction_report.is_some();
        if merged_dump_load_policy.compaction_policy_applied {
            merged_dump_load_policy
                .blockers
                .retain(|blocker| blocker != "compaction");
        } else if !merged_dump_load_policy
            .blockers
            .iter()
            .any(|blocker| blocker == "compaction")
        {
            merged_dump_load_policy
                .blockers
                .push("compaction".to_string());
        }
        merged_dump_load_policy.production_slice_ready =
            merged_dump_load_policy.blockers.is_empty();

        stages.push(StorageManagerStageReport {
            stage: "reap_metrics".to_string(),
            enabled: true,
            applied: !request.dry_run,
            skipped: false,
            reason: "reported slot/page/cache pressure metrics for the completed cycle".to_string(),
            pressure_signal: "slot_page_cache_metrics".to_string(),
            pressure_score: plan.slot_summaries.len() as u64
                + plan
                    .slot_summaries
                    .iter()
                    .map(|summary| summary.page_ref_count)
                    .sum::<u64>(),
            pressure_threshold: 1,
            pressure_triggered: !plan.slot_summaries.is_empty(),
            before_bytes: slot_physical_bytes,
            after_bytes: slot_physical_bytes,
            live_bytes: slot_logical_bytes,
            stale_bytes: reclaim_stale_bytes,
            metrics_slot_count: plan.slot_summaries.len(),
            metrics_page_ref_count: plan
                .slot_summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum(),
            ..StorageManagerStageReport::default()
        });
        let cycle_duration_ms = now_ms().saturating_sub(cycle_started_unix_ms);
        annotate_storage_manager_admin_stage_fields(
            &mut stages,
            cycle_started_unix_ms,
            cycle_duration_ms,
            &errors,
            pressure_signals.follower_cursor_retention_blockers
                + pressure_signals.raft_snapshot_retention_blockers,
        );

        let production_parity_slice = errors.is_empty()
            && cxx_stage_order
                .iter()
                .all(|stage| stages.iter().any(|report| &report.stage == stage))
            && stages.iter().all(|stage| stage.enabled)
            && merged_dump_load_policy.production_slice_ready;
        StorageManagerCycleReport {
            shard_id: request.shard_id,
            dry_run: request.dry_run,
            cxx_stage_order,
            completed: errors.is_empty(),
            production_parity_slice,
            pressure_signals,
            stages,
            plan,
            merged_dump_load_policy,
            lifecycle_report,
            expiry_report,
            compaction_report,
            wal_reclaim_report,
            index_gc_report,
            eviction_report,
            page_gc_dependency_plan,
            errors,
        }
    }

    fn storage_merged_dump_load_policy_report(
        &self,
        shard_id: ShardId,
        dry_run: bool,
        plan: &StorageLifecyclePlan,
        lifecycle_report: Option<&StorageLifecycleReport>,
        compaction_report: Option<&ShardCompactionReport>,
    ) -> StorageMergedDumpLoadPolicyReport {
        let mut report = StorageMergedDumpLoadPolicyReport {
            shard_id,
            dry_run,
            dirty_slot_count: plan.dirty_slots.len(),
            selected_dump_slot_count: plan.selected_dump_slots.len(),
            page_reclaim_policy_applied: dry_run
                || plan.reclaim_candidates.is_empty()
                || lifecycle_report
                    .map(|lifecycle| !lifecycle.delayed_destroy_purged_segments.is_empty())
                    .unwrap_or(false),
            compaction_policy_applied: dry_run
                || plan.reclaim_candidates.is_empty()
                || compaction_report.is_some(),
            index_gc_policy_applied: dry_run
                || lifecycle_report
                    .map(|lifecycle| lifecycle.manifest_prune_report.is_some())
                    .unwrap_or(false),
            cache_policy_applied: dry_run
                || lifecycle_report
                    .map(|lifecycle| {
                        lifecycle.cache_entries_removed > 0
                            || lifecycle.cache_disk_bytes_removed > 0
                            || lifecycle.cache_warmup_page_refs > 0
                    })
                    .unwrap_or(false),
            ..StorageMergedDumpLoadPolicyReport::default()
        };
        let Some(lifecycle) = lifecycle_report else {
            if !dry_run {
                report.blockers.push("lifecycle_report_missing".to_string());
            }
            report.production_slice_ready = dry_run;
            return report;
        };
        report.install_marker_policy_checked = true;
        report.install_roll_forward_checked = !lifecycle.install_roll_forward_reports.is_empty()
            || self.interrupted_slot_dump_installs(shard_id).is_empty();
        let Some(manifest) = lifecycle.dump_manifest.as_ref() else {
            if !plan.selected_dump_slots.is_empty() {
                report.blockers.push("dump_manifest_missing".to_string());
            }
            report.production_slice_ready = plan.selected_dump_slots.is_empty()
                && report.page_reclaim_policy_applied
                && report.compaction_policy_applied
                && report.index_gc_policy_applied;
            return report;
        };
        report.manifest_id = Some(manifest.manifest_id.clone());
        report.dumped_slot_count = manifest.slot_ids.len();
        let restored = serde_json::from_slice::<ShardState>(&manifest.index_bytes).ok();
        let manifest_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
        let live_page_entries = restored
            .as_ref()
            .map(|restored| {
                collect_live_page_entries(restored)
                    .into_iter()
                    .filter(|entry| {
                        let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
                            self.routing_slot_for_key(manifest.shard_id, &entry.object_key)
                        });
                        manifest_slots.is_empty() || manifest_slots.contains(&routing_slot)
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let expected_slot_summaries = restored
            .as_ref()
            .map(|restored| slot_dump_manifest_comparable_summaries(restored, &manifest_slots))
            .unwrap_or_default();
        let actual_slot_summaries = comparable_slot_dump_summaries(manifest.slot_summaries.clone());
        let expected_object_lifecycle = restored.as_ref().map(|restored| {
            storage_object_lifecycle_report_for_slots(
                manifest.shard_id,
                restored,
                &manifest_slots,
                |key| self.routing_slot_for_key(manifest.shard_id, key),
            )
        });
        let checksum_ok = slot_dump_manifest_checksum(manifest)
            .map(|checksum| checksum == manifest.checksum)
            .unwrap_or(false);
        let index_checksum_ok = !manifest.index_bytes.is_empty()
            && manifest.index_sha256 == sha256_hex_bytes(&manifest.index_bytes);
        report.manifest_checksum_validated = checksum_ok
            && index_checksum_ok
            && restored.is_some()
            && actual_slot_summaries == expected_slot_summaries;
        report.manifest_generation_validated = !manifest.dump_generation_id.is_empty()
            && manifest.dump_generation_id == slot_dump_generation_id(manifest);
        report.sequence_boundaries_validated = manifest.oplog_sequence
            >= plan.undumped_oplog_records
            && manifest.index_log_sequence > 0
            && manifest.index_sha256 == sha256_hex_bytes(&manifest.index_bytes);
        let preflight = self.slot_dump_install_preflight_report(manifest);
        report.page_segments_validated = preflight.missing_page_segment_ids.is_empty()
            && preflight.corrupt_page_segment_ids.is_empty();
        report.live_page_refs_validated = live_page_entries.len() as u64 == manifest.live_page_refs
            && preflight.unreadable_page_ref_count == 0
            && preflight.unreadable_page_bytes == 0;
        report.object_lifecycle_validated = expected_object_lifecycle
            .map(|expected| {
                expected.live_object_ids == manifest.object_lifecycle.live_object_ids
                    && expected.live_page_refs == manifest.object_lifecycle.live_page_refs
                    && manifest.object_lifecycle.missing_owner_page_refs == 0
                    && manifest.object_lifecycle.owner_mismatch_page_refs == 0
                    && manifest.object_lifecycle.reused_object_id_conflicts == 0
            })
            .unwrap_or(false);
        report.install_preflight_safe = preflight.install_safe;
        for (ready, blocker) in [
            (report.manifest_checksum_validated, "manifest_checksum"),
            (report.manifest_generation_validated, "manifest_generation"),
            (report.sequence_boundaries_validated, "sequence_boundaries"),
            (report.page_segments_validated, "page_segments"),
            (report.live_page_refs_validated, "live_page_refs"),
            (report.object_lifecycle_validated, "object_lifecycle"),
            (report.install_preflight_safe, "install_preflight"),
            (report.install_marker_policy_checked, "install_markers"),
            (report.install_roll_forward_checked, "install_roll_forward"),
            (report.page_reclaim_policy_applied, "page_reclaim"),
            (report.compaction_policy_applied, "compaction"),
            (report.index_gc_policy_applied, "index_gc"),
            (report.cache_policy_applied, "cache_policy"),
        ] {
            if !ready {
                report.blockers.push(blocker.to_string());
            }
        }
        report.production_slice_ready = report.blockers.is_empty();
        report
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
        let segment_integrity = storage_segment_integrity_report(shard_id, &recovery, &boundary);
        let plan = self.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
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
        let page_store = stats
            .as_ref()
            .map(|stats| stats.page_store.clone())
            .unwrap_or_else(|| self.page_store.stats());
        let log_compatibility = self.storage_log_compatibility_report(shard_id);
        let page_format_compatibility = self.storage_page_format_compatibility_report(shard_id);
        let slot_dump_manifest_count = self.list_slot_dump_manifests(shard_id).len();
        let interrupted_slot_dump_install_count = boundary.interrupted_slot_dump_installs.len();
        let undumped_oplog_records = boundary
            .latest_safe_oplog_sequence
            .saturating_sub(boundary.latest_dump_oplog_sequence);
        let mut blockers = Vec::new();
        if !boundary.stale_index_page_refs.is_empty() {
            blockers.push("stale_index_page_refs".to_string());
        }
        if !boundary.corrupt_page_segment_ids.is_empty() {
            blockers.push("corrupt_page_segments".to_string());
        }
        if boundary.unreadable_page_bytes > 0 || !recovery.all_live_pages_readable {
            blockers.push("unreadable_live_page_refs".to_string());
        }
        if !boundary.owner_mismatch_page_refs.is_empty() {
            blockers.push("owner_mismatch_page_refs".to_string());
        }
        if boundary.object_lifecycle.missing_owner_page_refs > 0 {
            blockers.push("missing_owner_page_refs".to_string());
        }
        if boundary.object_lifecycle.reused_object_id_conflicts > 0 {
            blockers.push("reused_object_id_conflicts".to_string());
        }
        if interrupted_slot_dump_install_count > 0 {
            blockers.push("interrupted_slot_dump_installs".to_string());
        }
        if !boundary.manifest_chain_issues.is_empty() {
            blockers.push("broken_slot_dump_manifest_chain".to_string());
        }
        if !segment_integrity.integrity_ok
            && !blockers
                .iter()
                .any(|blocker| blocker == "storage_segment_integrity_failed")
        {
            blockers.push("storage_segment_integrity_failed".to_string());
        }
        if recovery.feature_page_layout.has_errors() {
            blockers.push("feature_page_layout_mismatch".to_string());
        }

        let mut warnings = Vec::new();
        if !plan.dirty_slots.is_empty() {
            warnings.push("dirty_slots_pending_dump".to_string());
        }
        if !plan.stale_page_segment_ids.is_empty() {
            warnings.push("stale_page_segments_pending_gc".to_string());
        }
        if !boundary.orphan_page_segment_ids.is_empty() {
            warnings.push("orphan_page_segments_pending_gc".to_string());
        }
        if slot_dump_manifest_count == 0 && recovery.total_page_refs > 0 {
            warnings.push("no_slot_dump_manifest_for_live_pages".to_string());
        }
        if policy
            .max_dirty_slots
            .map(|limit| plan.dirty_slots.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("dirty_slots_exceed_policy".to_string());
        }
        if policy
            .max_stale_page_segments
            .map(|limit| plan.stale_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("stale_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_orphan_page_segments
            .map(|limit| boundary.orphan_page_segment_ids.len() > limit)
            .unwrap_or(false)
        {
            blockers.push("orphan_page_segments_exceed_policy".to_string());
        }
        if policy
            .max_undumped_oplog_records
            .map(|limit| undumped_oplog_records > limit)
            .unwrap_or(false)
        {
            blockers.push("undumped_oplog_records_exceed_policy".to_string());
        }
        if policy.require_slot_dump_manifest
            && slot_dump_manifest_count == 0
            && recovery.total_page_refs > 0
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
            dirty_slot_count: plan.dirty_slots.len(),
            stale_page_segment_count: plan.stale_page_segment_ids.len(),
            orphan_page_segment_count: boundary.orphan_page_segment_ids.len(),
            undumped_oplog_records,
            corrupt_page_segment_count: boundary.corrupt_page_segment_ids.len(),
            unreadable_page_ref_count: recovery.unreadable_page_refs.len(),
            owner_mismatch_page_ref_count: boundary.owner_mismatch_page_refs.len(),
            missing_owner_page_ref_count: boundary.object_lifecycle.missing_owner_page_refs,
            reused_object_id_conflict_count: boundary.object_lifecycle.reused_object_id_conflicts,
            interrupted_slot_dump_install_count,
            prepared_slot_dump_install_count: boundary.prepared_slot_dump_install_count,
            installed_slot_dump_install_count: boundary.installed_slot_dump_install_count,
            unknown_slot_dump_install_count: boundary.unknown_slot_dump_install_count,
            slot_dump_manifest_count,
            cache_memory_bytes: cache.memory_bytes,
            cache_disk_bytes: cache.disk_bytes,
            page_store_bytes_written: page_store.bytes_written,
            boundary,
            object_lifecycle: recovery.object_lifecycle,
            segment_integrity,
            log_compatibility,
            page_format_compatibility,
            feature_page_layout_mismatch_count: recovery.feature_page_layout.mismatch_count(),
            corrupt_feature_page_count: recovery
                .feature_page_layout
                .corrupt_packed_feature_pages
                .len(),
            feature_page_layout: recovery.feature_page_layout,
        }
    }

    pub fn storage_log_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StorageLogCompatibilityReport {
        let oplog_stats = self.oplog_store.stats(shard_id);
        let index_log_stats = self.index_log_store.stats(shard_id);
        let oplog_records = self
            .oplog_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        StorageLogCompatibilityReport {
            shard_id,
            oplog_format: "rust-jsonl-command-v1".to_string(),
            index_log_format: "rust-jsonl-shard-index-v1".to_string(),
            compatibility_mode: "rust_native_migration_only".to_string(),
            migration_required: true,
            cxx_reader_supported: false,
            cxx_writer_supported: false,
            golden_conversion_required: true,
            rust_native_replay_safe: true,
            cxx_binary_compatible: false,
            oplog_last_sequence: oplog_stats.last_sequence,
            index_log_last_sequence: index_log_stats.last_sequence,
            oplog_records,
            index_log_records,
            oplog_bytes: oplog_stats.bytes_written,
            index_log_bytes: index_log_stats.bytes_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed Rust/C++ binary log serving is not supported"
                    .to_string(),
                "C++ binary/protobuf oplog reader and writer are not implemented".to_string(),
                "C++ binary/protobuf index-log reader and writer are not implemented".to_string(),
                "golden log conversion/replay suite is required before C++ migration".to_string(),
            ],
        }
    }

    pub fn storage_page_format_compatibility_report(
        &self,
        shard_id: ShardId,
    ) -> StoragePageFormatCompatibilityReport {
        let stats = self.page_store.stats();
        let zones = self.page_store.zone_summary();
        StoragePageFormatCompatibilityReport {
            shard_id,
            page_format: "rust-page-envelope-v6".to_string(),
            rust_envelope_version: 6,
            compatibility_mode: "rust_envelope_migration_only".to_string(),
            migration_required: true,
            cxx_page_header_reader_supported: false,
            cxx_page_header_writer_supported: false,
            golden_conversion_required: true,
            rust_native_read_safe: true,
            cxx_page_header_compatible: false,
            checksum_protected: true,
            object_ids_embedded: true,
            routing_slots_embedded: true,
            compression_supported: true,
            active_zones: zones.active_zones,
            sealed_zones: zones.sealed_zones,
            delayed_destroy_zones: zones.delayed_destroy_zones,
            live_physical_bytes: zones.live_physical_bytes,
            reclaimable_physical_bytes: zones.reclaimable_physical_bytes,
            page_store_writes: stats.writes,
            page_store_bytes_written: stats.bytes_written,
            logical_bytes_written: stats.logical_bytes_written,
            compressed_records_written: stats.compressed_records_written,
            compatibility_gaps: vec![
                "compatibility mode is migration-only; direct mixed Rust-envelope/C++ page-header serving is not supported"
                    .to_string(),
                "C++ protobuf page header reader and writer are not implemented".to_string(),
                "C++ slot/page layout and page-id allocation are not byte-compatible".to_string(),
                "golden page conversion/replay suite is required before C++ migration".to_string(),
            ],
        }
    }

    pub fn warm_cache_from_page_index(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> usize {
        self.storage_cache_warmup_report(shard_id, selected_slots)
            .warmed_page_refs
    }

    pub fn storage_cache_warmup_report(
        &self,
        shard_id: ShardId,
        selected_slots: impl IntoIterator<Item = u32>,
    ) -> StorageCacheWarmupReport {
        let selected_slots = selected_slots.into_iter().collect::<BTreeSet<_>>();
        let mut report = StorageCacheWarmupReport {
            shard_id,
            selected_slots: selected_slots.iter().copied().collect(),
            ..StorageCacheWarmupReport::default()
        };
        let shards = self.shards.read().expect("engine lock poisoned");
        let Some(shard) = shards.get(&shard_id) else {
            return report;
        };
        for entry in collect_live_page_entries(shard) {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| self.routing_slot_for_key(shard_id, &entry.object_key));
            if !selected_slots.is_empty() && !selected_slots.contains(&routing_slot) {
                report.skipped_page_refs = report.skipped_page_refs.saturating_add(1);
                continue;
            }
            report.considered_page_refs = report.considered_page_refs.saturating_add(1);
            let key = CacheKey::page_with_slot(
                shard_id,
                entry.address.page_segment_id,
                entry.address.offset,
                entry.address.length,
                entry.address.routing_slot,
            );
            if self.cache.get(&key).ok().flatten().is_some() {
                report.already_cached_page_refs = report.already_cached_page_refs.saturating_add(1);
                report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
            } else if let Ok(bytes) = self.page_store.read(&entry.address) {
                report.page_store_reads = report.page_store_reads.saturating_add(1);
                let byte_len = bytes.len() as u64;
                match self.cache.put(key, bytes) {
                    Ok(()) => {
                        report.warmed_page_refs = report.warmed_page_refs.saturating_add(1);
                        report.warmed_bytes = report.warmed_bytes.saturating_add(byte_len);
                    }
                    Err(_) => {
                        report.failed_page_refs = report.failed_page_refs.saturating_add(1);
                    }
                }
            } else {
                report.failed_page_refs = report.failed_page_refs.saturating_add(1);
            }
        }
        report
    }

    pub fn storage_cache_inspection_report(
        &self,
        shard_id: ShardId,
    ) -> StorageCacheInspectionReport {
        let entries = self.cache.entries_for_shard(shard_id);
        let mut slot_summaries = BTreeMap::<u32, StorageCacheSlotSummary>::new();
        for entry in &entries {
            let Some(routing_slot) = cache_entry_routing_slot(entry) else {
                continue;
            };
            let summary = slot_summaries
                .entry(routing_slot)
                .or_insert(StorageCacheSlotSummary {
                    routing_slot,
                    ..StorageCacheSlotSummary::default()
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
            slot_summaries: slot_summaries.into_values().collect(),
        }
    }

    pub fn invalidate_storage_cache_slot(
        &self,
        request: StorageCacheInvalidateSlotRequest,
    ) -> Result<CacheGcReport, Status> {
        self.cache
            .invalidate_slot(request.shard_id, request.routing_slot)
            .map_err(|err| Status::error("cache_slot_invalidation_failed", err.to_string()))
    }

    pub fn storage_recovery_boundary_report(
        &self,
        shard_id: ShardId,
    ) -> StorageRecoveryBoundaryReport {
        let manifests = self.list_slot_dump_manifests(shard_id);
        let latest_dump_oplog_sequence = manifests
            .iter()
            .map(|manifest| manifest.oplog_sequence)
            .max()
            .unwrap_or_default();
        let latest_dump_index_log_sequence = manifests
            .iter()
            .map(|manifest| manifest.index_log_sequence)
            .max()
            .unwrap_or_default();
        let latest_safe_oplog_sequence = self.oplog_store.stats(shard_id).last_sequence;
        let latest_safe_index_log_sequence = self.index_log_store.stats(shard_id).last_sequence;
        let live_page_segment_ids = self
            .live_page_segment_ids(shard_id)
            .into_iter()
            .collect::<BTreeSet<_>>();
        let all_segment_ids = self
            .page_store
            .segment_ids()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeSet<_>>();
        let orphan_page_segment_ids = all_segment_ids
            .difference(&live_page_segment_ids)
            .copied()
            .collect::<Vec<_>>();
        let latest_dump_slots = manifests
            .last()
            .map(|manifest| manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>())
            .unwrap_or_default();
        let missing_dump_slot_ids = self
            .slot_storage_summaries(shard_id)
            .into_iter()
            .filter(|summary| summary.dirty_object_count > 0)
            .map(|summary| summary.routing_slot)
            .filter(|slot| !latest_dump_slots.contains(slot))
            .collect::<Vec<_>>();
        let interrupted_slot_dump_installs = self.interrupted_slot_dump_installs(shard_id);
        let (
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
        ) = slot_dump_install_phase_counts(&interrupted_slot_dump_installs);
        let manifest_chain_issues = slot_dump_manifest_chain_issues(&manifests);
        let recovery = self.storage_recovery_report_without_boundary(shard_id);
        let corrupt_page_segment_ids = recovery
            .page_segment_reports
            .iter()
            .filter(|report| report.has_corruption)
            .map(|report| report.page_segment_id)
            .collect::<Vec<_>>();
        let unreadable_page_bytes = recovery
            .unreadable_page_refs
            .iter()
            .map(|error| error.length)
            .sum();
        let object_lifecycle = recovery.object_lifecycle.clone();
        StorageRecoveryBoundaryReport {
            shard_id,
            latest_safe_oplog_sequence,
            latest_safe_index_log_sequence,
            latest_dump_oplog_sequence,
            latest_dump_index_log_sequence,
            selected_replay_oplog_sequence: latest_dump_oplog_sequence
                .min(latest_safe_oplog_sequence),
            selected_replay_index_log_sequence: latest_dump_index_log_sequence
                .min(latest_safe_index_log_sequence),
            orphan_page_segment_ids,
            missing_dump_slot_ids,
            stale_index_page_refs: recovery.unreadable_page_refs,
            interrupted_slot_dump_installs,
            prepared_slot_dump_install_count,
            installed_slot_dump_install_count,
            unknown_slot_dump_install_count,
            manifest_chain_issues,
            owner_mismatch_page_refs: recovery.owner_mismatch_page_refs,
            missing_owner_page_refs: recovery.missing_owner_page_refs,
            object_lifecycle,
            corrupt_page_segment_ids,
            unreadable_page_bytes,
        }
    }

    pub fn prometheus_metrics(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP temporalstore_shard_records Number of records by shard and kind.\n");
        out.push_str("# TYPE temporalstore_shard_records gauge\n");
        out.push_str("# HELP temporalstore_cache_operations_total Cache operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_cache_operations_total counter\n");
        out.push_str("# HELP temporalstore_cache_bytes Cache bytes by shard and tier.\n");
        out.push_str("# TYPE temporalstore_cache_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_operations_total Page store operation counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_operations_total counter\n");
        out.push_str("# HELP temporalstore_page_store_bytes_total Page store byte counters by shard and kind.\n");
        out.push_str("# TYPE temporalstore_page_store_bytes_total counter\n");
        out.push_str("# HELP temporalstore_page_store_zone_count Page-store zone counts by shard and lifecycle state.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_count gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_bytes Page-store physical bytes by shard and lifecycle kind.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_bytes gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_unix_ms Oldest page-store zone timestamp by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_unix_ms gauge\n");
        out.push_str("# HELP temporalstore_page_store_zone_oldest_age_ms Oldest page-store zone age by shard and lifecycle scope.\n");
        out.push_str("# TYPE temporalstore_page_store_zone_oldest_age_ms gauge\n");
        out.push_str("# HELP temporalstore_oplog_records_total Oplog append records by shard.\n");
        out.push_str("# TYPE temporalstore_oplog_records_total counter\n");
        out.push_str("# HELP temporalstore_oplog_bytes_total Oplog appended bytes by shard.\n");
        out.push_str("# TYPE temporalstore_oplog_bytes_total counter\n");
        out.push_str(
            "# HELP temporalstore_object_manager_objects Logical hot objects tracked by shard.\n",
        );
        out.push_str("# TYPE temporalstore_object_manager_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_page_refs Page-address references tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_page_refs gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_objects Dirty logical objects tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_objects gauge\n");
        out.push_str("# HELP temporalstore_object_manager_dirty_slots Dirty routing slots tracked by shard.\n");
        out.push_str("# TYPE temporalstore_object_manager_dirty_slots gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_page_refs Live page refs by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_page_refs gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_bytes Live bytes by shard, routing slot, and kind.\n");
        out.push_str("# TYPE temporalstore_storage_slot_bytes gauge\n");
        out.push_str("# HELP temporalstore_storage_slot_dirty_objects Dirty objects by shard and routing slot.\n");
        out.push_str("# TYPE temporalstore_storage_slot_dirty_objects gauge\n");
        out.push_str(
            "# HELP temporalstore_partition_routing_slots Routing slots owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_slots gauge\n");
        out.push_str(
            "# HELP temporalstore_ingestion_records_total Ingestion record counters by outcome.\n",
        );
        out.push_str("# TYPE temporalstore_ingestion_records_total counter\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_lag Kafka ingestion lag in offsets.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_lag gauge\n");
        out.push_str("# HELP temporalstore_ingestion_kafka_committed_offset Kafka committed offset by topic and partition.\n");
        out.push_str("# TYPE temporalstore_ingestion_kafka_committed_offset gauge\n");
        out.push_str("# HELP temporalstore_ingestion_flink_checkpoint_state Flink checkpoint state as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_ingestion_flink_checkpoint_state gauge\n");
        for stats in self.loaded_shard_stats() {
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "string".into()),
                ],
                stats.string_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "hash".into()),
                ],
                stats.hash_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "set".into()),
                ],
                stats.set_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "feature".into()),
                ],
                stats.feature_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "sequence".into()),
                ],
                stats.sequence_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "ips".into()),
                ],
                stats.ips_records as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_shard_records",
                &[
                    ("shard_id", stats.shard_id.to_string()),
                    ("kind", "risk".into()),
                ],
                stats.risk_records as u64,
            );
            for (kind, value) in [
                ("memory_hits", stats.cache.memory_hits),
                ("disk_hits", stats.cache.disk_hits),
                ("misses", stats.cache.misses),
                ("puts", stats.cache.puts),
                ("invalidations", stats.cache.invalidations),
                ("memory_evictions", stats.cache.memory_evictions),
                (
                    "memory_admission_accepted",
                    stats.cache.memory_admission_accepted,
                ),
                (
                    "memory_admission_rejected",
                    stats.cache.memory_admission_rejected,
                ),
                ("memory_fills", stats.cache.memory_fills),
                ("disk_fills", stats.cache.disk_fills),
                ("refill_failures", stats.cache.refill_failures),
                ("eviction_capacity", stats.cache.eviction_capacity),
                ("eviction_oversize", stats.cache.eviction_oversize),
                ("pinned_entries", stats.cache.pinned_entries),
                ("pin_operations", stats.cache.pin_operations),
                ("unpin_operations", stats.cache.unpin_operations),
                ("eviction_pinned_skips", stats.cache.eviction_pinned_skips),
                ("compressed_puts", stats.cache.compressed_puts),
                ("compressed_hits", stats.cache.compressed_hits),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (tier, value) in [
                ("memory", stats.cache.memory_bytes),
                ("disk", stats.cache.disk_bytes),
                ("compression_saved", stats.cache.compression_bytes_saved),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_cache_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("tier", tier.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("writes", stats.page_store.writes),
                ("reads", stats.page_store.reads),
                (
                    "compressed_writes",
                    stats.page_store.compressed_records_written,
                ),
                ("compressed_reads", stats.page_store.compressed_records_read),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_operations_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("written", stats.page_store.bytes_written),
                ("read", stats.page_store.bytes_read),
                ("logical_written", stats.page_store.logical_bytes_written),
                ("logical_read", stats.page_store.logical_bytes_read),
                (
                    "compression_saved",
                    stats.page_store.compression_bytes_saved,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_bytes_total",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (state, value) in [
                ("active", stats.page_store_zones.active_zones),
                ("sealed", stats.page_store_zones.sealed_zones),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_zones,
                ),
                ("purged", stats.page_store_zones.purged_zones),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_count",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("state", state.into()),
                    ],
                    value,
                );
            }
            for (kind, value) in [
                ("active", stats.page_store_zones.active_physical_bytes),
                ("sealed", stats.page_store_zones.sealed_physical_bytes),
                (
                    "delayed_destroy",
                    stats.page_store_zones.delayed_destroy_physical_bytes,
                ),
                ("purged", stats.page_store_zones.purged_physical_bytes),
                ("live", stats.page_store_zones.live_physical_bytes),
                (
                    "reclaimable",
                    stats.page_store_zones.reclaimable_physical_bytes,
                ),
                (
                    "total_known",
                    stats.page_store_zones.total_known_physical_bytes,
                ),
            ] {
                push_metric(
                    &mut out,
                    "temporalstore_page_store_zone_bytes",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("kind", kind.into()),
                    ],
                    value,
                );
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_zone_unix_ms),
                ("live", stats.page_store_zones.oldest_live_zone_unix_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_zone_unix_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_unix_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            for (scope, value) in [
                ("known", stats.page_store_zones.oldest_known_zone_age_ms),
                ("live", stats.page_store_zones.oldest_live_zone_age_ms),
                (
                    "reclaimable",
                    stats.page_store_zones.oldest_reclaimable_zone_age_ms,
                ),
            ] {
                if let Some(value) = value {
                    push_metric(
                        &mut out,
                        "temporalstore_page_store_zone_oldest_age_ms",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("scope", scope.into()),
                        ],
                        value,
                    );
                }
            }
            push_metric(
                &mut out,
                "temporalstore_oplog_records_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.oplog.writes,
            );
            push_metric(
                &mut out,
                "temporalstore_oplog_bytes_total",
                &[("shard_id", stats.shard_id.to_string())],
                stats.oplog.bytes_written,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_page_refs",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.page_ref_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_objects",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_object_count as u64,
            );
            push_metric(
                &mut out,
                "temporalstore_object_manager_dirty_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.dirty_slot_count as u64,
            );
            for summary in self.slot_storage_summaries(stats.shard_id) {
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_page_refs",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.page_ref_count,
                );
                for (kind, value) in [
                    ("logical", summary.logical_bytes),
                    ("physical", summary.physical_bytes),
                ] {
                    push_metric(
                        &mut out,
                        "temporalstore_storage_slot_bytes",
                        &[
                            ("shard_id", stats.shard_id.to_string()),
                            ("slot", summary.routing_slot.to_string()),
                            ("kind", kind.to_string()),
                        ],
                        value,
                    );
                }
                push_metric(
                    &mut out,
                    "temporalstore_storage_slot_dirty_objects",
                    &[
                        ("shard_id", stats.shard_id.to_string()),
                        ("slot", summary.routing_slot.to_string()),
                    ],
                    summary.dirty_object_count,
                );
            }
            push_metric(
                &mut out,
                "temporalstore_partition_routing_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.routing_slot_count as u64,
            );
        }
        let ingestion = self.ingestion_state_report();
        for (outcome, value) in [
            ("accepted", ingestion.stats.accepted_total),
            ("failed", ingestion.stats.failed_total),
            ("duplicate", ingestion.stats.duplicate_total),
            ("dead_letter", ingestion.stats.dead_letter_total),
            ("kafka_committed", ingestion.stats.kafka_committed_total),
            ("flink_precommit", ingestion.stats.flink_precommit_total),
            ("flink_commit", ingestion.stats.flink_commit_total),
            ("flink_abort", ingestion.stats.flink_abort_total),
        ] {
            push_metric(
                &mut out,
                "temporalstore_ingestion_records_total",
                &[("outcome", outcome.to_string())],
                value,
            );
        }
        push_metric(
            &mut out,
            "temporalstore_ingestion_kafka_lag",
            &[("scope", "max".to_string())],
            ingestion.stats.max_kafka_lag.max(0) as u64,
        );
        for offset in ingestion.kafka_offsets {
            push_metric(
                &mut out,
                "temporalstore_ingestion_kafka_committed_offset",
                &[
                    ("topic", offset.topic),
                    ("partition", offset.partition.to_string()),
                ],
                offset.committed_offset.max(0) as u64,
            );
        }
        for checkpoint in ingestion.flink_checkpoints {
            let status = format!("{:?}", checkpoint.status).to_ascii_lowercase();
            push_metric(
                &mut out,
                "temporalstore_ingestion_flink_checkpoint_state",
                &[
                    ("job_id", checkpoint.job_id),
                    ("operator_uid", checkpoint.operator_uid),
                    ("subtask_index", checkpoint.subtask_index.to_string()),
                    ("checkpoint_id", checkpoint.checkpoint_id.to_string()),
                    ("status", status),
                ],
                1,
            );
        }
        out
    }

    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Page => self
                .page_store
                .read_logical_range(request.page_segment_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::Index => fs::read(self.index_path(request.shard_id))
                .map_err(|err| err.to_string())
                .map(|bytes| {
                    let start = request.offset as usize;
                    let end = start.saturating_add(request.size as usize).min(bytes.len());
                    if start >= bytes.len() {
                        Vec::new()
                    } else {
                        bytes[start..end].to_vec()
                    }
                }),
            StreamKind::Oplog => self
                .oplog_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::IndexLog => self
                .index_log_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
        };
        match data {
            Ok(data) => StreamReadResponse {
                status: Status::ok(),
                data,
            },
            Err(err) => StreamReadResponse {
                status: Status::error("stream_read_failed", err.to_string()),
                data: Vec::new(),
            },
        }
    }

    pub fn scan_stream(&self, request: ScanStreamRequest) -> ScanStreamResponse {
        if request.start_offset > request.end_offset {
            return ScanStreamResponse {
                status: Status::error("invalid_stream_range", "start_offset is after end_offset"),
                records: Vec::new(),
                end_of_stream: true,
            };
        }
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(request.max_bytes);
        if request.stream_kind == StreamKind::Oplog || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Oplog => self
                    .oplog_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        request.max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Page => unreachable!(),
            };
            return match records {
                Ok(records) => ScanStreamResponse {
                    status: Status::ok(),
                    records: records
                        .into_iter()
                        .map(|(offset, data)| StreamRecord { offset, data })
                        .collect(),
                    end_of_stream: true,
                },
                Err(err) => ScanStreamResponse {
                    status: Status::error("stream_scan_failed", err.to_string()),
                    records: Vec::new(),
                    end_of_stream: true,
                },
            };
        }
        let read = self.read_stream(StreamReadRequest {
            shard_id: request.shard_id,
            stream_kind: request.stream_kind,
            page_segment_id: request.page_segment_id,
            offset: request.start_offset,
            size,
        });
        ScanStreamResponse {
            status: read.status.clone(),
            records: if read.status.ok && !read.data.is_empty() {
                vec![StreamRecord {
                    offset: request.start_offset,
                    data: read.data,
                }]
            } else {
                Vec::new()
            },
            end_of_stream: true,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        let responses = request
            .commands
            .into_iter()
            .map(|command| {
                self.execute(ExecuteRequest {
                    shard_id: request.shard_id,
                    command,
                })
            })
            .collect();
        BatchExecuteResponse {
            status: Status::ok(),
            responses,
        }
    }

    pub fn batch_execute_checked(
        &self,
        request: CheckedBatchExecuteRequest,
    ) -> CheckedBatchExecuteResponse {
        if let Err(status) = self.validate_load_version(request.shard_id, request.load_version) {
            return CheckedBatchExecuteResponse {
                status: status.clone(),
                response: BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                },
            };
        }
        let response = self.batch_execute(BatchExecuteRequest {
            shard_id: request.shard_id,
            commands: request.commands,
        });
        CheckedBatchExecuteResponse {
            status: response.status.clone(),
            response,
        }
    }

    pub fn export_index_bytes(&self, shard_id: ShardId) -> Result<Vec<u8>, std::io::Error> {
        fs::read(self.index_path(shard_id))
    }

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
        report.segment_integrity =
            storage_segment_integrity_report(shard_id, &report, &report.boundary);
        report
    }

    fn storage_recovery_report_without_boundary(&self, shard_id: ShardId) -> StorageRecoveryReport {
        let index_bytes = self
            .index_path(shard_id)
            .metadata()
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let oplog_records = self
            .oplog_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let index_log_records = self
            .index_log_store
            .scan(shard_id, 0, u64::MAX, u64::MAX)
            .map(|records| records.len())
            .unwrap_or_default();
        let active_page_segment_ids = self.page_store.segment_ids().unwrap_or_default();
        let zone_descriptors = self.page_store.zone_descriptors();
        let zone_summary = self.page_store.zone_summary();
        let page_segment_reports = self.page_store.segment_reports().unwrap_or_default();
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
        let mut page_segment_live_reports = page_segment_reports
            .iter()
            .map(|report| {
                (
                    report.page_segment_id,
                    StorageRecoverySegmentLiveReport {
                        page_segment_id: report.page_segment_id,
                        physical_bytes: report.physical_bytes,
                        logical_bytes: report.logical_bytes,
                        page_count: report.page_count,
                        ..StorageRecoverySegmentLiveReport::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut live_object_ids = BTreeMap::<u64, BTreeSet<u64>>::new();
        let mut live_routing_slots = BTreeMap::<u64, BTreeSet<u32>>::new();
        for address in &addresses {
            let segment_report = page_segment_live_reports
                .entry(address.page_segment_id)
                .or_insert(StorageRecoverySegmentLiveReport {
                    page_segment_id: address.page_segment_id,
                    ..StorageRecoverySegmentLiveReport::default()
                });
            segment_report.live_page_refs = segment_report.live_page_refs.saturating_add(1);
            segment_report.live_physical_bytes = segment_report
                .live_physical_bytes
                .saturating_add(address.length);
            if let Some(object_id) = address.object_id {
                let objects = live_object_ids.entry(address.page_segment_id).or_default();
                objects.insert(object_id);
                segment_report.live_object_count = objects.len() as u64;
            }
            if let Some(routing_slot) = address.routing_slot {
                let slots = live_routing_slots
                    .entry(address.page_segment_id)
                    .or_default();
                slots.insert(routing_slot);
                segment_report.live_routing_slot_count = slots.len() as u64;
            }
            match self.page_store.read(address) {
                Ok(bytes) => {
                    readable_page_refs += 1;
                    segment_report.readable_live_page_refs =
                        segment_report.readable_live_page_refs.saturating_add(1);
                    segment_report.live_logical_bytes = segment_report
                        .live_logical_bytes
                        .saturating_add(bytes.len() as u64);
                }
                Err(err) => {
                    segment_report.unreadable_live_page_refs =
                        segment_report.unreadable_live_page_refs.saturating_add(1);
                    unreadable_page_refs.push(StorageRecoveryPageError {
                        page_segment_id: address.page_segment_id,
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
        let page_segment_live_reports = page_segment_live_reports
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
        object_lifecycle.stale_object_ids = page_segment_live_reports
            .iter()
            .map(|report| report.stale_page_estimate)
            .sum();
        let mut live_page_segment_ids = addresses
            .iter()
            .map(|address| address.page_segment_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        live_page_segment_ids.sort_unstable();
        StorageRecoveryReport {
            shard_id,
            index_bytes,
            index_write_atomic: true,
            oplog_records,
            index_log_records,
            active_page_segment_ids,
            live_page_segment_ids,
            zone_descriptors,
            zone_summary,
            page_segment_reports,
            page_segment_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            owner_mismatch_page_refs,
            missing_owner_page_refs,
            object_lifecycle,
            all_live_pages_readable: total_page_refs == readable_page_refs,
            boundary: StorageRecoveryBoundaryReport::default(),
            segment_integrity: StorageSegmentIntegrityReport::default(),
            feature_page_layout,
        }
    }

    pub fn live_page_segment_ids(&self, shard_id: ShardId) -> Vec<u64> {
        let shards = self.shards.read().expect("engine lock poisoned");
        let mut ids = shards
            .get(&shard_id)
            .map(collect_live_page_segment_ids)
            .unwrap_or_default()
            .into_iter()
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    pub fn sweep_expired_records(
        &self,
        shard_id: ShardId,
    ) -> Result<ShardExpirySweepReport, Status> {
        self.sweep_expired_records_with_request(ShardExpirySweepRequest {
            shard_id,
            load_cold_slots: true,
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
        let mut hot_keys = shard
            .expires_at_ms
            .iter()
            .filter(|(key, _)| record_exists(shard, key))
            .map(|(key, expires_at)| (key.clone(), *expires_at))
            .collect::<Vec<_>>();
        hot_keys.sort_by(|left, right| left.0.cmp(&right.0));
        let mut cold_keys = shard
            .expires_at_ms
            .iter()
            .filter(|(key, _)| !record_exists(shard, key))
            .map(|(key, expires_at)| (key.clone(), *expires_at))
            .collect::<Vec<_>>();
        cold_keys.sort_by(|left, right| left.0.cmp(&right.0));

        let hot_limit = request.max_hot_slots_per_round;
        let cold_limit = request.max_cold_slots_per_round;
        let (hot_selected, next_hot_cursor) =
            select_expiry_cursor_window(hot_keys, request.hot_cursor.as_deref(), hot_limit);
        let (cold_selected, next_cold_cursor) =
            select_expiry_cursor_window(cold_keys, request.cold_cursor.as_deref(), cold_limit);
        let mut expired_records_removed = 0;
        let mut skipped_records = 0usize;
        let mut loaded_for_expire = 0usize;
        for (key, expires_at) in hot_selected.iter() {
            if *expires_at <= now {
                if delete_record(shard, key) {
                    invalidate_record_all(&self.cache, request.shard_id, key);
                    expired_records_removed += 1;
                }
            } else {
                skipped_records = skipped_records.saturating_add(1);
            }
        }
        for (key, expires_at) in cold_selected.iter() {
            if *expires_at <= now {
                if request.load_cold_slots {
                    loaded_for_expire = loaded_for_expire.saturating_add(1);
                    if delete_record(shard, key) {
                        invalidate_record_all(&self.cache, request.shard_id, key);
                        expired_records_removed += 1;
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
            let index_bytes = serde_json::to_vec_pretty(shard)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            self.persist_index_bytes(request.shard_id, &index_bytes)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            let _ = self
                .index_log_store
                .append_json(request.shard_id, &index_bytes);
        }
        Ok(ShardExpirySweepReport {
            shard_id: request.shard_id,
            expired_records_removed,
            hot_slots_scanned: hot_selected.len(),
            cold_slots_scanned: cold_selected.len(),
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

    fn validate_shard_page_ownership(
        &self,
        shard_id: ShardId,
        shard: &ShardState,
    ) -> StoragePageOwnershipValidation {
        let mut validation = StoragePageOwnershipValidation::default();
        for entry in collect_live_page_entries(shard) {
            let expected_object_id = expected_live_page_object_id(shard_id, &entry);
            let expected_routing_slot = self.routing_slot_for_key(shard_id, &entry.object_key);
            let object_mismatch = entry
                .address
                .object_id
                .is_some_and(|actual| actual != expected_object_id);
            let slot_mismatch = entry
                .address
                .routing_slot
                .is_some_and(|actual| actual != expected_routing_slot);
            if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
                validation.missing_owner_page_refs =
                    validation.missing_owner_page_refs.saturating_add(1);
            }
            if object_mismatch || slot_mismatch {
                validation
                    .mismatches
                    .push(StorageRecoveryPageOwnerMismatch {
                        object_key: entry.object_key,
                        page_segment_id: entry.address.page_segment_id,
                        offset: entry.address.offset,
                        expected_object_id,
                        actual_object_id: entry.address.object_id,
                        expected_routing_slot,
                        actual_routing_slot: entry.address.routing_slot,
                    });
            }
        }
        validation
    }

    pub fn compact_shard_pages(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
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
        let before_segments = collect_live_page_segment_ids(shard);
        let before = compaction_utility_report(&self.page_store, shard);
        let roll = self
            .page_store
            .roll_segment()
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let mut rewritten_page_refs = 0;

        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.strings.values_mut(),
            &mut rewritten_page_refs,
        )?;
        for fields in shard.hashes.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                fields.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for members in shard.sets.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                members.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.features.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.sequences.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.ips.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.risk_pages.values_mut(),
            &mut rewritten_page_refs,
        )?;
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.context_nodes.values_mut(),
            &mut rewritten_page_refs,
        )?;
        for series in shard.context_events.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.context_indexes.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.context_audits.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.context_dirty.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.context_children.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.context_embeddings.values_mut(),
            &mut rewritten_page_refs,
        )?;
        for series in shard.context_summaries.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.context_compressions.values_mut() {
            compact_feature_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series,
                &mut rewritten_page_refs,
            )?;
        }
        compact_page_addresses(
            &self.page_store,
            &self.cache,
            shard_id,
            shard.context_entities.values_mut(),
            &mut rewritten_page_refs,
        )?;
        for (key, meta_series) in &mut shard.ips_meta {
            if let Some(address_series) = shard.ips.get(key) {
                for (timestamp, meta) in meta_series {
                    if let Some(address) = address_series.get(timestamp) {
                        meta.address = address.clone();
                    }
                }
            }
        }

        rebuild_slot_first_index(shard, 0, u32::MAX);
        let after_segments = collect_live_page_segment_ids(shard);
        let after = compaction_utility_report(&self.page_store, shard);
        let stale_page_segment_ids = before_segments
            .difference(&after_segments)
            .copied()
            .collect::<Vec<_>>();
        let index_bytes = serde_json::to_vec_pretty(shard)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        self.persist_index_bytes(shard_id, &index_bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = self.index_log_store.append_json(shard_id, &index_bytes);
        Ok(ShardCompactionReport {
            shard_id,
            previous_page_segment_id: roll.previous_page_segment_id,
            compacted_page_segment_id: roll.new_page_segment_id,
            rewritten_page_refs,
            stale_page_segment_ids,
            before,
            after,
        })
    }

    fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    fn persist_slot_dump_manifest(
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

    fn persist_slot_dump_install_marker(
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

    fn persist_slot_dump_install_marker_by_fields(
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

    fn validate_slot_dump_generation_for_install(
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

    fn load_index(&self, shard_id: ShardId) -> Option<ShardState> {
        let bytes = fs::read(self.index_path(shard_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn persist_index_bytes(&self, shard_id: ShardId, bytes: &[u8]) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        atomic_write_bytes(&self.index_path(shard_id), bytes)
    }

    fn validate_load_version(&self, shard_id: ShardId, load_version: u64) -> Result<(), Status> {
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

    fn shard_stats(&self, shard_id: ShardId) -> Option<ShardStats> {
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
            let total_records = string_records
                + hash_records
                + set_records
                + feature_records
                + sequence_records
                + ips_records
                + risk_records;
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
            let partition_info = PartitionInfoStats {
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
                partition_info,
                cache: self.cache.stats(),
                page_store,
                page_store_zones,
                oplog: self.oplog_store.stats(shard_id),
            }
        })
    }
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec_pretty(shard).expect("shard index should serialize")
}

fn push_metric(out: &mut String, name: &str, labels: &[(&str, String)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&escape_metric_label(value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn atomic_write_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("index");
    let temp_path = parent.join(format!(
        ".{file_name}.tmp-{}-{}",
        std::process::id(),
        next_temp_counter()
    ));
    let write_result = (|| {
        let mut file = File::create(&temp_path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, path)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn next_temp_counter() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn unique_temp_path(kind: &str) -> PathBuf {
    let counter = next_temp_counter();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

fn slot_dump_manifest_dir(index_dir: &std::path::Path, shard_id: ShardId) -> PathBuf {
    index_dir
        .join("slot-dumps")
        .join(format!("shard-{shard_id}"))
}

fn slot_dump_manifest_path(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, shard_id).join(format!("{manifest_id}.json"))
}

fn slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<Option<SlotDumpManifest>, std::io::Error> {
    let path = slot_dump_manifest_path(index_dir, shard_id, manifest_id);
    if !path.exists() {
        return Ok(None);
    }
    serde_json::from_slice::<SlotDumpManifest>(&fs::read(path)?)
        .map(Some)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

fn slot_dump_install_marker_path(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> PathBuf {
    slot_dump_manifest_dir(index_dir, marker.shard_id).join(format!(
        "{}.{}.{}.marker",
        marker.manifest_id, marker.phase, marker.created_unix_ms
    ))
}

fn write_slot_dump_install_marker(
    index_dir: &std::path::Path,
    marker: &SlotDumpInstallMarker,
) -> Result<(), std::io::Error> {
    let path = slot_dump_install_marker_path(index_dir, marker);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    fs::write(path, bytes)
}

fn slot_dump_install_marker_files_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<(SlotDumpInstallMarker, PathBuf)>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut markers = Vec::new();
    if !dir.exists() {
        return Ok(markers);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "marker")
            .unwrap_or(false)
        {
            continue;
        }
        let path = entry.path();
        let marker = serde_json::from_slice::<SlotDumpInstallMarker>(&fs::read(&path)?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        markers.push((marker, path));
    }
    markers.sort_by_key(|(marker, _)| {
        (
            marker.index_log_sequence,
            marker.created_unix_ms,
            slot_dump_install_phase_rank(&marker.phase),
        )
    });
    Ok(markers)
}

fn list_slot_dump_install_markers_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    Ok(slot_dump_install_marker_files_at(index_dir, shard_id)?
        .into_iter()
        .map(|(marker, _)| marker)
        .collect())
}

fn interrupted_slot_dump_installs_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpInstallMarker>, std::io::Error> {
    let mut latest_by_manifest = BTreeMap::<String, SlotDumpInstallMarker>::new();
    for marker in list_slot_dump_install_markers_at(index_dir, shard_id)? {
        let replace = latest_by_manifest
            .get(&marker.manifest_id)
            .map(|existing| {
                slot_dump_install_phase_rank(&marker.phase)
                    > slot_dump_install_phase_rank(&existing.phase)
                    || (slot_dump_install_phase_rank(&marker.phase)
                        == slot_dump_install_phase_rank(&existing.phase)
                        && marker.created_unix_ms > existing.created_unix_ms)
            })
            .unwrap_or(true);
        if replace {
            latest_by_manifest.insert(marker.manifest_id.clone(), marker);
        }
    }
    Ok(latest_by_manifest
        .into_values()
        .filter(|marker| marker.phase != "commit")
        .collect())
}

fn remove_obsolete_slot_dump_install_markers(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    manifest_id: &str,
) -> Result<usize, std::io::Error> {
    let mut removed = 0usize;
    for (marker, path) in slot_dump_install_marker_files_at(index_dir, shard_id)? {
        if marker.manifest_id == manifest_id
            && (marker.phase == "prepare" || marker.phase == "install")
            && fs::remove_file(path).is_ok()
        {
            removed = removed.saturating_add(1);
        }
    }
    Ok(removed)
}

fn slot_dump_install_phase_counts(markers: &[SlotDumpInstallMarker]) -> (usize, usize, usize) {
    let mut prepared = 0usize;
    let mut installed = 0usize;
    let mut unknown = 0usize;
    for marker in markers {
        match marker.phase.as_str() {
            "prepare" => prepared = prepared.saturating_add(1),
            "install" => installed = installed.saturating_add(1),
            _ => unknown = unknown.saturating_add(1),
        }
    }
    (prepared, installed, unknown)
}

fn slot_dump_install_phase_rank(phase: &str) -> u8 {
    match phase {
        "prepare" => 1,
        "install" => 2,
        "commit" => 3,
        _ => 0,
    }
}

fn slot_dump_manifest_chain_issues(
    manifests: &[SlotDumpManifest],
) -> Vec<SlotDumpManifestChainIssue> {
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    manifests
        .iter()
        .filter_map(|manifest| {
            let parent = manifest.parent_manifest_id.as_ref()?;
            (!manifest_ids.contains(parent)).then(|| SlotDumpManifestChainIssue {
                manifest_id: manifest.manifest_id.clone(),
                parent_manifest_id: Some(parent.clone()),
                reason: "missing_parent_manifest".to_string(),
            })
        })
        .collect()
}

fn retained_slot_dump_manifest_ids(manifests: &[SlotDumpManifest]) -> BTreeSet<String> {
    let by_id = manifests
        .iter()
        .map(|manifest| (manifest.manifest_id.clone(), manifest))
        .collect::<BTreeMap<_, _>>();
    let mut retained = BTreeSet::new();
    let mut cursor = manifests
        .iter()
        .max_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms))
        .map(|manifest| manifest.manifest_id.clone());
    while let Some(manifest_id) = cursor {
        if !retained.insert(manifest_id.clone()) {
            break;
        }
        cursor = by_id
            .get(&manifest_id)
            .and_then(|manifest| manifest.parent_manifest_id.clone());
    }
    retained
}

fn slot_dump_manifest_prune_plan_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
    follower_cursors: &[SlotDumpFollowerReplayCursor],
    raft_snapshot_refs: &[SlotDumpRaftSnapshotRef],
) -> Result<SlotDumpManifestPrunePlan, std::io::Error> {
    let manifests = list_slot_dump_manifests_at(index_dir, shard_id)?;
    let mut retained = retained_slot_dump_manifest_ids(&manifests);
    let mut follower_blocks = Vec::new();
    let mut raft_snapshot_blocks = Vec::new();
    for cursor in follower_cursors
        .iter()
        .filter(|cursor| cursor.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= cursor.oplog_sequence
                && manifest.index_log_sequence <= cursor.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            follower_blocks.push(SlotDumpFollowerRetentionBlock {
                follower_id: cursor.follower_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                cursor_oplog_sequence: cursor.oplog_sequence,
                cursor_index_log_sequence: cursor.index_log_sequence,
                reason: "follower_cursor_anchor".to_string(),
            });
        }
    }
    for snapshot in raft_snapshot_refs
        .iter()
        .filter(|snapshot| snapshot.shard_id == shard_id)
    {
        let Some(anchor) = manifests.iter().rev().find(|manifest| {
            manifest.oplog_sequence <= snapshot.oplog_sequence
                && manifest.index_log_sequence <= snapshot.index_log_sequence
        }) else {
            continue;
        };
        if retained.insert(anchor.manifest_id.clone()) {
            raft_snapshot_blocks.push(SlotDumpRaftSnapshotRetentionBlock {
                snapshot_id: snapshot.snapshot_id.clone(),
                manifest_id: anchor.manifest_id.clone(),
                manifest_oplog_sequence: anchor.oplog_sequence,
                manifest_index_log_sequence: anchor.index_log_sequence,
                snapshot_oplog_sequence: snapshot.oplog_sequence,
                snapshot_index_log_sequence: snapshot.index_log_sequence,
                last_included_index: snapshot.last_included_index,
                last_included_term: snapshot.last_included_term,
                reason: "raft_snapshot_anchor".to_string(),
            });
        }
    }
    let interrupted = interrupted_slot_dump_installs_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .collect::<BTreeSet<_>>();
    let manifest_ids = manifests
        .iter()
        .map(|manifest| manifest.manifest_id.clone())
        .collect::<BTreeSet<_>>();
    let mut prunable_manifest_ids = Vec::new();
    let mut blocked_manifest_ids = Vec::new();
    for manifest in &manifests {
        if retained.contains(&manifest.manifest_id) {
            continue;
        }
        if interrupted.contains(&manifest.manifest_id) {
            blocked_manifest_ids.push(manifest.manifest_id.clone());
        } else {
            prunable_manifest_ids.push(manifest.manifest_id.clone());
        }
    }
    let prunable_marker_manifest_ids = list_slot_dump_install_markers_at(index_dir, shard_id)?
        .into_iter()
        .map(|marker| marker.manifest_id)
        .filter(|manifest_id| {
            !retained.contains(manifest_id)
                && !interrupted.contains(manifest_id)
                && (prunable_manifest_ids.iter().any(|id| id == manifest_id)
                    || !manifest_ids.contains(manifest_id))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let mut reasons = Vec::new();
    if !prunable_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_manifest".to_string());
    }
    if !prunable_marker_manifest_ids.is_empty() {
        reasons.push("obsolete_slot_dump_marker".to_string());
    }
    if !blocked_manifest_ids.is_empty() {
        reasons.push("interrupted_install_blocks_prune".to_string());
    }
    if !follower_blocks.is_empty() {
        reasons.push("follower_cursor_blocks_prune".to_string());
    }
    if !raft_snapshot_blocks.is_empty() {
        reasons.push("raft_snapshot_blocks_prune".to_string());
    }
    Ok(SlotDumpManifestPrunePlan {
        shard_id,
        retained_manifest_ids: retained.into_iter().collect(),
        prunable_manifest_ids,
        prunable_marker_manifest_ids,
        blocked_manifest_ids,
        follower_blocks,
        raft_snapshot_blocks,
        reasons,
    })
}

fn list_slot_dump_manifests_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Result<Vec<SlotDumpManifest>, std::io::Error> {
    let dir = slot_dump_manifest_dir(index_dir, shard_id);
    let mut manifests = Vec::new();
    if !dir.exists() {
        return Ok(manifests);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry
            .path()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "json")
            .unwrap_or(false)
        {
            continue;
        }
        let manifest = serde_json::from_slice::<SlotDumpManifest>(&fs::read(entry.path())?)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        manifests.push(manifest);
    }
    manifests.sort_by_key(|manifest| (manifest.index_log_sequence, manifest.created_unix_ms));
    Ok(manifests)
}

fn latest_slot_dump_manifest_at(
    index_dir: &std::path::Path,
    shard_id: ShardId,
) -> Option<SlotDumpManifest> {
    list_slot_dump_manifests_at(index_dir, shard_id)
        .ok()?
        .into_iter()
        .last()
}

fn slot_dump_manifest_checksum(manifest: &SlotDumpManifest) -> Result<String, Status> {
    let mut payload = manifest.clone();
    payload.checksum.clear();
    serde_json::to_vec(&payload)
        .map(|bytes| sha256_hex_bytes(&bytes))
        .map_err(|err| Status::error("slot_dump_checksum_failed", err.to_string()))
}

fn slot_dump_fault_scenario(
    scenario: impl Into<String>,
    expected_code: impl Into<String>,
    actual_code: impl Into<String>,
    blockers: Vec<String>,
    install_safe: bool,
) -> SlotDumpFaultScenarioReport {
    let expected_code = expected_code.into();
    let actual_code = actual_code.into();
    SlotDumpFaultScenarioReport {
        scenario: scenario.into(),
        passed: actual_code == expected_code,
        expected_code,
        actual_code,
        blockers,
        install_safe,
    }
}

fn slot_dump_generation_id(manifest: &SlotDumpManifest) -> String {
    let mut digest = Sha256::new();
    digest.update(manifest.shard_id.to_le_bytes());
    digest.update(manifest.oplog_sequence.to_le_bytes());
    digest.update(manifest.index_log_sequence.to_le_bytes());
    for slot_id in &manifest.slot_ids {
        digest.update(slot_id.to_le_bytes());
    }
    for page_segment_id in &manifest.page_segment_ids {
        digest.update(page_segment_id.to_le_bytes());
    }
    digest.update(manifest.index_sha256.as_bytes());
    if manifest.version >= 3 {
        digest.update(manifest.object_lifecycle.live_object_ids.to_le_bytes());
        digest.update(manifest.object_lifecycle.live_page_refs.to_le_bytes());
        digest.update(manifest.object_lifecycle.stale_object_ids.to_le_bytes());
        digest.update(
            manifest
                .object_lifecycle
                .tombstoned_object_ids
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .reused_object_id_conflicts
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .missing_owner_page_refs
                .to_le_bytes(),
        );
        digest.update(
            manifest
                .object_lifecycle
                .owner_mismatch_page_refs
                .to_le_bytes(),
        );
        for object_id in &manifest.object_lifecycle.reused_object_ids {
            digest.update(object_id.to_le_bytes());
        }
        for key in &manifest.object_lifecycle.tombstoned_object_keys {
            digest.update(key.as_bytes());
            digest.update([0]);
        }
    }
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn execute_on_shard(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    feature_max_size: usize,
    async_storage: bool,
    shard_id: ShardId,
    start_routing_slot: u32,
    end_routing_slot: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    let mut mutated = false;
    let response = match command {
        Command::CommonDelete { key } => {
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            let expires_at = now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    shard.expires_at_ms.insert(record_key, expires_at);
                }
            }
            mutated = true;
            invalidate_record_all(cache, shard_id, &key);
            CommandResponse::Empty
        }
        Command::CommonTtl { key } => {
            let expired = shard
                .expires_at_ms
                .get(&key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false);
            let value = ttl_ms(shard, &key);
            mutated = expired;
            CommandResponse::Integer { value }
        }
        Command::CommonExists { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_record_all(cache, shard_id, &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: if record_exists(shard, &key) { 1 } else { 0 },
            }
        }
        Command::StringSet { key, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(key.clone(), address);
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "string", &key, None);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                shard.strings.insert(key.clone(), address);
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetConditional {
            key,
            value,
            ttl_ms,
            condition,
            return_old,
        } => {
            remove_if_expired(shard, &key);
            let old_value = shard
                .strings
                .get(&key)
                .and_then(|address| read_page_bytes(cache, page_store, shard_id, address));
            let exists = old_value.is_some();
            let should_set = match condition {
                StringSetCondition::Always => true,
                StringSetCondition::IfExists => exists,
                StringSetCondition::IfNotExists => !exists,
            };
            if should_set {
                let object_id = stable_page_object_id(shard_id, "string", &key, None);
                let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    upsert_slot_index_page(
                        shard,
                        shard_id,
                        "string",
                        &key,
                        None,
                        address.clone(),
                        true,
                    );
                    shard.strings.insert(key.clone(), address);
                    if let Some(ttl_ms) = ttl_ms {
                        shard
                            .expires_at_ms
                            .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                    } else {
                        shard.expires_at_ms.remove(&key);
                    }
                    mutated = true;
                }
                invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            }
            if return_old {
                CommandResponse::Bytes { value: old_value }
            } else {
                CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                }
            }
        }
        Command::StringGet { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::string(shard_id, &key), || {
                CommandResponse::Bytes {
                    value: shard
                        .strings
                        .get(&key)
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address)),
                }
            })
        }
        Command::StringDelete { key } => {
            mutated = shard.strings.remove(&key).is_some();
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &value,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "hash",
                    &key,
                    Some(field.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::HashGet { key, field } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::hash(shard_id, &key, &field), || {
                CommandResponse::Bytes {
                    value: shard
                        .hashes
                        .get(&key)
                        .and_then(|fields| fields.get(&field))
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address)),
                }
            })
        }
        Command::HashMultiGet { key, fields } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Values {
                        values: vec![None; fields.len()],
                    },
                    mutated,
                };
            }
            let values = fields
                .iter()
                .map(|field| {
                    shard
                        .hashes
                        .get(&key)
                        .and_then(|entries| entries.get(field))
                        .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
                })
                .collect();
            CommandResponse::Values { values }
        }
        Command::HashMultiSet { key, entries } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            for (field, value) in entries {
                let object_id = stable_page_object_id(shard_id, "hash", &key, Some(&field));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    upsert_slot_index_page(
                        shard,
                        shard_id,
                        "hash",
                        &key,
                        Some(field.clone()),
                        address.clone(),
                        true,
                    );
                    shard
                        .hashes
                        .entry(key.clone())
                        .or_default()
                        .insert(field.clone(), address);
                    let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                    mutated = true;
                }
            }
            CommandResponse::Empty
        }
        Command::HashIncrBy {
            key,
            field,
            increment,
        } => {
            remove_if_expired(shard, &key);
            let current = shard
                .hashes
                .get(&key)
                .and_then(|entries| entries.get(&field))
                .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
                .and_then(|bytes| parse_i64(&bytes))
                .unwrap_or_default();
            let value = current.saturating_add(increment);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                value.to_string().as_bytes(),
                Some(stable_page_object_id(shard_id, "hash", &key, Some(&field))),
                Some(page_routing_slot(
                    &key,
                    start_routing_slot,
                    end_routing_slot,
                )),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "hash",
                    &key,
                    Some(field.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), address);
                let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
                mutated = true;
            }
            CommandResponse::Integer { value }
        }
        Command::HashGetAll { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let entries = shard
                .hashes
                .get(&key)
                .map(|fields| {
                    let mut entries = fields
                        .iter()
                        .filter_map(|(field, address)| {
                            read_page_bytes(cache, page_store, shard_id, address)
                                .map(|value| (field.clone(), value))
                        })
                        .collect::<Vec<_>>();
                    entries.sort_by(|a, b| a.0.cmp(&b.0));
                    entries
                })
                .unwrap_or_default();
            CommandResponse::HashEntries { entries }
        }
        Command::HashLen { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard
                    .hashes
                    .get(&key)
                    .map(|fields| fields.len() as i64)
                    .unwrap_or_default(),
            }
        }
        Command::HashDelete { key, field } => {
            if let Some(fields) = shard.hashes.get_mut(&key) {
                mutated = fields.remove(&field).is_some();
            }
            let _ = cache.invalidate(&CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::SetAdd { key, member } => {
            remove_if_expired(shard, &key);
            let member_component = hex::encode(&member);
            let object_id = stable_page_object_id(shard_id, "set", &key, Some(&member_component));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &member,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                upsert_slot_index_page(
                    shard,
                    shard_id,
                    "set",
                    &key,
                    Some(member_component.clone()),
                    address.clone(),
                    true,
                );
                shard
                    .sets
                    .entry(key.clone())
                    .or_default()
                    .insert(member.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::SetMembers { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "set", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Members {
                        members: Vec::new(),
                    },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::set_members(shard_id, &key), || {
                let members = shard
                    .sets
                    .get(&key)
                    .map(|set| {
                        set.values()
                            .filter_map(|address| {
                                read_page_bytes(cache, page_store, shard_id, address)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::Members { members }
            })
        }
        Command::SetRemove { key, member } => {
            if let Some(set) = shard.sets.get_mut(&key) {
                mutated = set.remove(&member).is_some();
            }
            let _ = cache.invalidate_record(shard_id, "set", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppend { key, points } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAppendWithPolicy {
            key,
            points,
            policy,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let mut accepted_points = Vec::new();
            let mut accepted_timestamps = BTreeSet::new();
            for point in sorted_feature_points(points) {
                let exists = series.contains_key(&point.timestamp_ms)
                    || accepted_timestamps.contains(&point.timestamp_ms);
                let should_write = match policy {
                    FeatureWritePolicy::Upsert => true,
                    FeatureWritePolicy::InsertIfAbsent => !exists,
                    FeatureWritePolicy::ReplaceExisting => exists,
                };
                if should_write {
                    accepted_timestamps.insert(point.timestamp_ms);
                    accepted_points.push(point);
                }
            }
            if !accepted_points.is_empty() {
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "feature",
                    &key,
                    accepted_points,
                    routing_slot,
                    async_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::FeatureQuery {
            key,
            start_ms,
            end_ms,
            count,
        } => cached_response(
            cache,
            CacheKey::feature_query(shard_id, &key, start_ms, end_ms, count),
            || {
                let points = shard
                    .features
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .take(count.unwrap_or(5000))
                            .filter_map(|(timestamp_ms, address)| {
                                read_feature_point(
                                    cache,
                                    page_store,
                                    shard_id,
                                    *timestamp_ms,
                                    address,
                                )
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::FeaturePoints { points }
            },
        ),
        Command::FeatureQueryFiltered {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            let limit = count.unwrap_or(feature_max_size).min(feature_max_size);
            let points = shard
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(limit)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                                .and_then(|point| {
                                    let row = SequenceFeatureRow::decode_cpp_feature_value(
                                        point.timestamp_ms,
                                        &point.value,
                                    )?;
                                    filters
                                        .iter()
                                        .all(|filter| sequence_filter_matches(&row, filter))
                                        .then_some(point)
                                })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::FeatureReplace {
            key,
            start_ms,
            end_ms,
            points,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let replaced = series
                .range(start_ms..=end_ms)
                .map(|(timestamp_ms, _)| *timestamp_ms)
                .collect::<Vec<_>>();
            for timestamp_ms in replaced {
                series.remove(&timestamp_ms);
                mutated = true;
            }
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "feature",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                    mutated = true;
                } else {
                    break;
                }
            }
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureDelete { key } => {
            mutated = shard.features.remove(&key).is_some();
            let _ = cache.invalidate_record(shard_id, "feature", &key);
            CommandResponse::Empty
        }
        Command::FeatureAggQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "feature", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Aggregate { value: 0 },
                    mutated,
                };
            }
            let values = shard
                .features
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(5000))
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                                .map(|point| point.value)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            CommandResponse::Aggregate {
                value: aggregate_feature_values(&values, &aggregator),
            }
        }
        Command::SequenceAdd { key, rows } => {
            remove_if_expired(shard, &key);
            let series = shard.sequences.entry(key.clone()).or_default();
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = rows
                .into_iter()
                .filter_map(|row| {
                    serde_json::to_vec(&row).ok().map(|value| FeaturePoint {
                        timestamp_ms: row.timestamp_ms,
                        value,
                    })
                })
                .collect::<Vec<_>>();
            let points = sorted_feature_points(points);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "sequence",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            while series.len() > feature_max_size {
                if let Some(oldest) = series.keys().next().copied() {
                    series.remove(&oldest);
                } else {
                    break;
                }
            }
            CommandResponse::Empty
        }
        Command::SequenceQuery {
            key,
            start_ms,
            end_ms,
            count,
            filters,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::SequenceRows { rows: Vec::new() },
                    mutated,
                };
            }
            let rows = shard
                .sequences
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_sequence_row(cache, page_store, shard_id, *timestamp_ms, address)
                        })
                        .filter(|row| {
                            filters
                                .iter()
                                .all(|filter| sequence_filter_matches(row, filter))
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::SequenceRows { rows }
        }
        Command::SequenceBatchQuery { queries } => {
            let groups = queries
                .into_iter()
                .map(
                    |SequenceQuerySpec {
                         key,
                         start_ms,
                         end_ms,
                         count,
                         filters,
                     }| {
                        if remove_if_expired(shard, &key) {
                            mutated = true;
                            return (key, Vec::new());
                        }
                        let rows = sequence_rows_in_range(
                            cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                            &filters,
                        );
                        (key, rows)
                    },
                )
                .collect();
            CommandResponse::SequenceRowGroups { groups }
        }
        Command::IpsAdd {
            key,
            timestamp_ms,
            instance,
        } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                vec![FeaturePoint {
                    timestamp_ms,
                    value: instance,
                }],
                routing_slot,
                async_storage,
            ) {
                let address = addresses
                    .into_iter()
                    .find_map(|(timestamp, address)| (timestamp == timestamp_ms).then_some(address))
                    .expect("single IPS timestamped page ref should exist");
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type: None,
                        table_id: None,
                        request_id: None,
                    },
                );
                mutated = true;
            }
            CommandResponse::Empty
        }
        Command::IpsAddWithOptions {
            key,
            timestamp_ms,
            instance,
            action_type,
            table_id,
            request_id,
        } => {
            remove_if_expired(shard, &key);
            if let Some(request_id) = &request_id {
                if shard
                    .ips_request_ids
                    .get(&key)
                    .is_some_and(|ids| ids.contains(request_id))
                {
                    return ExecuteOutcome {
                        response: CommandResponse::Integer { value: 0 },
                        mutated: false,
                    };
                }
            }
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                vec![FeaturePoint {
                    timestamp_ms,
                    value: instance,
                }],
                routing_slot,
                async_storage,
            ) {
                let address = addresses
                    .into_iter()
                    .find_map(|(timestamp, address)| (timestamp == timestamp_ms).then_some(address))
                    .expect("single IPS timestamped page ref should exist");
                shard
                    .ips
                    .entry(key.clone())
                    .or_default()
                    .insert(timestamp_ms, address.clone());
                shard.ips_meta.entry(key.clone()).or_default().insert(
                    timestamp_ms,
                    IpsPointMeta {
                        address,
                        action_type,
                        table_id,
                        request_id: request_id.clone(),
                    },
                );
                if let Some(request_id) = request_id {
                    shard
                        .ips_request_ids
                        .entry(key)
                        .or_default()
                        .insert(request_id);
                }
                mutated = true;
            }
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsLoad { key, points } => {
            remove_if_expired(shard, &key);
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            let points = sorted_feature_points(points);
            let mut loaded = 0i64;
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "ips",
                &key,
                points,
                routing_slot,
                async_storage,
            ) {
                for (timestamp_ms, address) in addresses {
                    shard
                        .ips
                        .entry(key.clone())
                        .or_default()
                        .insert(timestamp_ms, address.clone());
                    shard.ips_meta.entry(key.clone()).or_default().insert(
                        timestamp_ms,
                        IpsPointMeta {
                            address,
                            action_type: None,
                            table_id: None,
                            request_id: None,
                        },
                    );
                    mutated = true;
                    loaded += 1;
                }
            }
            CommandResponse::Integer { value: loaded }
        }
        Command::IpsQueryLast { key, count } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .ips
                .get(&key)
                .map(|series| {
                    series
                        .iter()
                        .rev()
                        .take(count)
                        .filter_map(|(timestamp_ms, address)| {
                            read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::IpsQueryRange {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsBatchQueryLast { keys, count } => {
            let groups = keys
                .into_iter()
                .map(|key| {
                    if remove_if_expired(shard, &key) {
                        mutated = true;
                        return (key, Vec::new());
                    }
                    let points = shard
                        .ips
                        .get(&key)
                        .map(|series| {
                            series
                                .iter()
                                .rev()
                                .take(count)
                                .filter_map(|(timestamp_ms, address)| {
                                    read_feature_point(
                                        cache,
                                        page_store,
                                        shard_id,
                                        *timestamp_ms,
                                        address,
                                    )
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    (key, points)
                })
                .collect();
            CommandResponse::FeaturePointGroups { groups }
        }
        Command::IpsRemove { key, timestamp_ms } => {
            if let Some(series) = shard.ips.get_mut(&key) {
                mutated = series.remove(&timestamp_ms).is_some();
                if series.is_empty() {
                    shard.ips.remove(&key);
                }
            }
            if let Some(series) = shard.ips_meta.get_mut(&key) {
                if let Some(meta) = series.remove(&timestamp_ms) {
                    if let Some(request_id) = meta.request_id {
                        if let Some(ids) = shard.ips_request_ids.get_mut(&key) {
                            ids.remove(&request_id);
                        }
                    }
                }
                if series.is_empty() {
                    shard.ips_meta.remove(&key);
                }
            }
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsDelete { key } => {
            mutated = shard.ips.remove(&key).is_some();
            mutated |= shard.ips_meta.remove(&key).is_some();
            shard.ips_request_ids.remove(&key);
            CommandResponse::Integer {
                value: if mutated { 1 } else { 0 },
            }
        }
        Command::IpsCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .ips
                .get(&key)
                .map(|series| series.range(start_ms..=end_ms).count() as i64)
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::IpsQueryRangeWithOptions {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::IpsSnapshot {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range(
                    cache, page_store, shard_id, shard, &key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsSnapshotReport {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::IpsSnapshotReport {
                        report: empty_ips_snapshot_report(key, start_ms, end_ms, count),
                    },
                    mutated,
                };
            }
            CommandResponse::IpsSnapshotReport {
                report: ips_snapshot_report_in_range(
                    cache, page_store, shard_id, shard, key, start_ms, end_ms, count,
                ),
            }
        }
        Command::IpsStat {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::IpsStats {
                        stats: IpsStats {
                            total: 0,
                            first_timestamp_ms: None,
                            last_timestamp_ms: None,
                            action_type_counts: Vec::new(),
                            table_id_counts: Vec::new(),
                        },
                    },
                    mutated,
                };
            }
            CommandResponse::IpsStats {
                stats: ips_stats_in_range(shard, &key, start_ms, end_ms),
            }
        }
        Command::IpsFilter {
            key,
            start_ms,
            end_ms,
            count,
            action_type,
            table_id,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            CommandResponse::FeaturePoints {
                points: ips_points_in_range_with_options(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    &key,
                    start_ms,
                    end_ms,
                    count,
                    action_type,
                    table_id,
                ),
            }
        }
        Command::RiskIncrement {
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskIncrementWithOptions {
            key,
            timestamp_ms,
            amount,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default() += amount;
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
            }
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskChangeAdd {
            key,
            timestamp_ms,
            value,
            precision_ms,
            ttl_ms,
        } => {
            remove_if_expired(shard, &key);
            let bucket_ms = precision_ms
                .filter(|precision_ms| *precision_ms > 0)
                .map(|precision_ms| timestamp_ms - timestamp_ms % precision_ms)
                .unwrap_or(timestamp_ms);
            shard
                .risk_changes
                .entry(key.clone())
                .or_default()
                .entry(bucket_ms)
                .or_default()
                .insert(value);
            if let Some(ttl_ms) = ttl_ms {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskCount {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let value = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .map(|(_, value)| *value)
                        .sum()
                })
                .unwrap_or_default();
            CommandResponse::Integer { value }
        }
        Command::RiskQuery {
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskDetail {
            key,
            start_ms,
            end_ms,
            count,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::FeaturePoints { points: Vec::new() },
                    mutated,
                };
            }
            let points = shard
                .risk
                .get(&key)
                .map(|series| {
                    series
                        .range(start_ms..=end_ms)
                        .take(count.unwrap_or(usize::MAX))
                        .map(|(timestamp_ms, amount)| FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value: amount.to_string().into_bytes(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
        }
        Command::RiskSet {
            family,
            key,
            timestamp_ms,
            amount,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            *shard
                .risk
                .entry(key.clone())
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskSetAndGet {
            family,
            key,
            timestamp_ms,
            amount,
            start_ms,
            end_ms,
            aggregator,
        } => {
            remove_if_expired(shard, &key);
            let key = risk_family_key(family, &key);
            let series = shard.risk.entry(key.clone()).or_default();
            *series.entry(timestamp_ms).or_default() += amount;
            let values = series
                .range(start_ms..=end_ms)
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
            persist_risk_page(
                cache,
                page_store,
                shard_id,
                shard,
                &key,
                start_routing_slot,
                end_routing_slot,
                async_storage,
            );
            mutated = true;
            CommandResponse::Integer {
                value: aggregate_risk_values(&values, &aggregator),
            }
        }
        Command::RiskFamilyQuery {
            family,
            key,
            start_ms,
            end_ms,
            aggregator,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            let key = risk_family_key(family, &key);
            if is_risk_change_aggregator(&aggregator) {
                CommandResponse::Integer {
                    value: count_risk_changes(shard, &key, start_ms, end_ms),
                }
            } else {
                let values = shard
                    .risk
                    .get(&key)
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(_, value)| *value)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                CommandResponse::Integer {
                    value: aggregate_risk_values(&values, &aggregator),
                }
            }
        }
        Command::RiskFolSet {
            key,
            value,
            occur_time_ms,
            ttl_ms,
            fol_type,
        } => {
            remove_if_expired(shard, &key);
            let should_store = shard
                .risk_fol
                .get(&key)
                .map(|existing| match fol_type {
                    RiskFolType::First => occur_time_ms < existing.occur_time_ms,
                    RiskFolType::Last => occur_time_ms > existing.occur_time_ms,
                })
                .unwrap_or(true);
            if should_store {
                shard.risk_fol.insert(
                    key.clone(),
                    RiskFolValue {
                        occur_time_ms,
                        value,
                        fol_type,
                    },
                );
            }
            if ttl_ms > 0 {
                shard
                    .expires_at_ms
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
            mutated = true;
            CommandResponse::Empty
        }
        Command::RiskFolQuery { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard.risk_fol.get(&key).map(|stored| stored.value.clone()),
            }
        }
        Command::RiskManager { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
                let family_key = risk_family_key(family, &key);
                let values = shard
                    .risk
                    .get(&family_key)
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                entries.push((
                    format!("{}_events", risk_family_name(family)),
                    values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{}_sum", risk_family_name(family)),
                    values.iter().sum::<i64>().to_string().into_bytes(),
                ));
            }
            if let Some(fol) = shard.risk_fol.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.fol_type {
                        RiskFolType::First => b"first".to_vec(),
                        RiskFolType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::RiskDebug {
            key,
            start_ms,
            end_ms,
        } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            let mut entries = Vec::new();
            entries.push(("key".to_string(), key.as_bytes().to_vec()));
            entries.push(("start_ms".to_string(), start_ms.to_string().into_bytes()));
            entries.push(("end_ms".to_string(), end_ms.to_string().into_bytes()));
            for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
                let family_key = risk_family_key(family, &key);
                let name = risk_family_name(family);
                let series = shard.risk.get(&family_key);
                let all_values = series
                    .map(|series| series.values().copied().collect::<Vec<_>>())
                    .unwrap_or_default();
                let window = series
                    .map(|series| {
                        series
                            .range(start_ms..=end_ms)
                            .map(|(timestamp_ms, value)| (*timestamp_ms, *value))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                entries.push((
                    format!("{name}_events"),
                    all_values.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_sum"),
                    all_values.iter().sum::<i64>().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_events"),
                    window.len().to_string().into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_sum"),
                    window
                        .iter()
                        .map(|(_, value)| *value)
                        .sum::<i64>()
                        .to_string()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_first_timestamp_ms"),
                    window
                        .first()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
                entries.push((
                    format!("{name}_window_last_timestamp_ms"),
                    window
                        .last()
                        .map(|(timestamp_ms, _)| timestamp_ms.to_string())
                        .unwrap_or_default()
                        .into_bytes(),
                ));
            }
            if let Some(fol) = shard.risk_fol.get(&key) {
                entries.push(("fol_value".to_string(), fol.value.clone()));
                entries.push((
                    "fol_occur_time_ms".to_string(),
                    fol.occur_time_ms.to_string().into_bytes(),
                ));
                entries.push((
                    "fol_type".to_string(),
                    match fol.fol_type {
                        RiskFolType::First => b"first".to_vec(),
                        RiskFolType::Last => b"last".to_vec(),
                    },
                ));
            }
            CommandResponse::HashEntries { entries }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            let object_key = context_node_key(tenant_hash, node.node_hash);
            let object_id =
                stable_page_object_id(shard_id, "hash", &object_key, Some(CONTEXT_NODE_FIELD));
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            let bytes = context_bytes(&node);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &bytes,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard
                    .hashes
                    .entry(object_key.clone())
                    .or_default()
                    .insert(CONTEXT_NODE_FIELD.to_string(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            let object_key = context_node_key(tenant_hash, node_hash);
            let node = shard
                .hashes
                .get(&object_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .or_else(|| shard.context_nodes.get(&object_key))
                .and_then(|address| {
                    read_page_bytes(cache, page_store, shard_id, address)
                        .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            CommandResponse::ContextNode { object_key, node }
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            first_write_only,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            let timeline_key = context_timeline_key(event.event_time_ms, event.event_id_hash);
            let series = shard.context_events.entry(object_key.clone()).or_default();
            if !(first_write_only && series.contains_key(&timeline_key)) {
                let value = context_bytes(&event);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value,
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            first_write_only,
        } => {
            let event_object_key = context_event_key(tenant_hash, node_hash);
            let event_timeline_key = context_timeline_key(event.event_time_ms, event.event_id_hash);
            let event_series = shard
                .context_events
                .entry(event_object_key.clone())
                .or_default();
            if !(first_write_only && event_series.contains_key(&event_timeline_key)) {
                let value = context_bytes(&event);
                let routing_slot =
                    page_routing_slot(&event_object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_event",
                    &event_object_key,
                    vec![FeaturePoint {
                        timestamp_ms: event_timeline_key,
                        value,
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    for (timestamp_ms, address) in addresses {
                        event_series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &event_object_key);

            let index_ref = ContextIndexRef {
                primary_node_hash: node_hash,
                primary_event_time_ms: event.event_time_ms,
                event_id_hash: event.event_id_hash,
            };
            let mut index_object_keys = Vec::new();
            let mut write_default_index =
                |index_name: &str, value_hash: u64, index_time_ms: u64| {
                    if value_hash == 0 || index_time_ms == 0 {
                        return;
                    }
                    let object_key =
                        context_index_key(tenant_hash, index_name, value_hash, indexes.scope_hash);
                    let timeline_key = context_timeline_key(index_time_ms, index_ref.event_id_hash);
                    let value = context_bytes(&index_ref);
                    let routing_slot =
                        page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                    if let Ok(addresses) = append_timestamped_kv_pages(
                        cache,
                        page_store,
                        shard_id,
                        "context_index",
                        &object_key,
                        vec![FeaturePoint {
                            timestamp_ms: timeline_key,
                            value,
                        }],
                        routing_slot,
                        async_storage,
                    ) {
                        let series = shard.context_indexes.entry(object_key.clone()).or_default();
                        for (timestamp_ms, address) in addresses {
                            series.insert(timestamp_ms, address);
                            mutated = true;
                        }
                        invalidate_record_all(cache, shard_id, &object_key);
                        index_object_keys.push(object_key);
                    }
                };

            if !context_index_disabled(&indexes, InternalContextIndex::EventKind) {
                write_default_index(
                    "event_kind",
                    context_event_kind_hash(&event),
                    event.event_time_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Status) {
                write_default_index("status", indexes.status_hash, event.event_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Source) {
                write_default_index("source", indexes.source_hash, event.event_time_ms);
            }
            if !context_index_disabled(&indexes, InternalContextIndex::EventTimeBucket) {
                write_default_index(
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.event_time_bucket_ms,
                );
            }
            if !context_index_disabled(&indexes, InternalContextIndex::Entity) {
                for entity_hash in &indexes.entity_hashes {
                    write_default_index("entity", *entity_hash, event.event_time_ms);
                }
            }
            CommandResponse::ContextExtractedEventWrite {
                event_object_key,
                written_index_count: index_object_keys.len(),
                index_object_keys,
            }
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            current_valid_only,
            as_of_ms,
            kinds,
            statuses,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_event_key(tenant_hash, node_hash);
            let events = shard
                .context_events
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextEvent>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .filter(|event| {
                            context_event_matches_filter(
                                event,
                                current_valid_only,
                                as_of_ms,
                                end_time_ms,
                                &kinds,
                                &statuses,
                                min_confidence,
                                min_importance,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextEvents { object_key, events }
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            event_time_ms,
            index_ref,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            let timeline_key = context_timeline_key(event_time_ms, index_ref.event_id_hash);
            let value = context_bytes(&index_ref);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_index",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard.context_indexes.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key =
                context_index_key(tenant_hash, &index_name, index_value_hash, scope_hash);
            let refs = shard
                .context_indexes
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextIndexRef>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextIndexRefs { object_key, refs }
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            let object_key = context_audit_key(tenant_hash, audit.session_hash);
            let timeline_key =
                context_timeline_key(audit.request_time_ms, stable_object_hash(&audit.query_id));
            let value = context_bytes(&audit);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_audit",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard.context_audits.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key = context_audit_key(tenant_hash, session_hash);
            let audits = shard
                .context_audits
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextPackAudit>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextPackAudits { object_key, audits }
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => {
            let object_key = context_dirty_key(tenant_hash, marker.node_hash);
            let timeline_key = context_timeline_key(marker.event_time_ms, marker.node_hash);
            let value = context_bytes(&marker);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_dirty",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value,
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard.context_dirty.entry(object_key.clone()).or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let object_key = context_dirty_key(tenant_hash, node_hash);
            let markers = shard
                .context_dirty
                .get(&object_key)
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(start_time_ms)
                                ..context_timeline_end(end_time_ms),
                        )
                        .take(context_limit(limit))
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextSummaryDirtyMarker>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::ContextSummaryDirtyMarkers {
                object_key,
                markers,
            }
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            let object_key = context_entity_key(tenant_hash, entity.node_hash, entity.entity_hash);
            let object_id = stable_page_object_id(shard_id, "context_entity", &object_key, None);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            let bytes = context_bytes(&entity);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &bytes,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.context_entities.insert(object_key.clone(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            let object_key = context_entity_key(tenant_hash, node_hash, entity_hash);
            let entity = shard.context_entities.get(&object_key).and_then(|address| {
                read_page_bytes(cache, page_store, shard_id, address)
                    .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
            });
            CommandResponse::ContextEntity { object_key, entity }
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            let object_key = context_entity_collection_key(tenant_hash, node_hash);
            let entities = entity_hashes
                .iter()
                .copied()
                .take(context_limit(limit))
                .filter_map(|entity_hash| {
                    let entity_key = context_entity_key(tenant_hash, node_hash, entity_hash);
                    shard.context_entities.get(&entity_key).and_then(|address| {
                        read_page_bytes(cache, page_store, shard_id, address)
                            .and_then(|bytes| context_from_bytes::<ContextEntity>(&bytes))
                    })
                })
                .collect();
            CommandResponse::ContextEntities {
                object_key,
                entities,
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            let object_key = context_child_key(tenant_hash, child_ref.parent_hash);
            let existing = load_context_children(cache, page_store, shard_id, shard, &object_key);
            let created = existing
                .iter()
                .all(|stored| stored.child_hash != child_ref.child_hash);
            if created {
                let timeline_key =
                    context_timeline_key(child_ref.updated_at_ms, child_ref.child_hash);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_child",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&child_ref),
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    let series = shard
                        .context_children
                        .entry(object_key.clone())
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            let count =
                load_context_children(cache, page_store, shard_id, shard, &object_key).len();
            CommandResponse::ContextChildRefs {
                object_key,
                refs: Vec::new(),
                created: Some(created),
                parent_child_count: Some(count as u32),
            }
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            let object_key = context_child_key(tenant_hash, parent_hash);
            let mut refs = load_context_children(cache, page_store, shard_id, shard, &object_key);
            refs.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            refs.truncate(context_limit(limit));
            CommandResponse::ContextChildRefs {
                object_key,
                refs,
                created: None,
                parent_child_count: None,
            }
        }
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => {
            let object_key = context_embedding_key(tenant_hash, embedding.ref_hash);
            let object_id = stable_page_object_id(shard_id, "context_embedding", &object_key, None);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &context_bytes(&embedding),
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
                shard.context_embeddings.insert(object_key.clone(), address);
                mutated = true;
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes,
            limit,
        } => {
            let embeddings = ref_hashes
                .iter()
                .copied()
                .filter(|ref_hash| *ref_hash != 0)
                .take(context_limit(limit))
                .filter_map(|ref_hash| {
                    let object_key = context_embedding_key(tenant_hash, ref_hash);
                    shard
                        .context_embeddings
                        .get(&object_key)
                        .and_then(|address| {
                            read_page_bytes(cache, page_store, shard_id, address)
                                .and_then(|bytes| context_from_bytes::<ContextEmbedding>(&bytes))
                        })
                })
                .collect();
            CommandResponse::ContextEmbeddings { embeddings }
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            leaf_only,
        } => {
            let nodes = traverse_context_tree(
                cache,
                page_store,
                shard_id,
                shard,
                tenant_hash,
                start_node_hash,
                &query_vector,
                max_depth,
                top_k_per_depth,
                max_children_scored_per_parent,
                max_candidate_nodes,
                leaf_only,
            );
            CommandResponse::ContextTraversedNodes { nodes }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            let object_key = context_summary_key(tenant_hash, summary.node_hash, summary.level);
            let timeline_key =
                context_timeline_key(summary.valid_from_ms, u64::from(summary.level));
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_summary",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&summary),
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard
                    .context_summaries
                    .entry(object_key.clone())
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            let object_key = context_summary_key(tenant_hash, node_hash, level);
            let mut summaries = load_context_summaries(
                cache,
                page_store,
                shard_id,
                shard,
                &object_key,
                as_of_ms,
                limit,
            );
            summaries.sort_by_key(|summary| summary.valid_from_ms);
            CommandResponse::ContextSummaries {
                object_key,
                summaries,
            }
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            let object_key = context_compression_key(tenant_hash, event.node_hash);
            let timeline_key =
                context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
            let routing_slot = page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
            if let Ok(addresses) = append_timestamped_kv_pages(
                cache,
                page_store,
                shard_id,
                "context_compression",
                &object_key,
                vec![FeaturePoint {
                    timestamp_ms: timeline_key,
                    value: context_bytes(&event),
                }],
                routing_slot,
                async_storage,
            ) {
                let series = shard
                    .context_compressions
                    .entry(object_key.clone())
                    .or_default();
                for (timestamp_ms, address) in addresses {
                    series.insert(timestamp_ms, address);
                    mutated = true;
                }
            }
            invalidate_record_all(cache, shard_id, &object_key);
            CommandResponse::ContextObjectKey { object_key }
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            let mut events = load_context_compression_events(
                cache,
                page_store,
                shard_id,
                shard,
                tenant_hash,
                &node_hashes,
                start_time_ms,
                end_time_ms,
                limit,
            );
            let object_key = node_hashes
                .iter()
                .find(|node_hash| **node_hash != 0)
                .map(|node_hash| context_compression_key(tenant_hash, *node_hash))
                .unwrap_or_else(|| context_compression_key(tenant_hash, 0));
            CommandResponse::ContextCompressionEvents {
                object_key,
                events: {
                    events.truncate(context_limit(limit));
                    events
                },
                source_event_count: None,
                truncated_source_events: None,
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            let object_key = context_compression_key(tenant_hash, node_hash);
            let source_limit = context_limit(max_source_events);
            let mut selected = shard
                .context_events
                .get(&context_event_key(tenant_hash, node_hash))
                .map(|series| {
                    series
                        .range(
                            context_timeline_start(source_start_ms)
                                ..context_timeline_end(source_end_ms),
                        )
                        .filter_map(|(timeline_key, address)| {
                            read_context_value::<ContextEvent>(
                                cache,
                                page_store,
                                shard_id,
                                *timeline_key,
                                address,
                            )
                        })
                        .filter(|event| {
                            event.confidence >= min_confidence && event.importance >= min_importance
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            selected.sort_by_key(|event| (event.event_time_ms, event.event_id_hash));
            let truncated = selected.len() > source_limit;
            selected.truncate(source_limit);
            if selected.is_empty() {
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: Vec::new(),
                    source_event_count: Some(0),
                    truncated_source_events: Some(false),
                }
            } else {
                let event = build_context_compression_event(
                    tenant_hash,
                    node_hash,
                    source_start_ms,
                    source_end_ms,
                    compressed_time_ms,
                    &selected,
                    truncated,
                );
                let timeline_key =
                    context_timeline_key(event.compressed_time_ms, event.compression_id_hash);
                let routing_slot =
                    page_routing_slot(&object_key, start_routing_slot, end_routing_slot);
                if let Ok(addresses) = append_timestamped_kv_pages(
                    cache,
                    page_store,
                    shard_id,
                    "context_compression",
                    &object_key,
                    vec![FeaturePoint {
                        timestamp_ms: timeline_key,
                        value: context_bytes(&event),
                    }],
                    routing_slot,
                    async_storage,
                ) {
                    let series = shard
                        .context_compressions
                        .entry(object_key.clone())
                        .or_default();
                    for (timestamp_ms, address) in addresses {
                        series.insert(timestamp_ms, address);
                        mutated = true;
                    }
                }
                invalidate_record_all(cache, shard_id, &object_key);
                CommandResponse::ContextCompressionEvents {
                    object_key,
                    events: vec![event],
                    source_event_count: Some(selected.len() as u32),
                    truncated_source_events: Some(truncated),
                }
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            let node_key = context_node_key(tenant_hash, node_hash);
            let node = shard
                .hashes
                .get(&node_key)
                .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
                .or_else(|| shard.context_nodes.get(&node_key))
                .and_then(|address| {
                    read_page_bytes(cache, page_store, shard_id, address)
                        .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
                });
            let level = summary_level.unwrap_or(1).max(1);
            let summary_key = context_summary_key(tenant_hash, node_hash, level);
            let latest_summary = load_latest_context_summary(
                cache,
                page_store,
                shard_id,
                shard,
                &summary_key,
                as_of_ms,
            );
            let cold_window_summaries = if cold_start_time_ms == 0 && cold_end_time_ms == 0 {
                Vec::new()
            } else {
                load_context_compression_events(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    &[node_hash],
                    cold_start_time_ms,
                    cold_end_time_ms,
                    compression_limit,
                )
            };
            CommandResponse::ContextNodeContext {
                node_exists: node.is_some(),
                node,
                overall_summary_exists: latest_summary.is_some(),
                overall_summary: latest_summary,
                cold_window_summaries,
            }
        }
    };
    ExecuteOutcome { response, mutated }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn ttl_ms(shard: &mut ShardState, key: &str) -> i64 {
    if remove_if_expired(shard, key) {
        return -2;
    }
    if !record_exists(shard, key) {
        return -2;
    }
    associated_record_keys(key)
        .into_iter()
        .filter_map(|record_key| shard.expires_at_ms.get(&record_key).copied())
        .map(|expires_at| expires_at.saturating_sub(now_ms()) as i64)
        .min()
        .unwrap_or(-1)
}

fn select_expiry_cursor_window(
    keys: Vec<(String, u64)>,
    cursor: Option<&str>,
    limit: usize,
) -> (Vec<(String, u64)>, Option<String>) {
    let start = cursor
        .and_then(|cursor| keys.iter().position(|(key, _)| key.as_str() > cursor))
        .unwrap_or_default();
    let remaining = keys.into_iter().skip(start).collect::<Vec<_>>();
    if limit == 0 || remaining.len() <= limit {
        return (remaining, None);
    }
    let mut selected = remaining.into_iter().take(limit).collect::<Vec<_>>();
    let next_cursor = selected.last().map(|(key, _)| key.clone());
    (std::mem::take(&mut selected), next_cursor)
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    let now = now_ms();
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        if shard
            .expires_at_ms
            .get(&record_key)
            .map(|expires_at| *expires_at <= now)
            .unwrap_or(false)
        {
            removed |= delete_record_exact(shard, &record_key);
        }
    }
    removed
}

fn delete_record(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    for record_key in associated_record_keys(key) {
        removed |= delete_record_exact(shard, &record_key);
    }
    removed
}

fn delete_record_exact(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.features.remove(key).is_some();
    removed |= shard.sequences.remove(key).is_some();
    removed |= shard.ips.remove(key).is_some();
    removed |= shard.ips_meta.remove(key).is_some();
    removed |= shard.ips_request_ids.remove(key).is_some();
    removed |= shard.risk.remove(key).is_some();
    removed |= shard.risk_pages.remove(key).is_some();
    removed |= shard.risk_changes.remove(key).is_some();
    removed |= shard.risk_fol.remove(key).is_some();
    removed |= shard.context_nodes.remove(key).is_some();
    removed |= shard.context_events.remove(key).is_some();
    removed |= shard.context_indexes.remove(key).is_some();
    removed |= shard.context_audits.remove(key).is_some();
    removed |= shard.context_dirty.remove(key).is_some();
    removed |= shard.context_entities.remove(key).is_some();
    removed |= shard.context_children.remove(key).is_some();
    removed |= shard.context_embeddings.remove(key).is_some();
    removed |= shard.context_summaries.remove(key).is_some();
    removed |= shard.context_compressions.remove(key).is_some();
    removed
}

fn associated_record_keys(key: &str) -> Vec<String> {
    if key.starts_with("risk:") {
        return vec![key.to_string()];
    }
    let mut keys = Vec::with_capacity(4);
    keys.push(key.to_string());
    for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
        keys.push(risk_family_key(family, key));
    }
    keys
}

fn collect_live_page_segment_ids(shard: &ShardState) -> BTreeSet<u64> {
    let mut ids = BTreeSet::new();
    ids.extend(
        shard
            .strings
            .values()
            .map(|address| address.page_segment_id),
    );
    for fields in shard.hashes.values() {
        ids.extend(fields.values().map(|address| address.page_segment_id));
    }
    for members in shard.sets.values() {
        ids.extend(members.values().map(|address| address.page_segment_id));
    }
    for series in shard.features.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.sequences.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.ips.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_nodes
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_events.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_indexes.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_audits.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_dirty.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_entities
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_children.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids.extend(
        shard
            .context_embeddings
            .values()
            .map(|address| address.page_segment_id),
    );
    for series in shard.context_summaries.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    for series in shard.context_compressions.values() {
        ids.extend(series.values().map(|address| address.page_segment_id));
    }
    ids
}

fn storage_segment_integrity_report(
    shard_id: ShardId,
    recovery: &StorageRecoveryReport,
    boundary: &StorageRecoveryBoundaryReport,
) -> StorageSegmentIntegrityReport {
    let indexed_page_segment_count = recovery.active_page_segment_ids.len();
    let discovered_page_segment_count = recovery.page_segment_reports.len();
    let live_page_segment_count = recovery.live_page_segment_ids.len();
    let orphan_page_segment_count = boundary.orphan_page_segment_ids.len();
    let stale_page_ref_count = boundary.stale_index_page_refs.len();
    let corrupt_page_segment_count = boundary.corrupt_page_segment_ids.len();
    let unreadable_page_ref_count = recovery.unreadable_page_refs.len();
    let unreadable_page_bytes = boundary.unreadable_page_bytes;
    let owner_mismatch_page_ref_count = boundary.owner_mismatch_page_refs.len();
    let missing_owner_page_ref_count = boundary.missing_owner_page_refs;
    let reclaim_required = orphan_page_segment_count > 0
        || recovery
            .page_segment_live_reports
            .iter()
            .any(|report| report.stale_page_estimate > 0);
    let integrity_ok = stale_page_ref_count == 0
        && corrupt_page_segment_count == 0
        && unreadable_page_ref_count == 0
        && unreadable_page_bytes == 0
        && owner_mismatch_page_ref_count == 0
        && missing_owner_page_ref_count == 0
        && recovery.all_live_pages_readable;

    StorageSegmentIntegrityReport {
        shard_id,
        indexed_page_segment_count,
        discovered_page_segment_count,
        live_page_segment_count,
        orphan_page_segment_count,
        stale_page_ref_count,
        corrupt_page_segment_count,
        unreadable_page_ref_count,
        unreadable_page_bytes,
        owner_mismatch_page_ref_count,
        missing_owner_page_ref_count,
        reclaim_required,
        integrity_ok,
    }
}

fn storage_reclaim_candidates_from_recovery(
    recovery: &StorageRecoveryReport,
    fully_stale_segment_ids: &BTreeSet<u64>,
) -> Vec<StorageReclaimCandidate> {
    let mut candidates = recovery
        .page_segment_live_reports
        .iter()
        .filter_map(|report| {
            let fully_stale = fully_stale_segment_ids.contains(&report.page_segment_id);
            let stale_page_estimate = if fully_stale {
                report.page_count
            } else {
                report.stale_page_estimate
            };
            let stale_physical_bytes = if fully_stale {
                report.physical_bytes
            } else {
                report
                    .physical_bytes
                    .saturating_sub(report.live_physical_bytes)
            };
            if stale_page_estimate == 0 && stale_physical_bytes == 0 {
                return None;
            }
            let reclaim_score = stale_physical_bytes
                .saturating_mul(10_000_u64.saturating_sub(report.live_ref_density_basis_points))
                .saturating_div(10_000)
                .saturating_add(stale_page_estimate);
            Some(StorageReclaimCandidate {
                page_segment_id: report.page_segment_id,
                physical_bytes: report.physical_bytes,
                live_physical_bytes: report.live_physical_bytes,
                stale_physical_bytes,
                page_count: report.page_count,
                live_page_refs: report.live_page_refs,
                stale_page_estimate,
                live_ref_density_basis_points: report.live_ref_density_basis_points,
                reclaim_score,
                reason: if fully_stale {
                    "orphan_segment".to_string()
                } else {
                    "low_live_density".to_string()
                },
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .reclaim_score
            .cmp(&left.reclaim_score)
            .then_with(|| right.stale_physical_bytes.cmp(&left.stale_physical_bytes))
            .then_with(|| left.page_segment_id.cmp(&right.page_segment_id))
    });
    candidates
}

fn annotate_storage_manager_admin_stage_fields(
    stages: &mut [StorageManagerStageReport],
    last_run_unix_ms: u64,
    duration_ms: u64,
    errors: &[String],
    retention_blockers: usize,
) {
    for stage in stages {
        stage.last_run_unix_ms = last_run_unix_ms;
        stage.duration_ms = duration_ms;
        if stage.skipped && stage.skipped_reason.is_empty() {
            stage.skipped_reason = stage.reason.clone();
        }
        if !errors.is_empty() {
            let prefix = format!("{}:", stage.stage);
            stage.errors = errors
                .iter()
                .filter(|error| error.starts_with(&prefix))
                .cloned()
                .collect();
        }
        stage.bytes_reclaimed = stage
            .page_bytes_reclaimed
            .max(stage.cache_disk_bytes_removed)
            .max(stage.before_bytes.saturating_sub(stage.after_bytes));
        stage.pages_compacted = stage.rewritten_page_refs;
        if stage.wal_floor_sequence == 0 {
            stage.wal_floor_sequence = stage.retain_from_wal_sequence;
        }
        if stage.index_log_floor_sequence == 0 {
            stage.index_log_floor_sequence = stage.retain_from_index_log_sequence;
        }
        if stage.retention_blockers == 0 {
            stage.retention_blockers = retention_blockers;
        }
        if stage.pressure_before == 0 {
            stage.pressure_before = stage.eviction_pressure_before.max(stage.before_bytes);
        }
        if stage.pressure_after == 0 {
            stage.pressure_after = stage.eviction_pressure_after.max(stage.after_bytes);
        }
    }
}

#[derive(Debug, Clone)]
struct LivePageEntry {
    object_key: String,
    kind: String,
    component: Option<String>,
    address: PageAddress,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

#[derive(Debug, Default)]
struct StoragePageOwnershipValidation {
    mismatches: Vec<StorageRecoveryPageOwnerMismatch>,
    missing_owner_page_refs: usize,
}

fn live_page_entry(
    object_key: impl Into<String>,
    kind: impl Into<String>,
    component: Option<String>,
    address: PageAddress,
) -> LivePageEntry {
    LivePageEntry {
        object_key: object_key.into(),
        kind: kind.into(),
        component,
        address,
        dirty: false,
        deleted: false,
        log_backed: true,
    }
}

fn collect_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    if !shard.slot_index.slots.is_empty() {
        return collect_slot_index_live_page_entries(shard);
    }
    collect_model_live_page_entries(shard)
}

fn collect_slot_index_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    for slot in shard.slot_index.slots.values() {
        for page in slot.page_refs.values() {
            entries.push(LivePageEntry {
                object_key: page.object_key.clone(),
                kind: page.model_id.clone(),
                component: page.component.clone(),
                address: page.address.clone(),
                dirty: page.dirty,
                deleted: page.deleted,
                log_backed: page.log_backed,
            });
        }
    }
    entries
}

fn collect_model_live_page_entries(shard: &ShardState) -> Vec<LivePageEntry> {
    let mut entries = Vec::new();
    entries.extend(
        shard
            .strings
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "string", None, address.clone())),
    );
    for (key, fields) in &shard.hashes {
        entries.extend(fields.iter().map(|(field, address)| {
            live_page_entry(key.clone(), "hash", Some(field.clone()), address.clone())
        }));
    }
    for (key, members) in &shard.sets {
        entries.extend(members.iter().map(|(member, address)| {
            live_page_entry(
                key.clone(),
                "set",
                Some(hex::encode(member)),
                address.clone(),
            )
        }));
    }
    for (key, series) in &shard.features {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "feature", None, address)),
        );
    }
    for (key, series) in &shard.sequences {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "sequence", None, address)),
        );
    }
    for (key, series) in &shard.ips {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "ips", None, address)),
        );
    }
    entries.extend(
        shard
            .risk_pages
            .iter()
            .map(|(key, address)| live_page_entry(key.clone(), "risk", None, address.clone())),
    );
    entries.extend(
        shard.context_nodes.iter().map(|(key, address)| {
            live_page_entry(key.clone(), "context_node", None, address.clone())
        }),
    );
    for (key, series) in &shard.context_events {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_event", None, address)),
        );
    }
    for (key, series) in &shard.context_indexes {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_index", None, address)),
        );
    }
    for (key, series) in &shard.context_audits {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_audit", None, address)),
        );
    }
    for (key, series) in &shard.context_dirty {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_dirty", None, address)),
        );
    }
    entries.extend(shard.context_entities.iter().map(|(key, address)| {
        live_page_entry(key.clone(), "context_entity", None, address.clone())
    }));
    for (key, series) in &shard.context_children {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_child", None, address)),
        );
    }
    entries.extend(shard.context_embeddings.iter().map(|(key, address)| {
        live_page_entry(key.clone(), "context_embedding", None, address.clone())
    }));
    for (key, series) in &shard.context_summaries {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_summary", None, address)),
        );
    }
    for (key, series) in &shard.context_compressions {
        entries.extend(
            unique_timestamped_kv_page_addresses(series)
                .into_iter()
                .map(|address| live_page_entry(key.clone(), "context_compression", None, address)),
        );
    }
    entries
}

fn page_index_ref_key(entry: &LivePageEntry) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        entry.kind,
        entry.object_key,
        entry.component.as_deref().unwrap_or(""),
        entry.address.page_segment_id,
        entry.address.offset
    )
}

fn upsert_slot_index_page(
    shard: &mut ShardState,
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    address: PageAddress,
    dirty: bool,
) {
    let routing_slot = address
        .routing_slot
        .unwrap_or_else(|| page_routing_slot(object_key, 0, u32::MAX));
    let object_id = address
        .object_id
        .unwrap_or_else(|| stable_page_object_id(shard_id, kind, object_key, component.as_deref()));
    let entry = LivePageEntry {
        object_key: object_key.to_string(),
        kind: kind.to_string(),
        component,
        address,
        dirty,
        deleted: false,
        log_backed: true,
    };
    for slot in shard.slot_index.slots.values_mut() {
        slot.page_refs.retain(|_, page| {
            !(page.object_key == entry.object_key
                && page.model_id == entry.kind
                && page.component == entry.component)
        });
        if !slot
            .page_refs
            .values()
            .any(|page| page.object_id == object_id)
        {
            slot.object_ids.remove(&object_id);
        }
        update_slot_layout(slot);
    }
    let slot = shard
        .slot_index
        .slots
        .entry(routing_slot)
        .or_insert_with(|| SlotNodeIndex {
            routing_slot,
            meta_loaded: true,
            in_memory: true,
            ..SlotNodeIndex::default()
        });
    slot.dirty |= dirty;
    slot.in_memory = true;
    slot.object_ids.insert(object_id);
    slot.page_refs.insert(
        page_index_ref_key(&entry),
        PageIndexEntry {
            object_key: entry.object_key,
            model_id: entry.kind,
            component: entry.component,
            object_id,
            address: entry.address,
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
        },
    );
    update_slot_layout(slot);
}

fn classify_slot_layout(object_count: usize, page_ref_count: usize) -> SlotLayoutState {
    match (object_count, page_ref_count) {
        (0, _) => SlotLayoutState::Empty,
        (1, 0) => SlotLayoutState::SingleObject,
        (_, 0) => SlotLayoutState::Empty,
        (1, 1) => SlotLayoutState::SinglePageObject,
        (1, _) => SlotLayoutState::MultiPageObject,
        _ => SlotLayoutState::MultiObject,
    }
}

fn slot_layout_name(layout: SlotLayoutState) -> &'static str {
    match layout {
        SlotLayoutState::Empty => "empty",
        SlotLayoutState::SingleObject => "single_object",
        SlotLayoutState::SinglePageObject => "single_page_object",
        SlotLayoutState::MultiPageObject => "multi_page_object",
        SlotLayoutState::MultiObject => "multi_object",
    }
}

fn update_slot_layout(slot: &mut SlotNodeIndex) {
    slot.object_ids = slot
        .page_refs
        .values()
        .filter(|page| !page.deleted)
        .map(|page| page.object_id)
        .collect();
    slot.layout = classify_slot_layout(slot.object_ids.len(), slot.page_refs.len());
}

fn rebuild_slot_first_index(
    shard: &mut ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) {
    let mut slot_index = SlotFirstIndex::default();
    for entry in collect_model_live_page_entries(shard) {
        let routing_slot = entry.address.routing_slot.unwrap_or_else(|| {
            page_routing_slot(&entry.object_key, start_routing_slot, end_routing_slot)
        });
        let object_id = entry.address.object_id.unwrap_or_else(|| {
            stable_page_object_id(
                0,
                &entry.kind,
                &entry.object_key,
                entry.component.as_deref(),
            )
        });
        let slot = slot_index
            .slots
            .entry(routing_slot)
            .or_insert_with(|| SlotNodeIndex {
                routing_slot,
                meta_loaded: true,
                in_memory: true,
                ..SlotNodeIndex::default()
            });
        let page_dirty = shard.dirty_objects.contains(&entry.object_key) || entry.dirty;
        slot.dirty |= page_dirty;
        slot.in_memory |= true;
        slot.object_ids.insert(object_id);
        slot.page_refs.insert(
            page_index_ref_key(&entry),
            PageIndexEntry {
                object_key: entry.object_key,
                model_id: entry.kind,
                component: entry.component,
                object_id,
                address: entry.address,
                dirty: page_dirty,
                deleted: entry.deleted,
                log_backed: entry.log_backed,
            },
        );
        update_slot_layout(slot);
    }
    shard.slot_index = slot_index;
}

fn expected_live_page_object_id(shard_id: ShardId, entry: &LivePageEntry) -> u64 {
    stable_page_object_id(
        shard_id,
        &entry.kind,
        &entry.object_key,
        entry.component.as_deref(),
    )
}

fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_slots(shard_id, shard, &BTreeSet::new(), |_| 0)
}

fn storage_object_lifecycle_report_for_slots(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let entries = collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_page_refs = 0u64;
    let mut owner_mismatch_page_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_page_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        if entry.address.object_id.is_none() || entry.address.routing_slot.is_none() {
            missing_owner_page_refs = missing_owner_page_refs.saturating_add(1);
        }
        match entry.address.object_id {
            Some(actual_object_id) => {
                actual_object_owners
                    .entry(actual_object_id)
                    .or_default()
                    .insert(expected_object_id);
                if actual_object_id != expected_object_id {
                    owner_mismatch_page_refs = owner_mismatch_page_refs.saturating_add(1);
                }
            }
            None => {}
        }
    }

    let reused_object_ids = actual_object_owners
        .into_iter()
        .filter_map(|(actual_object_id, expected_ids)| {
            (expected_ids.len() > 1).then_some(actual_object_id)
        })
        .collect::<Vec<_>>();
    let tombstoned_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_slot = routing_slot_for_key(key);
            (selected_slots.is_empty() || selected_slots.contains(&routing_slot))
                && !record_exists(shard, key)
        })
        .cloned()
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_page_refs: entries.len() as u64,
        stale_object_ids: 0,
        tombstoned_object_ids: tombstoned_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_page_refs,
        owner_mismatch_page_refs,
        reused_object_ids,
        tombstoned_object_keys,
    }
}

fn slot_dump_entries_by_key(
    shard_id: ShardId,
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
    routing_slot_for_key: impl Fn(&str) -> u32,
) -> BTreeMap<String, PageAddress> {
    collect_live_page_entries(shard)
        .into_iter()
        .filter(|entry| {
            let routing_slot = entry
                .address
                .routing_slot
                .unwrap_or_else(|| routing_slot_for_key(&entry.object_key));
            selected_slots.is_empty() || selected_slots.contains(&routing_slot)
        })
        .map(|entry| {
            let component = entry.component.unwrap_or_default();
            let page_id = entry.address.page_id.unwrap_or_else(|| {
                stable_page_object_id(
                    shard_id,
                    &entry.kind,
                    &entry.object_key,
                    (!component.is_empty()).then_some(component.as_str()),
                )
            });
            (
                format!(
                    "{}:{}:{}:{}",
                    entry.kind, entry.object_key, component, page_id
                ),
                entry.address,
            )
        })
        .collect()
}

fn slot_storage_summaries(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> Vec<SlotStorageSummary> {
    let mut slots = BTreeMap::<u32, SlotStorageSummary>::new();
    let mut objects_by_slot = BTreeMap::<u32, BTreeSet<String>>::new();
    let mut page_segments_by_slot = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length);
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length);
        if let Some(zone_id) = entry.address.zone_id {
            summary.last_compacted_zone = Some(
                summary
                    .last_compacted_zone
                    .map_or(zone_id, |current| current.max(zone_id)),
            );
        }
        objects_by_slot
            .entry(routing_slot)
            .or_default()
            .insert(entry.object_key);
        page_segments_by_slot
            .entry(routing_slot)
            .or_default()
            .insert(entry.address.page_segment_id);
    }
    for key in &shard.dirty_objects {
        let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
        let summary = slots.entry(routing_slot).or_insert(SlotStorageSummary {
            routing_slot,
            ..SlotStorageSummary::default()
        });
        summary.dirty_object_count = summary.dirty_object_count.saturating_add(1);
        summary.dirty_generation = summary.dirty_generation.saturating_add(1);
    }
    for (routing_slot, summary) in &mut slots {
        summary.object_count = objects_by_slot
            .get(routing_slot)
            .map(|objects| objects.len() as u64)
            .unwrap_or_default();
        summary.page_segment_ids = page_segments_by_slot
            .get(routing_slot)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    slots.into_values().collect()
}

const CPP_PACKED_PAGE_INDEX_SIZE: usize = 17;
const CPP_PACKED_SLOT_NODE_SIZE: usize = 24;

fn storage_model_code(kind: &str) -> u8 {
    match kind {
        "string" => 1,
        "hash" => 2,
        "set" => 3,
        "feature" => 4,
        "sequence" => 5,
        "ips" => 6,
        "risk" => 7,
        "context_node" => 8,
        "context_event" => 9,
        "context_index" => 10,
        "context_audit" => 11,
        "context_dirty" => 12,
        "context_entity" => 13,
        "context_child" => 14,
        "context_embedding" => 15,
        "context_summary" => 16,
        "context_compression" => 17,
        _ => 0,
    }
}

fn physical_address_word(address: &PageAddress) -> u64 {
    address.page_segment_id.wrapping_shl(32) | (address.offset & u32::MAX as u64)
}

fn cpp_packed_page_index_bytes(
    page: &StoragePhysicalPageIndex,
) -> [u8; CPP_PACKED_PAGE_INDEX_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_PAGE_INDEX_SIZE];
    bytes[0] = page.object_id.unwrap_or_default() as u8;
    bytes[1] = storage_model_code(&page.model_id);
    bytes[2..4].copy_from_slice(&(page.page_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(page.dirty) | (u8::from(page.log_backed) << 1);
    let page_size = if page.deleted { 0 } else { page.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&PageAddress {
        page_segment_id: page.page_segment_id,
        offset: page.offset,
        length: page.length,
        page_id: page.page_id,
        object_id: page.object_id,
        routing_slot: Some(page.routing_slot),
        zone_id: page.zone_id,
        sha256: page.checksum.clone(),
    });
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

fn cpp_packed_slot_node_bytes(slot: &StoragePhysicalSlotNode) -> [u8; CPP_PACKED_SLOT_NODE_SIZE] {
    let mut bytes = [0u8; CPP_PACKED_SLOT_NODE_SIZE];
    let page_in_log = slot.page_indexes.iter().any(|page| page.log_backed);
    let trivial_page = slot.page_ref_count <= 1;
    let page_deleted = slot.page_ref_count == 0;
    let mut flags = 0u32;
    flags |= (slot.ttl_ms.is_some() as u32) << 1;
    flags |= (slot.dirty as u32) << 2;
    flags |= (slot.loading as u32) << 4;
    flags |= (slot.in_memory as u32) << 5;
    flags |= (slot.dirty as u32) << 6;
    flags |= (page_deleted as u32) << 7;
    flags |= (page_in_log as u32) << 8;
    flags |= (trivial_page as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(slot.physical_bytes as u32).to_le_bytes());
    let model_code = slot
        .page_indexes
        .first()
        .map(|page| storage_model_code(&page.model_id))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&slot.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = slot
        .page_indexes
        .first()
        .map(|page| page.page_segment_id.wrapping_shl(32) | (page.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}

fn storage_physical_index_report(
    shard_id: ShardId,
    shard: &ShardState,
    summaries: Vec<SlotStorageSummary>,
) -> StoragePhysicalIndexReport {
    let summary_by_slot = summaries
        .into_iter()
        .map(|summary| (summary.routing_slot, summary))
        .collect::<BTreeMap<_, _>>();
    let mut slots = summary_by_slot
        .iter()
        .map(|(routing_slot, summary)| {
            (
                *routing_slot,
                StoragePhysicalSlotNode {
                    routing_slot: *routing_slot,
                    layout: "empty".to_string(),
                    dirty: summary.dirty_object_count > 0,
                    meta_loaded: true,
                    loading: false,
                    in_memory: summary.page_ref_count > 0,
                    ttl_ms: None,
                    object_count: summary.object_count,
                    page_ref_count: summary.page_ref_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    dirty_generation: summary.dirty_generation,
                    last_dump_sequence: summary.last_dump_sequence,
                    cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                    cpp_packed_slot_node_hex: String::new(),
                    page_indexes: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let mut missing_routing_slot_count = 0usize;
    for entry in collect_live_page_entries(shard) {
        if entry.address.routing_slot.is_none() {
            missing_routing_slot_count = missing_routing_slot_count.saturating_add(1);
        }
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        let slot = slots
            .entry(routing_slot)
            .or_insert(StoragePhysicalSlotNode {
                routing_slot,
                layout: "empty".to_string(),
                meta_loaded: true,
                in_memory: true,
                cpp_packed_slot_node_len: CPP_PACKED_SLOT_NODE_SIZE,
                ..StoragePhysicalSlotNode::default()
            });
        let mut page_index = StoragePhysicalPageIndex {
            object_key: entry.object_key.clone(),
            model_id: entry.kind.clone(),
            component: entry.component.clone(),
            routing_slot,
            page_segment_id: entry.address.page_segment_id,
            offset: entry.address.offset,
            length: entry.address.length,
            page_id: entry.address.page_id,
            object_id: entry.address.object_id,
            zone_id: entry.address.zone_id,
            checksum: entry.address.sha256.clone(),
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
            cpp_packed_page_index_len: CPP_PACKED_PAGE_INDEX_SIZE,
            cpp_packed_page_index_hex: String::new(),
        };
        page_index.cpp_packed_page_index_hex =
            hex::encode(cpp_packed_page_index_bytes(&page_index));
        slot.page_indexes.push(page_index);
    }
    for slot in slots.values_mut() {
        slot.page_indexes.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.model_id.cmp(&right.model_id))
                .then(left.component.cmp(&right.component))
                .then(left.page_segment_id.cmp(&right.page_segment_id))
                .then(left.offset.cmp(&right.offset))
        });
        let object_count = slot
            .page_indexes
            .iter()
            .filter_map(|page| page.object_id)
            .collect::<BTreeSet<_>>()
            .len();
        slot.layout = slot_layout_name(classify_slot_layout(object_count, slot.page_indexes.len()))
            .to_string();
        slot.cpp_packed_slot_node_len = CPP_PACKED_SLOT_NODE_SIZE;
        slot.cpp_packed_slot_node_hex = hex::encode(cpp_packed_slot_node_bytes(slot));
    }
    let page_index_count = slots
        .values()
        .map(|slot| slot.page_indexes.len())
        .sum::<usize>();
    let page_indexes = slots
        .values()
        .flat_map(|slot| slot.page_indexes.iter())
        .collect::<Vec<_>>();
    let missing_object_id_count = page_indexes
        .iter()
        .filter(|page| page.object_id.is_none())
        .count();
    let missing_page_id_count = page_indexes
        .iter()
        .filter(|page| page.page_id.is_none())
        .count();
    let missing_checksum_count = page_indexes
        .iter()
        .filter(|page| page.checksum.is_none())
        .count();
    StoragePhysicalIndexReport {
        shard_id,
        slot_first: true,
        slot_index_authority: !shard.slot_index.slots.is_empty(),
        slot_count: slots.len(),
        page_index_count,
        dirty_slot_count: slots.values().filter(|slot| slot.dirty).count(),
        missing_object_id_count,
        missing_routing_slot_count,
        missing_page_id_count,
        missing_checksum_count,
        cpp_packed_page_index_size: CPP_PACKED_PAGE_INDEX_SIZE,
        cpp_packed_slot_node_size: CPP_PACKED_SLOT_NODE_SIZE,
        cpp_packed_layout_compatible: true,
        slot_nodes: slots.into_values().collect(),
    }
}

fn merge_last_dump_sequence(
    mut summaries: Vec<SlotStorageSummary>,
    manifest: &SlotDumpManifest,
) -> Vec<SlotStorageSummary> {
    let dumped_slots = manifest.slot_ids.iter().copied().collect::<BTreeSet<_>>();
    for summary in &mut summaries {
        if dumped_slots.contains(&summary.routing_slot) {
            summary.last_dump_sequence = manifest.index_log_sequence;
        }
    }
    summaries
}

fn slot_dump_manifest_comparable_summaries(
    shard: &ShardState,
    selected_slots: &BTreeSet<u32>,
) -> Vec<SlotStorageSummary> {
    comparable_slot_dump_summaries(
        slot_storage_summaries(shard, 0, u32::MAX)
            .into_iter()
            .filter(|summary| {
                selected_slots.is_empty() || selected_slots.contains(&summary.routing_slot)
            })
            .collect(),
    )
}

fn comparable_slot_dump_summaries(
    mut summaries: Vec<SlotStorageSummary>,
) -> Vec<SlotStorageSummary> {
    for summary in &mut summaries {
        summary.dirty_object_count = 0;
        summary.dirty_generation = 0;
        summary.last_dump_sequence = 0;
        summary.page_segment_ids.sort_unstable();
        summary.page_segment_ids.dedup();
    }
    summaries.retain(|summary| {
        summary.object_count > 0
            || summary.page_ref_count > 0
            || summary.logical_bytes > 0
            || summary.physical_bytes > 0
    });
    summaries.sort_by_key(|summary| summary.routing_slot);
    summaries
}

fn slot_dump_summary_matches_current_generation(
    manifest_summary: &SlotStorageSummary,
    current_summary: &SlotStorageSummary,
    manifest_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
    current_slot_fingerprints: &BTreeMap<u32, BTreeSet<String>>,
) -> bool {
    let mut manifest_segments = manifest_summary.page_segment_ids.clone();
    manifest_segments.sort_unstable();
    manifest_segments.dedup();
    let mut current_segments = current_summary.page_segment_ids.clone();
    current_segments.sort_unstable();
    current_segments.dedup();
    manifest_summary.routing_slot == current_summary.routing_slot
        && manifest_summary.dirty_generation == current_summary.dirty_generation
        && manifest_summary.object_count == current_summary.object_count
        && manifest_summary.page_ref_count == current_summary.page_ref_count
        && manifest_summary.logical_bytes == current_summary.logical_bytes
        && manifest_summary.physical_bytes == current_summary.physical_bytes
        && manifest_segments == current_segments
        && manifest_slot_fingerprints.get(&manifest_summary.routing_slot)
            == current_slot_fingerprints.get(&current_summary.routing_slot)
}

fn slot_generation_fingerprints_by_slot(shard: &ShardState) -> BTreeMap<u32, BTreeSet<String>> {
    let mut by_slot = BTreeMap::<u32, BTreeSet<String>>::new();
    for entry in collect_live_page_entries(shard) {
        let routing_slot = entry
            .address
            .routing_slot
            .unwrap_or_else(|| slot_for_object(&entry.object_key, 0, u32::MAX));
        by_slot.entry(routing_slot).or_default().insert(format!(
            "{}|{}|{}|{}|{}|{}|{}|{}|{}",
            entry.kind,
            entry.object_key,
            entry.component.unwrap_or_default(),
            entry.address.page_segment_id,
            entry.address.offset,
            entry.address.length,
            entry.address.page_id.unwrap_or_default(),
            entry.address.object_id.unwrap_or_default(),
            entry.address.sha256.unwrap_or_default()
        ));
    }
    by_slot
}

fn collect_live_page_addresses(shard: &ShardState) -> Vec<PageAddress> {
    let mut addresses = Vec::new();
    addresses.extend(shard.strings.values().cloned());
    for fields in shard.hashes.values() {
        addresses.extend(fields.values().cloned());
    }
    for members in shard.sets.values() {
        addresses.extend(members.values().cloned());
    }
    for series in shard.features.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.sequences.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.ips.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    addresses.extend(shard.risk_pages.values().cloned());
    addresses.extend(shard.context_nodes.values().cloned());
    for series in shard.context_events.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.context_indexes.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.context_audits.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.context_dirty.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    addresses.extend(shard.context_entities.values().cloned());
    for series in shard.context_children.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    addresses.extend(shard.context_embeddings.values().cloned());
    for series in shard.context_summaries.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    for series in shard.context_compressions.values() {
        addresses.extend(unique_timestamped_kv_page_addresses(series));
    }
    addresses
}

fn unique_timestamped_kv_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    let mut addresses = series
        .values()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    addresses.sort_by(|left, right| {
        left.page_segment_id
            .cmp(&right.page_segment_id)
            .then(left.offset.cmp(&right.offset))
            .then(left.length.cmp(&right.length))
    });
    addresses
}

fn unique_feature_page_addresses(series: &BTreeMap<u64, PageAddress>) -> Vec<PageAddress> {
    unique_timestamped_kv_page_addresses(series)
}

fn timestamped_kv_series<'a>(
    shard: &'a ShardState,
) -> Vec<(&'static str, &'a str, &'a BTreeMap<u64, PageAddress>)> {
    let mut series = Vec::new();
    for (key, timeline) in &shard.features {
        series.push(("feature", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.sequences {
        series.push(("sequence", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.ips {
        series.push(("ips", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_events {
        series.push(("context_event", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_indexes {
        series.push(("context_index", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_audits {
        series.push(("context_audit", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_dirty {
        series.push(("context_dirty", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_children {
        series.push(("context_child", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_summaries {
        series.push(("context_summary", key.as_str(), timeline));
    }
    for (key, timeline) in &shard.context_compressions {
        series.push(("context_compression", key.as_str(), timeline));
    }
    series
}

fn storage_feature_page_layout_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> StorageFeaturePageLayoutReport {
    let mut report = StorageFeaturePageLayoutReport::default();
    let mut family_reports = BTreeMap::<String, StorageTimestampedPageFamilyReport>::new();
    for (kind, key, series) in timestamped_kv_series(shard) {
        report.indexed_timestamped_points = report
            .indexed_timestamped_points
            .saturating_add(series.len());
        if kind == "feature" {
            report.indexed_feature_points =
                report.indexed_feature_points.saturating_add(series.len());
        }
        let family = family_reports.entry(kind.to_string()).or_insert_with(|| {
            StorageTimestampedPageFamilyReport {
                kind: kind.to_string(),
                ..StorageTimestampedPageFamilyReport::default()
            }
        });
        family.indexed_points = family.indexed_points.saturating_add(series.len());
        let mut timestamps_by_address = HashMap::<PageAddress, BTreeSet<u64>>::new();
        for (timestamp_ms, address) in series {
            timestamps_by_address
                .entry(address.clone())
                .or_default()
                .insert(*timestamp_ms);
        }
        report.unique_timestamped_page_refs = report
            .unique_timestamped_page_refs
            .saturating_add(timestamps_by_address.len());
        family.unique_page_refs = family
            .unique_page_refs
            .saturating_add(timestamps_by_address.len());
        if kind == "feature" {
            report.unique_feature_page_refs = report
                .unique_feature_page_refs
                .saturating_add(timestamps_by_address.len());
        }

        for (address, indexed_timestamps) in timestamps_by_address {
            match page_store.read(&address) {
                Ok(bytes) => match decode_feature_page_strict(&bytes) {
                    PackedFeaturePageDecode::Packed(points) => {
                        report.packed_timestamped_pages =
                            report.packed_timestamped_pages.saturating_add(1);
                        family.packed_pages = family.packed_pages.saturating_add(1);
                        if kind == "feature" {
                            report.packed_feature_pages =
                                report.packed_feature_pages.saturating_add(1);
                        }
                        let mut packed_timestamp_counts = BTreeMap::<u64, usize>::new();
                        for point in &points {
                            let count = packed_timestamp_counts
                                .entry(point.timestamp_ms)
                                .or_default();
                            if *count == 1 {
                                report.duplicate_packed_timestamps.push(
                                    feature_page_timestamp_mismatch(
                                        kind,
                                        key,
                                        point.timestamp_ms,
                                        &address,
                                    ),
                                );
                                family.mismatch_count = family.mismatch_count.saturating_add(1);
                            }
                            *count = (*count).saturating_add(1);
                        }
                        let packed_timestamps = points
                            .into_iter()
                            .map(|point| point.timestamp_ms)
                            .collect::<BTreeSet<_>>();
                        for timestamp_ms in
                            indexed_timestamps.difference(&packed_timestamps).copied()
                        {
                            report.missing_indexed_timestamps.push(
                                feature_page_timestamp_mismatch(kind, key, timestamp_ms, &address),
                            );
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                        for timestamp_ms in
                            packed_timestamps.difference(&indexed_timestamps).copied()
                        {
                            report
                                .orphan_packed_timestamps
                                .push(feature_page_timestamp_mismatch(
                                    kind,
                                    key,
                                    timestamp_ms,
                                    &address,
                                ));
                            family.mismatch_count = family.mismatch_count.saturating_add(1);
                        }
                    }
                    PackedFeaturePageDecode::Corrupt(error) => {
                        report
                            .corrupt_packed_feature_pages
                            .push(feature_page_error(kind, key, &address, error));
                        family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                    }
                    PackedFeaturePageDecode::Legacy => {
                        report.legacy_timestamped_value_pages =
                            report.legacy_timestamped_value_pages.saturating_add(1);
                        family.legacy_value_pages = family.legacy_value_pages.saturating_add(1);
                        if kind == "feature" {
                            report.legacy_feature_value_pages =
                                report.legacy_feature_value_pages.saturating_add(1);
                        }
                        if indexed_timestamps.len() > 1 {
                            report.corrupt_packed_feature_pages.push(feature_page_error(
                                kind,
                                key,
                                &address,
                                "legacy timestamped value page shared by multiple timestamps",
                            ));
                            family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                        }
                    }
                },
                Err(err) => {
                    report.corrupt_packed_feature_pages.push(feature_page_error(
                        kind,
                        key,
                        &address,
                        err.to_string(),
                    ));
                    family.corrupt_pages = family.corrupt_pages.saturating_add(1);
                }
            }
        }
    }
    report.families = family_reports.into_values().collect();
    report
}

fn feature_page_error(
    kind: &str,
    key: &str,
    address: &PageAddress,
    error: impl Into<String>,
) -> StorageFeaturePageError {
    StorageFeaturePageError {
        kind: kind.to_string(),
        key: key.to_string(),
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
        error: error.into(),
    }
}

fn feature_page_timestamp_mismatch(
    kind: &str,
    key: &str,
    timestamp_ms: u64,
    address: &PageAddress,
) -> StorageFeaturePageTimestampMismatch {
    StorageFeaturePageTimestampMismatch {
        kind: kind.to_string(),
        key: key.to_string(),
        timestamp_ms,
        page_segment_id: address.page_segment_id,
        offset: address.offset,
        length: address.length,
    }
}

fn compaction_utility_report(
    page_store: &LocalPageStore,
    shard: &ShardState,
) -> ShardCompactionUtilityReport {
    let entries = collect_live_page_entries(shard);
    let addresses = entries
        .iter()
        .filter(|entry| !entry.deleted)
        .map(|entry| entry.address.clone())
        .collect::<Vec<_>>();
    let live_page_segment_ids = addresses
        .iter()
        .map(|address| address.page_segment_id)
        .collect::<BTreeSet<_>>();
    let segment_page_counts = page_store
        .segment_reports()
        .unwrap_or_default()
        .into_iter()
        .map(|report| (report.page_segment_id, report.page_count))
        .collect::<BTreeMap<_, _>>();
    let total_page_count = live_page_segment_ids
        .iter()
        .map(|page_segment_id| {
            segment_page_counts
                .get(page_segment_id)
                .copied()
                .unwrap_or_default()
        })
        .sum::<u64>();
    let live_page_refs = addresses.len() as u64;
    let stale_page_estimate = total_page_count.saturating_sub(live_page_refs);
    let live_ref_density_basis_points = if total_page_count == 0 {
        0
    } else {
        live_page_refs.saturating_mul(10_000) / total_page_count
    };
    ShardCompactionUtilityReport {
        live_page_segment_count: live_page_segment_ids.len(),
        total_page_count,
        live_page_refs,
        stale_page_estimate,
        live_ref_density_basis_points,
        model_policies: model_compaction_policy_reports(&entries, &segment_page_counts),
    }
}

fn model_compaction_policy_reports(
    entries: &[LivePageEntry],
    segment_page_counts: &BTreeMap<u64, u64>,
) -> Vec<ModelCompactionPolicyReport> {
    #[derive(Default)]
    struct ModelStats {
        live_page_refs: u64,
        deleted_page_refs: u64,
        segment_ids: BTreeSet<u64>,
    }

    let mut by_model = BTreeMap::<String, ModelStats>::new();
    for entry in entries {
        let stats = by_model.entry(entry.kind.clone()).or_default();
        if entry.deleted {
            stats.deleted_page_refs = stats.deleted_page_refs.saturating_add(1);
        } else {
            stats.live_page_refs = stats.live_page_refs.saturating_add(1);
            stats.segment_ids.insert(entry.address.page_segment_id);
        }
    }

    by_model
        .into_iter()
        .map(|(model_id, stats)| {
            let total_segment_pages = stats
                .segment_ids
                .iter()
                .map(|segment_id| {
                    segment_page_counts
                        .get(segment_id)
                        .copied()
                        .unwrap_or_default()
                })
                .sum::<u64>();
            let stale_page_estimate = total_segment_pages.saturating_sub(stats.live_page_refs);
            let stale_density_basis_points = if total_segment_pages == 0 {
                0
            } else {
                stale_page_estimate.saturating_mul(10_000) / total_segment_pages
            };
            let total_refs = stats.live_page_refs.saturating_add(stats.deleted_page_refs);
            let tombstone_density_basis_points = if total_refs == 0 {
                0
            } else {
                stats.deleted_page_refs.saturating_mul(10_000) / total_refs
            };
            ModelCompactionPolicyReport {
                layout_policy: compaction_layout_policy_for_model(&model_id).to_string(),
                model_id,
                live_page_refs: stats.live_page_refs,
                deleted_page_refs: stats.deleted_page_refs,
                total_segment_pages,
                stale_page_estimate,
                stale_density_basis_points,
                tombstone_density_basis_points,
            }
        })
        .collect()
}

fn compaction_layout_policy_for_model(model_id: &str) -> &'static str {
    match model_id {
        "string" | "risk" | "context_node" | "context_entity" | "context_embedding" => {
            "single_page_object"
        }
        "hash" | "set" => "component_page_object",
        "feature" | "sequence" | "ips" => "timestamped_chunked_pages",
        model if model.starts_with("context_") => "context_timeline_or_sidecar_pages",
        _ => "generic_page_object",
    }
}

fn compact_page_addresses<'a>(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    addresses: impl IntoIterator<Item = &'a mut PageAddress>,
    rewritten_page_refs: &mut usize,
) -> Result<(), Status> {
    for address in addresses {
        let bytes = read_page_bytes(cache, page_store, shard_id, address).ok_or_else(|| {
            Status::error(
                "page_compaction_failed",
                "missing page bytes during compaction",
            )
        })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, address.object_id, address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page_with_slot(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
            ),
            bytes,
        );
        *rewritten_page_refs += 1;
    }
    Ok(())
}

fn compact_feature_page_addresses(
    page_store: &LocalPageStore,
    cache: &MultiLayerCache,
    shard_id: ShardId,
    series: &mut BTreeMap<u64, PageAddress>,
    rewritten_page_refs: &mut usize,
) -> Result<(), Status> {
    let unique_addresses = unique_feature_page_addresses(series);
    let mut rewritten = HashMap::<PageAddress, PageAddress>::new();
    for old_address in unique_addresses {
        let bytes =
            read_page_bytes(cache, page_store, shard_id, &old_address).ok_or_else(|| {
                Status::error(
                    "page_compaction_failed",
                    "missing feature page bytes during compaction",
                )
            })?;
        let new_address = page_store
            .append_with_page_metadata(&bytes, old_address.object_id, old_address.routing_slot)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        let _ = cache.put(
            CacheKey::page_with_slot(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
                new_address.routing_slot,
            ),
            bytes,
        );
        rewritten.insert(old_address, new_address);
        *rewritten_page_refs += 1;
    }
    for address in series.values_mut() {
        if let Some(new_address) = rewritten.get(address) {
            *address = new_address.clone();
        }
    }
    Ok(())
}

fn append_value(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    bytes: &[u8],
    object_id: Option<u64>,
    routing_slot: Option<u32>,
    async_storage: bool,
) -> Result<PageAddress, PageStoreError> {
    if !async_storage {
        return page_store.append_with_page_metadata(bytes, object_id, routing_slot);
    }
    let address = PageAddress {
        page_segment_id: HOT_PAGE_SEGMENT_ID,
        offset: HOT_PAGE_OFFSET.fetch_add(1, Ordering::Relaxed),
        length: bytes.len() as u64,
        page_id: None,
        object_id,
        routing_slot,
        zone_id: None,
        sha256: None,
    };
    let bytes = bytes.to_vec();
    cache.put_memory_only(
        CacheKey::page_with_slot(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
            address.routing_slot,
        ),
        bytes,
    );
    Ok(address)
}

fn persist_risk_page(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
    start_routing_slot: u32,
    end_routing_slot: u32,
    async_storage: bool,
) -> bool {
    let Some(series) = shard.risk.get(key) else {
        shard.risk_pages.remove(key);
        return false;
    };
    let Ok(bytes) = serde_json::to_vec(series) else {
        return false;
    };
    let object_id = stable_page_object_id(shard_id, "risk", key, None);
    let routing_slot = page_routing_slot(key, start_routing_slot, end_routing_slot);
    if let Ok(address) = append_value(
        cache,
        page_store,
        shard_id,
        &bytes,
        Some(object_id),
        Some(routing_slot),
        async_storage,
    ) {
        upsert_slot_index_page(shard, shard_id, "risk", key, None, address.clone(), true);
        shard.risk_pages.insert(key.to_string(), address);
        true
    } else {
        false
    }
}

fn invalidate_cache_key(cache: &MultiLayerCache, key: CacheKey, memory_only: bool) {
    if memory_only {
        cache.invalidate_memory_only(&key);
    } else {
        let _ = cache.invalidate(&key);
    }
}

fn record_exists(shard: &ShardState, key: &str) -> bool {
    associated_record_keys(key)
        .iter()
        .any(|record_key| record_exists_exact(shard, record_key))
}

fn record_exists_exact(shard: &ShardState, key: &str) -> bool {
    shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.features.contains_key(key)
        || shard.sequences.contains_key(key)
        || shard.ips.contains_key(key)
        || shard.risk.contains_key(key)
        || shard.risk_pages.contains_key(key)
        || shard.risk_changes.contains_key(key)
        || shard.risk_fol.contains_key(key)
        || shard.context_nodes.contains_key(key)
        || shard.context_events.contains_key(key)
        || shard.context_indexes.contains_key(key)
        || shard.context_audits.contains_key(key)
        || shard.context_dirty.contains_key(key)
        || shard.context_entities.contains_key(key)
        || shard.context_children.contains_key(key)
        || shard.context_embeddings.contains_key(key)
        || shard.context_summaries.contains_key(key)
        || shard.context_compressions.contains_key(key)
}

fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    let _ = cache.invalidate_record(shard_id, "hash", key);
    let _ = cache.invalidate_record(shard_id, "set", key);
    let _ = cache.invalidate_record(shard_id, "feature", key);
}

fn read_sequence_row(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &PageAddress,
) -> Option<SequenceFeatureRow> {
    let bytes = read_page_bytes(cache, page_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms)
            .and_then(|point| serde_json::from_slice(&point.value).ok()),
        PackedFeaturePageDecode::Legacy => serde_json::from_slice(&bytes).ok(),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn sequence_filter_matches(row: &SequenceFeatureRow, filter: &FeatureFilter) -> bool {
    let lhs = match filter.field.as_str() {
        "gid" => row.gid,
        "action_type" => row.action_type as u64,
        "duration" => row.duration as u64,
        "author_id" => row.author_id,
        _ => return false,
    };
    match filter.op {
        FeatureFilterOp::Equal => lhs == filter.value,
        FeatureFilterOp::NotEqual => lhs != filter.value,
        FeatureFilterOp::GreaterThan => lhs > filter.value,
        FeatureFilterOp::GreaterOrEqual => lhs >= filter.value,
        FeatureFilterOp::LessThan => lhs < filter.value,
        FeatureFilterOp::LessOrEqual => lhs <= filter.value,
    }
}

fn sequence_rows_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: usize,
    filters: &[FeatureFilter],
) -> Vec<SequenceFeatureRow> {
    shard
        .sequences
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count)
                .filter_map(|(timestamp_ms, address)| {
                    read_sequence_row(cache, page_store, shard_id, *timestamp_ms, address)
                })
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn aggregate_feature_values(values: &[Vec<u8>], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" => values.iter().filter_map(parse_i64).sum(),
        "avg" | "average" => {
            let numeric = values.iter().filter_map(parse_i64).collect::<Vec<_>>();
            if numeric.is_empty() {
                0
            } else {
                numeric.iter().sum::<i64>() / numeric.len() as i64
            }
        }
        "min" => values
            .iter()
            .filter_map(parse_i64)
            .min()
            .unwrap_or_default(),
        "max" => values
            .iter()
            .filter_map(parse_i64)
            .max()
            .unwrap_or_default(),
        "first" => values.first().and_then(parse_i64).unwrap_or_default(),
        "last" => values.last().and_then(parse_i64).unwrap_or_default(),
        "count" | "events" | "" => values.len() as i64,
        _ => values.len() as i64,
    }
}

fn aggregate_risk_values(values: &[i64], aggregator: &str) -> i64 {
    match aggregator.to_ascii_lowercase().as_str() {
        "sum" | "count" | "" => values.iter().sum(),
        "events" | "len" => values.len() as i64,
        "min" => values.iter().copied().min().unwrap_or_default(),
        "max" => values.iter().copied().max().unwrap_or_default(),
        "first" => values.first().copied().unwrap_or_default(),
        "last" => values.last().copied().unwrap_or_default(),
        _ => values.iter().sum(),
    }
}

fn is_risk_change_aggregator(aggregator: &str) -> bool {
    aggregator.eq_ignore_ascii_case("change")
}

fn count_risk_changes(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> i64 {
    let mut unique = BTreeSet::new();
    if let Some(series) = shard.risk_changes.get(key) {
        for (_, values) in series.range(start_ms..=end_ms) {
            unique.extend(values.iter().cloned());
        }
    }
    unique.len() as i64
}

fn risk_family_key(family: RiskFamily, key: &str) -> String {
    format!("risk:{}:{key}", risk_family_name(family))
}

fn risk_family_name(family: RiskFamily) -> &'static str {
    match family {
        RiskFamily::H => "h",
        RiskFamily::Cpc => "cpc",
        RiskFamily::Fol => "fol",
    }
}

fn ips_points_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
) -> Vec<FeaturePoint> {
    shard
        .ips
        .get(key)
        .map(|series| {
            series
                .range(start_ms..=end_ms)
                .take(count.unwrap_or(usize::MAX))
                .filter_map(|(timestamp_ms, address)| {
                    read_feature_point(cache, page_store, shard_id, *timestamp_ms, address)
                })
                .collect()
        })
        .unwrap_or_default()
}

fn ips_points_in_range_with_options(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: &str,
    start_ms: u64,
    end_ms: u64,
    count: Option<usize>,
    action_type: Option<u32>,
    table_id: Option<u64>,
) -> Vec<FeaturePoint> {
    let Some(series) = shard.ips_meta.get(key) else {
        return ips_points_in_range(
            cache, page_store, shard_id, shard, key, start_ms, end_ms, count,
        );
    };
    series
        .range(start_ms..=end_ms)
        .filter(|(_, meta)| {
            action_type
                .map(|expected| meta.action_type == Some(expected))
                .unwrap_or(true)
                && table_id
                    .map(|expected| meta.table_id == Some(expected))
                    .unwrap_or(true)
        })
        .take(count.unwrap_or(usize::MAX))
        .filter_map(|(timestamp_ms, meta)| {
            read_feature_point(cache, page_store, shard_id, *timestamp_ms, &meta.address)
        })
        .collect()
}

fn empty_ips_snapshot_report(
    key: String,
    start_ms: u64,
    end_ms: u64,
    requested_count: Option<usize>,
) -> IpsSnapshotReport {
    IpsSnapshotReport {
        key,
        start_ms,
        end_ms,
        requested_count,
        returned_count: 0,
        total_in_range: 0,
        first_timestamp_ms: None,
        last_timestamp_ms: None,
        action_type_counts: Vec::new(),
        table_id_counts: Vec::new(),
        unique_page_ref_count: 0,
        packed_timestamped_page_count: 0,
        page_segment_ids: Vec::new(),
    }
}

fn ips_snapshot_report_in_range(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    key: String,
    start_ms: u64,
    end_ms: u64,
    requested_count: Option<usize>,
) -> IpsSnapshotReport {
    let points = ips_points_in_range(
        cache,
        page_store,
        shard_id,
        shard,
        &key,
        start_ms,
        end_ms,
        requested_count,
    );
    let stats = ips_stats_in_range(shard, &key, start_ms, end_ms);
    let mut page_refs = HashSet::<PageAddress>::new();
    let mut page_segment_ids = BTreeSet::<u64>::new();
    let mut packed_timestamped_page_count = 0usize;
    if let Some(series) = shard.ips.get(&key) {
        for (_, address) in series.range(start_ms..=end_ms) {
            if page_refs.insert(address.clone()) {
                page_segment_ids.insert(address.page_segment_id);
                if read_page_bytes(cache, page_store, shard_id, address)
                    .map(|bytes| {
                        matches!(
                            decode_feature_page_strict(&bytes),
                            PackedFeaturePageDecode::Packed(_)
                        )
                    })
                    .unwrap_or(false)
                {
                    packed_timestamped_page_count += 1;
                }
            }
        }
    }
    IpsSnapshotReport {
        key,
        start_ms,
        end_ms,
        requested_count,
        returned_count: points.len(),
        total_in_range: stats.total,
        first_timestamp_ms: stats.first_timestamp_ms,
        last_timestamp_ms: stats.last_timestamp_ms,
        action_type_counts: stats.action_type_counts,
        table_id_counts: stats.table_id_counts,
        unique_page_ref_count: page_refs.len(),
        packed_timestamped_page_count,
        page_segment_ids: page_segment_ids.into_iter().collect(),
    }
}

fn ips_stats_in_range(shard: &ShardState, key: &str, start_ms: u64, end_ms: u64) -> IpsStats {
    let mut total = 0u64;
    let mut first_timestamp_ms = None;
    let mut last_timestamp_ms = None;
    let mut action_type_counts = BTreeMap::<u32, u64>::new();
    let mut table_id_counts = BTreeMap::<u64, u64>::new();

    if let Some(series) = shard.ips.get(key) {
        for (timestamp_ms, _) in series.range(start_ms..=end_ms) {
            total += 1;
            first_timestamp_ms.get_or_insert(*timestamp_ms);
            last_timestamp_ms = Some(*timestamp_ms);
        }
    }
    if let Some(series) = shard.ips_meta.get(key) {
        for (_, meta) in series.range(start_ms..=end_ms) {
            if let Some(action_type) = meta.action_type {
                *action_type_counts.entry(action_type).or_default() += 1;
            }
            if let Some(table_id) = meta.table_id {
                *table_id_counts.entry(table_id).or_default() += 1;
            }
        }
    }

    IpsStats {
        total,
        first_timestamp_ms,
        last_timestamp_ms,
        action_type_counts: action_type_counts.into_iter().collect(),
        table_id_counts: table_id_counts.into_iter().collect(),
    }
}

fn read_page_bytes(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    address: &PageAddress,
) -> Option<Vec<u8>> {
    let cache_key = CacheKey::page_with_slot(
        shard_id,
        address.page_segment_id,
        address.offset,
        address.length,
        address.routing_slot,
    );
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
}

fn sorted_feature_points(mut points: Vec<FeaturePoint>) -> Vec<FeaturePoint> {
    let mut by_timestamp = BTreeMap::new();
    for point in points.drain(..) {
        by_timestamp.insert(point.timestamp_ms, point);
    }
    by_timestamp.into_values().collect()
}

fn encode_feature_page(points: &[FeaturePoint]) -> Vec<u8> {
    let page = PackedFeaturePage {
        version: 1,
        points: points.to_vec(),
    };
    let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
    if let Ok(mut payload) = serde_json::to_vec(&page) {
        bytes.append(&mut payload);
    }
    bytes
}

fn append_timestamped_kv_pages(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    kind: &str,
    key: &str,
    points: Vec<FeaturePoint>,
    routing_slot: u32,
    async_storage: bool,
) -> Result<Vec<(u64, PageAddress)>, PageStoreError> {
    let object_id = stable_page_object_id(shard_id, kind, key, None);
    let mut refs = Vec::new();
    for chunk in chunk_timestamped_kv_points(points) {
        let packed = encode_feature_page(&chunk);
        let address = append_value(
            cache,
            page_store,
            shard_id,
            &packed,
            Some(object_id),
            Some(routing_slot),
            async_storage,
        )?;
        refs.extend(
            chunk
                .into_iter()
                .map(|point| (point.timestamp_ms, address.clone())),
        );
    }
    Ok(refs)
}

fn chunk_timestamped_kv_points(points: Vec<FeaturePoint>) -> Vec<Vec<FeaturePoint>> {
    let mut chunks = Vec::new();
    let mut current = Vec::new();

    for point in points {
        current.push(point);
        let encoded_len = encode_feature_page(&current).len();
        if encoded_len > TIMESTAMPED_KV_PAGE_TARGET_BYTES && current.len() > 1 {
            let overflow = current.pop().expect("current chunk is non-empty");
            chunks.push(current);
            current = vec![overflow];
        }
    }

    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

#[cfg(test)]
fn decode_feature_page(bytes: &[u8]) -> Option<Vec<FeaturePoint>> {
    match decode_feature_page_strict(bytes) {
        PackedFeaturePageDecode::Packed(points) => Some(points),
        PackedFeaturePageDecode::Legacy | PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn decode_feature_page_strict(bytes: &[u8]) -> PackedFeaturePageDecode {
    let Some(payload) = bytes.strip_prefix(FEATURE_PAGE_MAGIC) else {
        return PackedFeaturePageDecode::Legacy;
    };
    let page = match serde_json::from_slice::<PackedFeaturePage>(payload) {
        Ok(page) => page,
        Err(err) => {
            return PackedFeaturePageDecode::Corrupt(format!(
                "invalid packed feature page payload: {err}"
            ));
        }
    };
    if page.version != 1 {
        return PackedFeaturePageDecode::Corrupt(format!(
            "unsupported packed feature page version {}",
            page.version
        ));
    }
    PackedFeaturePageDecode::Packed(page.points)
}

fn read_feature_point(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timestamp_ms: u64,
    address: &PageAddress,
) -> Option<FeaturePoint> {
    let bytes = read_page_bytes(cache, page_store, shard_id, address)?;
    match decode_feature_page_strict(&bytes) {
        PackedFeaturePageDecode::Packed(points) => points
            .into_iter()
            .find(|point| point.timestamp_ms == timestamp_ms),
        PackedFeaturePageDecode::Legacy => Some(FeaturePoint {
            timestamp_ms,
            value: bytes,
        }),
        PackedFeaturePageDecode::Corrupt(_) => None,
    }
}

fn cache_entry_routing_slot(entry: &CacheEntryInfo) -> Option<u32> {
    entry
        .selector
        .strip_prefix("slot-")?
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn object_manager_stats(
    shard: &ShardState,
    start_routing_slot: u32,
    end_routing_slot: u32,
) -> ObjectManagerStats {
    let object_count = shard.strings.len()
        + shard.hashes.len()
        + shard.sets.len()
        + shard.features.len()
        + shard.sequences.len()
        + shard.ips.len()
        + shard.risk.len()
        + shard.context_nodes.len()
        + shard.context_events.len()
        + shard.context_indexes.len()
        + shard.context_audits.len()
        + shard.context_dirty.len()
        + shard.context_entities.len()
        + shard.context_children.len()
        + shard.context_embeddings.len()
        + shard.context_summaries.len()
        + shard.context_compressions.len();
    let page_ref_count = shard.strings.len()
        + shard.hashes.values().map(HashMap::len).sum::<usize>()
        + shard.sets.values().map(BTreeMap::len).sum::<usize>()
        + shard.features.values().map(BTreeMap::len).sum::<usize>()
        + shard.sequences.values().map(BTreeMap::len).sum::<usize>()
        + shard.ips.values().map(BTreeMap::len).sum::<usize>()
        + shard.context_nodes.len()
        + shard
            .context_events
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_indexes
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_audits
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_dirty
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard.context_entities.len()
        + shard
            .context_children
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard.context_embeddings.len()
        + shard
            .context_summaries
            .values()
            .map(BTreeMap::len)
            .sum::<usize>()
        + shard
            .context_compressions
            .values()
            .map(BTreeMap::len)
            .sum::<usize>();
    let routing_slot_count = routing_slot_count(start_routing_slot, end_routing_slot);
    let dirty_slots = shard
        .dirty_objects
        .iter()
        .map(|key| slot_for_object(key, start_routing_slot, routing_slot_count))
        .collect::<BTreeSet<_>>();
    ObjectManagerStats {
        object_count,
        page_ref_count,
        dirty_object_count: shard.dirty_objects.len(),
        dirty_slot_count: dirty_slots.len(),
        routing_slot_count,
    }
}

fn routing_slot_count(start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    if end_routing_slot < start_routing_slot {
        return 0;
    }
    end_routing_slot
        .saturating_sub(start_routing_slot)
        .saturating_add(1)
}

fn slot_for_object(key: &str, start_routing_slot: u32, routing_slot_count: u32) -> u32 {
    if routing_slot_count == 0 {
        return start_routing_slot;
    }
    start_routing_slot + (stable_object_hash(key) % routing_slot_count as u64) as u32
}

fn stable_object_hash(key: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn stable_page_object_id(shard_id: ShardId, kind: &str, key: &str, component: Option<&str>) -> u64 {
    let mut identity = format!("{shard_id}:{kind}:{key}");
    if let Some(component) = component {
        identity.push(':');
        identity.push_str(component);
    }
    stable_object_hash(&identity)
}

fn context_node_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:node:{tenant_hash}:{node_hash}")
}

fn context_event_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:event:{tenant_hash}:{node_hash}")
}

fn context_index_key(
    tenant_hash: u64,
    index_name: &str,
    index_value_hash: u64,
    scope_hash: u64,
) -> String {
    format!("ctxidx:{tenant_hash}:{index_name}:{index_value_hash}:{scope_hash}")
}

fn context_index_disabled(
    indexes: &ContextExtractedEventIndexes,
    index: InternalContextIndex,
) -> bool {
    indexes.disabled_indexes.contains(&index)
}

fn context_event_kind_hash(event: &ContextEvent) -> u64 {
    u64::from(if event.event_type != 0 {
        event.event_type
    } else {
        event.kind
    })
}

fn context_audit_key(tenant_hash: u64, session_hash: u64) -> String {
    format!("ctx:audit:{tenant_hash}:{session_hash}")
}

fn context_dirty_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:dirty:{tenant_hash}:{node_hash}")
}

fn context_entity_key(tenant_hash: u64, node_hash: u64, entity_hash: u64) -> String {
    format!("ctx:entity:{tenant_hash}:{node_hash}:{entity_hash}")
}

fn context_entity_collection_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:entity:{tenant_hash}:{node_hash}")
}

fn context_child_key(tenant_hash: u64, parent_hash: u64) -> String {
    format!("ctx:child:{tenant_hash}:{parent_hash}")
}

fn context_embedding_key(tenant_hash: u64, ref_hash: u64) -> String {
    format!("ctx:embedding:{tenant_hash}:{ref_hash}")
}

fn context_summary_key(tenant_hash: u64, node_hash: u64, level: u32) -> String {
    format!("ctx:summary:{tenant_hash}:{node_hash}:{level}")
}

fn context_compression_key(tenant_hash: u64, node_hash: u64) -> String {
    format!("ctx:compress:{tenant_hash}:{node_hash}")
}

fn context_timeline_key(timestamp_ms: u64, disambiguator: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(disambiguator % CONTEXT_TIMELINE_FANOUT)
}

fn context_timeline_start(timestamp_ms: u64) -> u64 {
    timestamp_ms.saturating_mul(CONTEXT_TIMELINE_FANOUT)
}

fn context_timeline_end(timestamp_ms: u64) -> u64 {
    timestamp_ms
        .saturating_mul(CONTEXT_TIMELINE_FANOUT)
        .saturating_add(CONTEXT_TIMELINE_FANOUT)
}

fn context_limit(limit: Option<usize>) -> usize {
    limit
        .unwrap_or(CONTEXT_DEFAULT_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT)
}

fn context_bytes<T: ContextWire>(value: &T) -> Vec<u8> {
    value.encode_context_value()
}

fn context_from_bytes<T: ContextWire>(bytes: &[u8]) -> Option<T> {
    T::decode_context_value(bytes)
}

fn read_context_value<T: ContextWire>(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    timeline_key: u64,
    address: &PageAddress,
) -> Option<T> {
    let point = read_feature_point(cache, page_store, shard_id, timeline_key, address)?;
    context_from_bytes(&point.value)
}

fn context_event_matches_filter(
    event: &ContextEvent,
    current_valid_only: bool,
    as_of_ms: u64,
    end_time_ms: u64,
    kinds: &[u32],
    statuses: &[u32],
    min_confidence: f32,
    min_importance: f32,
) -> bool {
    if !kinds.is_empty() && !kinds.contains(&event.kind) {
        return false;
    }
    if !statuses.is_empty() && !statuses.contains(&event.status) {
        return false;
    }
    if event.confidence < min_confidence || event.importance < min_importance {
        return false;
    }
    if current_valid_only {
        let as_of = if as_of_ms == 0 { end_time_ms } else { as_of_ms };
        if event.event_time_ms > as_of {
            return false;
        }
        if event.valid_until_ms != 0 && event.valid_until_ms <= as_of {
            return false;
        }
    }
    true
}

fn page_routing_slot(key: &str, start_routing_slot: u32, end_routing_slot: u32) -> u32 {
    slot_for_object(
        key,
        start_routing_slot,
        routing_slot_count(start_routing_slot, end_routing_slot),
    )
}

fn command_object_keys(command: &Command) -> Vec<String> {
    match command {
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
        | Command::StringSetConditional { key, .. }
        | Command::StringDelete { key }
        | Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashIncrBy { key, .. }
        | Command::HashDelete { key, .. }
        | Command::SetAdd { key, .. }
        | Command::SetRemove { key, .. }
        | Command::FeatureAppend { key, .. }
        | Command::FeatureAppendWithPolicy { key, .. }
        | Command::FeatureReplace { key, .. }
        | Command::FeatureDelete { key }
        | Command::SequenceAdd { key, .. }
        | Command::IpsAdd { key, .. }
        | Command::IpsAddWithOptions { key, .. }
        | Command::IpsLoad { key, .. }
        | Command::IpsRemove { key, .. }
        | Command::IpsDelete { key }
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskChangeAdd { key, .. }
        | Command::RiskFolSet { key, .. } => vec![key.clone()],
        Command::RiskSet { family, key, .. } | Command::RiskSetAndGet { family, key, .. } => {
            vec![risk_family_key(*family, key)]
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            vec![context_node_key(*tenant_hash, node.node_hash)]
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_event_key(*tenant_hash, *node_hash)],
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            let mut keys = vec![context_event_key(*tenant_hash, *node_hash)];
            if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_kind",
                    context_event_kind_hash(event),
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Status)
                && indexes.status_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "status",
                    indexes.status_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Source)
                && indexes.source_hash != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "source",
                    indexes.source_hash,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket)
                && indexes.event_time_bucket_ms != 0
            {
                keys.push(context_index_key(
                    *tenant_hash,
                    "event_time_bucket",
                    indexes.event_time_bucket_ms,
                    indexes.scope_hash,
                ));
            }
            if !context_index_disabled(indexes, InternalContextIndex::Entity) {
                keys.extend(
                    indexes
                        .entity_hashes
                        .iter()
                        .copied()
                        .filter(|hash| *hash != 0)
                        .map(|entity_hash| {
                            context_index_key(
                                *tenant_hash,
                                "entity",
                                entity_hash,
                                indexes.scope_hash,
                            )
                        }),
                );
            }
            keys
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            scope_hash,
            ..
        } => vec![context_index_key(
            *tenant_hash,
            index_name,
            *index_value_hash,
            *scope_hash,
        )],
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            vec![context_audit_key(*tenant_hash, audit.session_hash)]
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => vec![context_dirty_key(*tenant_hash, marker.node_hash)],
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => vec![context_entity_key(
            *tenant_hash,
            entity.node_hash,
            entity.entity_hash,
        )],
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => vec![context_child_key(*tenant_hash, child_ref.parent_hash)],
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => vec![context_embedding_key(*tenant_hash, embedding.ref_hash)],
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => vec![context_summary_key(
            *tenant_hash,
            summary.node_hash,
            summary.level,
        )],
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            vec![context_compression_key(*tenant_hash, event.node_hash)]
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            ..
        } => vec![context_compression_key(*tenant_hash, *node_hash)],
        Command::SequenceBatchQuery { .. }
        | Command::CommonTtl { .. }
        | Command::CommonExists { .. }
        | Command::StringGet { .. }
        | Command::HashGet { .. }
        | Command::HashMultiGet { .. }
        | Command::HashGetAll { .. }
        | Command::HashLen { .. }
        | Command::SetMembers { .. }
        | Command::FeatureQuery { .. }
        | Command::FeatureQueryFiltered { .. }
        | Command::FeatureAggQuery { .. }
        | Command::SequenceQuery { .. }
        | Command::IpsQueryLast { .. }
        | Command::IpsQueryRange { .. }
        | Command::IpsBatchQueryLast { .. }
        | Command::IpsCount { .. }
        | Command::IpsQueryRangeWithOptions { .. }
        | Command::IpsSnapshot { .. }
        | Command::IpsSnapshotReport { .. }
        | Command::IpsStat { .. }
        | Command::IpsFilter { .. }
        | Command::RiskCount { .. }
        | Command::RiskQuery { .. }
        | Command::RiskDetail { .. }
        | Command::RiskFamilyQuery { .. }
        | Command::RiskFolQuery { .. }
        | Command::RiskManager { .. }
        | Command::RiskDebug { .. }
        | Command::ContextGetNode { .. }
        | Command::ContextQueryEvents { .. }
        | Command::ContextQueryIndex { .. }
        | Command::ContextQueryPackAudit { .. }
        | Command::ContextQuerySummaryDirty { .. }
        | Command::ContextGetEntity { .. }
        | Command::ContextQueryEntities { .. }
        | Command::ContextQueryChildren { .. }
        | Command::ContextQueryEmbeddings { .. }
        | Command::ContextTraverseTree { .. }
        | Command::ContextQuerySummaries { .. }
        | Command::ContextQueryCompressionEvents { .. }
        | Command::ContextQueryNodeContext { .. } => Vec::new(),
    }
}

fn command_updates_slot_index_directly(command: &Command) -> bool {
    matches!(
        command,
        Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::SetAdd { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
    )
}

fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringSetConditional { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::IpsLoad { .. }
            | Command::IpsRemove { .. }
            | Command::IpsDelete { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskChangeAdd { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFolSet { .. }
            | Command::ContextUpsertNode { .. }
            | Command::ContextWriteEvent { .. }
            | Command::ContextWriteExtractedEvent { .. }
            | Command::ContextWriteIndexRef { .. }
            | Command::ContextWritePackAudit { .. }
            | Command::ContextMarkSummaryDirty { .. }
            | Command::ContextUpsertEntity { .. }
            | Command::ContextUpsertChildRef { .. }
            | Command::ContextUpsertEmbedding { .. }
            | Command::ContextUpsertSummary { .. }
            | Command::ContextWriteCompressionEvent { .. }
            | Command::ContextCompressEvents { .. }
    )
}

fn admission_limits(
    shard_id: ShardId,
    write_command: bool,
    config: &Config,
    info: &Option<ShardInfo>,
) -> Vec<AdmissionLimit> {
    let mut limits = Vec::new();
    if let Some(limit) = if write_command {
        config.write_qps
    } else {
        config.read_qps
    } {
        limits.push(AdmissionLimit {
            scope: AdmissionScope::Shard(shard_id),
            limit,
            label: if write_command {
                "write_qps"
            } else {
                "read_qps"
            },
        });
    }
    if let Some(table_name) = info
        .as_ref()
        .map(|info| info.table_name.trim())
        .filter(|table_name| !table_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.table_write_qps
        } else {
            config.table_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Table(table_name.to_string()),
                limit,
                label: if write_command {
                    "table_write_qps"
                } else {
                    "table_read_qps"
                },
            });
        }
    }
    if let Some(tenant_name) = config
        .tenant_name
        .as_deref()
        .map(str::trim)
        .filter(|tenant_name| !tenant_name.is_empty())
    {
        if let Some(limit) = if write_command {
            config.tenant_write_qps
        } else {
            config.tenant_read_qps
        } {
            limits.push(AdmissionLimit {
                scope: AdmissionScope::Tenant(tenant_name.to_string()),
                limit,
                label: if write_command {
                    "tenant_write_qps"
                } else {
                    "tenant_read_qps"
                },
            });
        }
    }
    limits
}

fn reset_admission_window(admission: &mut AdmissionState, now_sec: u64) {
    if admission.window_epoch_sec != now_sec {
        admission.window_epoch_sec = now_sec;
        admission.read_count = 0;
        admission.write_count = 0;
    }
}

fn admission_count(admission: &mut AdmissionState, write_command: bool) -> &mut u64 {
    if write_command {
        &mut admission.write_count
    } else {
        &mut admission.read_count
    }
}

fn now_epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn validate_command_preconditions(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    command: &Command,
) -> Result<(), Status> {
    match command {
        Command::CommonExpire { key, .. } => {
            if shard
                .expires_at_ms
                .get(key)
                .map(|expires_at| *expires_at <= now_ms())
                .unwrap_or(false)
                || !record_exists(shard, key)
            {
                return Err(Status::error("not_found", "key not found"));
            }
        }
        Command::FeatureAppend { key, points }
        | Command::FeatureAppendWithPolicy { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::FeatureReplace { key, points, .. } => {
            let current = shard
                .features
                .get(key)
                .map(|series| series.len())
                .unwrap_or(0);
            if current.saturating_add(points.len()) > FEATURE_ADD_HARD_MAX_SIZE {
                return Err(Status::error(
                    "invalid_argument",
                    format!("{key} size bigger than {FEATURE_ADD_HARD_MAX_SIZE}"),
                ));
            }
        }
        Command::ContextUpsertNode { tenant_hash, node } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_node(node)?;
        }
        Command::ContextGetNode {
            tenant_hash,
            node_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
        }
        Command::ContextWriteEvent {
            tenant_hash,
            node_hash,
            event,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
        }
        Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_event(event)?;
            validate_context_extracted_indexes(event, indexes)?;
        }
        Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            kinds,
            statuses,
            min_confidence,
            min_importance,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            validate_context_filters(kinds, statuses, *min_confidence, *min_importance)?;
        }
        Command::ContextWriteIndexRef {
            tenant_hash,
            index_name,
            index_value_hash,
            event_time_ms,
            index_ref,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_required(*event_time_ms != 0, "event_time_ms is required")?;
            validate_context_timestamp(*event_time_ms)?;
            validate_context_index_ref(index_ref)?;
        }
        Command::ContextQueryIndex {
            tenant_hash,
            index_name,
            index_value_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_index_name(index_name)?;
            validate_context_required(*index_value_hash != 0, "index_value_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextWritePackAudit { tenant_hash, audit } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_pack_audit(audit)?;
        }
        Command::ContextQueryPackAudit {
            tenant_hash,
            session_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*session_hash != 0, "session_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextMarkSummaryDirty {
            tenant_hash,
            marker,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_dirty_marker(marker)?;
        }
        Command::ContextQuerySummaryDirty {
            tenant_hash,
            node_hash,
            start_time_ms,
            end_time_ms,
            limit,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
        }
        Command::ContextUpsertEntity {
            tenant_hash,
            entity,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_entity(entity)?;
        }
        Command::ContextGetEntity {
            tenant_hash,
            node_hash,
            entity_hash,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*entity_hash != 0, "entity_hash is required")?;
        }
        Command::ContextQueryEntities {
            tenant_hash,
            node_hash,
            entity_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_limit(*limit)?;
            if entity_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes exceeds maximum",
                ));
            }
            if entity_hashes.iter().any(|hash| *hash == 0) {
                return Err(Status::error(
                    "invalid_argument",
                    "entity_hashes must be non-zero",
                ));
            }
        }
        Command::ContextUpsertChildRef {
            tenant_hash,
            child_ref,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_child_ref(child_ref)?;
        }
        Command::ContextQueryChildren {
            tenant_hash,
            parent_hash,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*parent_hash != 0, "parent_hash is required")?;
            validate_context_limit(*limit)?;
        }
        Command::ContextUpsertEmbedding {
            tenant_hash,
            embedding,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_embedding(embedding)?;
        }
        Command::ContextQueryEmbeddings {
            tenant_hash,
            ref_hashes,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            if ref_hashes.len() > CONTEXT_MAX_LIMIT {
                return Err(Status::error("invalid_argument", "too many ref_hashes"));
            }
        }
        Command::ContextTraverseTree {
            tenant_hash,
            start_node_hash,
            query_vector,
            max_depth,
            top_k_per_depth,
            max_children_scored_per_parent,
            max_candidate_nodes,
            ..
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*start_node_hash != 0, "start_node_hash is required")?;
            validate_context_required(!query_vector.is_empty(), "query_vector is required")?;
            validate_context_embedding_vector("query_vector", query_vector)?;
            if max_depth.unwrap_or_default() > CONTEXT_MAX_TRAVERSAL_DEPTH {
                return Err(Status::error(
                    "invalid_argument",
                    "max_depth exceeds maximum",
                ));
            }
            for limit in [
                *top_k_per_depth,
                *max_children_scored_per_parent,
                *max_candidate_nodes,
            ] {
                validate_context_limit(limit)?;
            }
        }
        Command::ContextUpsertSummary {
            tenant_hash,
            summary,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_summary(summary)?;
        }
        Command::ContextQuerySummaries {
            tenant_hash,
            node_hash,
            level,
            as_of_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*level != 0, "level is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            validate_context_limit(*limit)?;
        }
        Command::ContextWriteCompressionEvent { tenant_hash, event } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_compression_event(event)?;
        }
        Command::ContextQueryCompressionEvents {
            tenant_hash,
            node_hashes,
            start_time_ms,
            end_time_ms,
            limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_limit(*limit)?;
            validate_context_range(*start_time_ms, *end_time_ms)?;
            if node_hashes.len() > CONTEXT_MAX_FILTER_VALUES {
                return Err(Status::error("invalid_argument", "too many node_hashes"));
            }
        }
        Command::ContextCompressEvents {
            tenant_hash,
            node_hash,
            source_start_ms,
            source_end_ms,
            compressed_time_ms,
            max_source_events,
            min_confidence,
            min_importance,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_range(*source_start_ms, *source_end_ms)?;
            validate_context_limit(*max_source_events)?;
            validate_context_score("min_confidence", *min_confidence)?;
            validate_context_score("min_importance", *min_importance)?;
            if *compressed_time_ms != 0 {
                validate_context_timestamp(*compressed_time_ms)?;
            }
        }
        Command::ContextQueryNodeContext {
            tenant_hash,
            node_hash,
            summary_level,
            as_of_ms,
            cold_start_time_ms,
            cold_end_time_ms,
            compression_limit,
        } => {
            validate_context_required(*tenant_hash != 0, "tenant_hash is required")?;
            validate_context_required(*node_hash != 0, "node_hash is required")?;
            validate_context_required(*as_of_ms != 0, "as_of_ms is required")?;
            validate_context_timestamp(*as_of_ms)?;
            if summary_level.unwrap_or(1) == 0 {
                return Err(Status::error(
                    "invalid_argument",
                    "summary_level is required",
                ));
            }
            validate_context_limit(*compression_limit)?;
            if *cold_start_time_ms != 0 || *cold_end_time_ms != 0 {
                validate_context_range(*cold_start_time_ms, *cold_end_time_ms)?;
            }
        }
        _ => {}
    }

    if let Command::HashIncrBy {
        key,
        field,
        increment,
    } = command
    {
        if shard
            .expires_at_ms
            .get(key)
            .map(|expires_at| *expires_at <= now_ms())
            .unwrap_or(false)
        {
            return Ok(());
        }
        let Some(bytes) = shard
            .hashes
            .get(key)
            .and_then(|entries| entries.get(field))
            .and_then(|address| read_page_bytes(cache, page_store, shard_id, address))
        else {
            return 0_i64
                .checked_add(*increment)
                .map(|_| ())
                .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"));
        };
        let current = parse_i64(&bytes)
            .ok_or_else(|| Status::error("unmatched", "hash value is not an integer"))?;
        current
            .checked_add(*increment)
            .map(|_| ())
            .ok_or_else(|| Status::error("out_of_range", "hash increment overflows i64"))?;
    }
    Ok(())
}

fn validate_context_required(ok: bool, message: &'static str) -> Result<(), Status> {
    if ok {
        Ok(())
    } else {
        Err(Status::error("invalid_argument", message))
    }
}

fn validate_context_byte_len(
    name: &'static str,
    value_len: usize,
    max_len: usize,
) -> Result<(), Status> {
    if value_len <= max_len {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} is too large"),
        ))
    }
}

fn validate_context_score(name: &'static str, value: f32) -> Result<(), Status> {
    if value.is_finite() && (0.0..=1.0).contains(&value) {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} must be in [0, 1]"),
        ))
    }
}

fn validate_context_limit(limit: Option<usize>) -> Result<(), Status> {
    if limit.unwrap_or_default() <= CONTEXT_MAX_LIMIT {
        Ok(())
    } else {
        Err(Status::error("invalid_argument", "limit exceeds maximum"))
    }
}

fn validate_context_range(start_time_ms: u64, end_time_ms: u64) -> Result<(), Status> {
    if end_time_ms > start_time_ms {
        validate_context_timestamp(start_time_ms)?;
        validate_context_timestamp(end_time_ms)
    } else {
        Err(Status::error(
            "invalid_argument",
            "end_time_ms must be greater than start_time_ms",
        ))
    }
}

fn validate_context_timestamp(timestamp_ms: u64) -> Result<(), Status> {
    if timestamp_ms <= u64::MAX / CONTEXT_TIMELINE_FANOUT {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            "timestamp_ms is too large",
        ))
    }
}

fn validate_context_index_name(index_name: &str) -> Result<(), Status> {
    validate_context_required(!index_name.is_empty(), "index_name is required")?;
    validate_context_byte_len("index_name", index_name.len(), CONTEXT_MAX_INDEX_NAME_BYTES)?;
    if index_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            "index_name contains invalid characters",
        ))
    }
}

fn validate_context_node(node: &ContextNode) -> Result<(), Status> {
    validate_context_required(node.node_hash != 0, "node_hash must be non-zero")?;
    validate_context_required(
        !node.canonical_name.is_empty(),
        "canonical_name is required",
    )?;
    validate_context_byte_len(
        "canonical_name",
        node.canonical_name.len(),
        CONTEXT_MAX_CANONICAL_NAME_BYTES,
    )?;
    validate_context_byte_len("l0", node.l0.len(), CONTEXT_MAX_L0_BYTES)?;
    validate_context_byte_len("l1_ref", node.l1_ref.len(), CONTEXT_MAX_REF_BYTES)?;
    validate_context_byte_len(
        "raw_metadata_ref",
        node.raw_metadata_ref.len(),
        CONTEXT_MAX_REF_BYTES,
    )?;
    if node.last_event_time_ms != 0 {
        validate_context_timestamp(node.last_event_time_ms)?;
    }
    Ok(())
}

fn validate_context_event(event: &ContextEvent) -> Result<(), Status> {
    validate_context_required(
        event.event_time_ms != 0 && event.event_id_hash != 0,
        "event_time_ms and event_id_hash must be non-zero",
    )?;
    if event.valid_until_ms != 0 && event.valid_until_ms <= event.event_time_ms {
        return Err(Status::error(
            "invalid_argument",
            "valid_until_ms must be greater than event_time_ms",
        ));
    }
    validate_context_timestamp(event.event_time_ms)?;
    if event.valid_until_ms != 0 {
        validate_context_timestamp(event.valid_until_ms)?;
    }
    validate_context_score("confidence", event.confidence)?;
    validate_context_score("importance", event.importance)?;
    validate_context_byte_len("text", event.text.len(), CONTEXT_MAX_EVENT_TEXT_BYTES)?;
    validate_context_byte_len("source_ref", event.source_ref.len(), CONTEXT_MAX_REF_BYTES)?;
    if event.related_node_hashes.len() > CONTEXT_MAX_RELATED_NODE_HASHES {
        return Err(Status::error(
            "invalid_argument",
            "related_node_hashes exceeds maximum",
        ));
    }
    validate_context_byte_len(
        "compact_attrs",
        event.compact_attrs.len(),
        CONTEXT_MAX_COMPACT_ATTRS_BYTES,
    )
}

fn validate_context_filters(
    kinds: &[u32],
    statuses: &[u32],
    min_confidence: f32,
    min_importance: f32,
) -> Result<(), Status> {
    if kinds.len() > CONTEXT_MAX_FILTER_VALUES || statuses.len() > CONTEXT_MAX_FILTER_VALUES {
        return Err(Status::error("invalid_argument", "too many filter values"));
    }
    validate_context_score("min_confidence", min_confidence)?;
    validate_context_score("min_importance", min_importance)
}

fn validate_context_index_ref(index_ref: &ContextIndexRef) -> Result<(), Status> {
    validate_context_required(
        index_ref.primary_node_hash != 0
            && index_ref.primary_event_time_ms != 0
            && index_ref.event_id_hash != 0,
        "invalid context index ref",
    )?;
    validate_context_timestamp(index_ref.primary_event_time_ms)
}

fn validate_context_extracted_indexes(
    event: &ContextEvent,
    indexes: &ContextExtractedEventIndexes,
) -> Result<(), Status> {
    if !context_index_disabled(indexes, InternalContextIndex::EventKind) {
        validate_context_required(
            context_event_kind_hash(event) != 0,
            "event kind is required",
        )?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Status) {
        validate_context_required(indexes.status_hash != 0, "status_hash is required")?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Source) {
        validate_context_required(indexes.source_hash != 0, "source_hash is required")?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::EventTimeBucket) {
        validate_context_required(
            indexes.event_time_bucket_ms != 0,
            "event_time_bucket_ms is required",
        )?;
        validate_context_timestamp(indexes.event_time_bucket_ms)?;
    }
    if !context_index_disabled(indexes, InternalContextIndex::Entity) {
        for entity_hash in &indexes.entity_hashes {
            validate_context_required(*entity_hash != 0, "entity_hashes cannot contain zero")?;
        }
    }
    Ok(())
}

fn validate_context_audit_ref(audit_ref: &ContextAuditRef) -> Result<(), Status> {
    validate_context_required(
        audit_ref.node_hash != 0 && audit_ref.event_time_ms != 0,
        "audit ref node_hash and event_time_ms are required",
    )?;
    validate_context_timestamp(audit_ref.event_time_ms)?;
    validate_context_byte_len(
        "audit ref reason",
        audit_ref.reason.len(),
        CONTEXT_MAX_REF_BYTES,
    )
}

fn validate_context_pack_audit(audit: &ContextPackAudit) -> Result<(), Status> {
    validate_context_required(
        audit.session_hash != 0 && audit.request_time_ms != 0 && !audit.query_id.is_empty(),
        "session_hash, request_time_ms, and query_id are required",
    )?;
    validate_context_byte_len("query_id", audit.query_id.len(), CONTEXT_MAX_REF_BYTES)?;
    validate_context_timestamp(audit.request_time_ms)?;
    if audit.selected_refs.len() > CONTEXT_MAX_AUDIT_REFS
        || audit.blocked_refs.len() > CONTEXT_MAX_AUDIT_REFS
    {
        return Err(Status::error(
            "invalid_argument",
            "audit refs exceed maximum",
        ));
    }
    for audit_ref in &audit.selected_refs {
        validate_context_audit_ref(audit_ref)?;
    }
    for audit_ref in &audit.blocked_refs {
        validate_context_audit_ref(audit_ref)?;
    }
    Ok(())
}

fn validate_context_dirty_marker(marker: &ContextSummaryDirtyMarker) -> Result<(), Status> {
    validate_context_required(
        marker.node_hash != 0 && marker.event_time_ms != 0,
        "node_hash and event_time_ms are required",
    )?;
    if marker.propagate_depth > CONTEXT_MAX_PROPAGATE_DEPTH {
        return Err(Status::error(
            "invalid_argument",
            "propagate_depth exceeds maximum",
        ));
    }
    validate_context_timestamp(marker.event_time_ms)
}

fn validate_context_entity(entity: &ContextEntity) -> Result<(), Status> {
    validate_context_required(
        entity.entity_hash != 0 && entity.node_hash != 0 && entity.updated_at_ms != 0,
        "entity_hash, node_hash, and updated_at_ms are required",
    )?;
    validate_context_timestamp(entity.updated_at_ms)?;
    if entity.valid_from_ms != 0 {
        validate_context_timestamp(entity.valid_from_ms)?;
    }
    validate_context_score("confidence", entity.confidence)?;
    validate_context_byte_len(
        "entity name",
        entity.name.len(),
        CONTEXT_MAX_ENTITY_NAME_BYTES,
    )?;
    validate_context_byte_len(
        "entity value",
        entity.value.len(),
        CONTEXT_MAX_ENTITY_VALUE_BYTES,
    )?;
    if entity.source_event_hashes.len() > CONTEXT_MAX_AUDIT_REFS {
        return Err(Status::error(
            "invalid_argument",
            "source_event_hashes exceeds maximum",
        ));
    }
    Ok(())
}

fn validate_context_child_ref(child_ref: &ContextChildRef) -> Result<(), Status> {
    validate_context_required(
        child_ref.parent_hash != 0 && child_ref.child_hash != 0 && child_ref.updated_at_ms != 0,
        "parent_hash, child_hash, and updated_at_ms are required",
    )?;
    validate_context_timestamp(child_ref.updated_at_ms)
}

fn validate_context_embedding_vector(name: &'static str, vector: &[f32]) -> Result<(), Status> {
    validate_context_required(!vector.is_empty(), "embedding vector is required")?;
    if vector.len() > CONTEXT_MAX_EMBEDDING_DIM {
        return Err(Status::error(
            "invalid_argument",
            format!("{name} dimension exceeds maximum"),
        ));
    }
    if vector.iter().all(|value| value.is_finite()) {
        Ok(())
    } else {
        Err(Status::error(
            "invalid_argument",
            format!("{name} contains non-finite value"),
        ))
    }
}

fn validate_context_embedding(embedding: &ContextEmbedding) -> Result<(), Status> {
    validate_context_required(
        embedding.ref_hash != 0 && embedding.level != 0 && embedding.updated_at_ms != 0,
        "ref_hash, level, and updated_at_ms are required",
    )?;
    validate_context_timestamp(embedding.updated_at_ms)?;
    validate_context_embedding_vector("embedding vector", &embedding.vector)
}

fn validate_context_summary(summary: &ContextSummary) -> Result<(), Status> {
    validate_context_required(
        summary.node_hash != 0 && summary.level != 0 && summary.valid_from_ms != 0,
        "node_hash, level, and valid_from_ms are required",
    )?;
    validate_context_timestamp(summary.valid_from_ms)?;
    validate_context_byte_len(
        "summary text",
        summary.text.len(),
        CONTEXT_MAX_SUMMARY_BYTES,
    )
}

fn validate_context_compression_event(event: &ContextCompressionEvent) -> Result<(), Status> {
    validate_context_required(
        event.compression_id_hash != 0
            && event.node_hash != 0
            && event.source_start_ms != 0
            && event.source_end_ms != 0
            && event.compressed_time_ms != 0,
        "compression_id_hash, node_hash, source range, and compressed_time_ms are required",
    )?;
    validate_context_range(event.source_start_ms, event.source_end_ms)?;
    validate_context_timestamp(event.compressed_time_ms)?;
    validate_context_byte_len(
        "compression summary",
        event.summary.len(),
        CONTEXT_MAX_SUMMARY_BYTES,
    )
}

fn load_context_children(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Vec<ContextChildRef> {
    shard
        .context_children
        .get(object_key)
        .map(|series| {
            series
                .iter()
                .filter_map(|(timeline_key, address)| {
                    read_context_value::<ContextChildRef>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn load_context_embedding(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    ref_hash: u64,
) -> Option<ContextEmbedding> {
    shard
        .context_embeddings
        .get(&context_embedding_key(tenant_hash, ref_hash))
        .and_then(|address| {
            read_page_bytes(cache, page_store, shard_id, address)
                .and_then(|bytes| context_from_bytes::<ContextEmbedding>(&bytes))
        })
}

fn load_context_summaries(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextSummary> {
    shard
        .context_summaries
        .get(object_key)
        .map(|series| {
            series
                .range(0..context_timeline_end(as_of_ms))
                .take(context_limit(limit))
                .filter_map(|(timeline_key, address)| {
                    read_context_value::<ContextSummary>(
                        cache,
                        page_store,
                        shard_id,
                        *timeline_key,
                        address,
                    )
                })
                .filter(|summary| summary.valid_from_ms <= as_of_ms)
                .collect()
        })
        .unwrap_or_default()
}

fn load_latest_context_summary(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
    as_of_ms: u64,
) -> Option<ContextSummary> {
    load_context_summaries(
        cache, page_store, shard_id, shard, object_key, as_of_ms, None,
    )
    .into_iter()
    .max_by_key(|summary| summary.valid_from_ms)
}

fn load_context_compression_events(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    node_hashes: &[u64],
    start_time_ms: u64,
    end_time_ms: u64,
    limit: Option<usize>,
) -> Vec<ContextCompressionEvent> {
    let mut events = Vec::new();
    for node_hash in node_hashes
        .iter()
        .copied()
        .filter(|node_hash| *node_hash != 0)
    {
        let object_key = context_compression_key(tenant_hash, node_hash);
        if let Some(series) = shard.context_compressions.get(&object_key) {
            events.extend(series.iter().filter_map(|(timeline_key, address)| {
                read_context_value::<ContextCompressionEvent>(
                    cache,
                    page_store,
                    shard_id,
                    *timeline_key,
                    address,
                )
                .filter(|event| {
                    event.source_end_ms >= start_time_ms && event.source_start_ms <= end_time_ms
                })
            }));
        }
        if events.len() >= context_limit(limit) {
            break;
        }
    }
    events.sort_by(|left, right| {
        right
            .source_end_ms
            .cmp(&left.source_end_ms)
            .then_with(|| right.compressed_time_ms.cmp(&left.compressed_time_ms))
            .then_with(|| left.compression_id_hash.cmp(&right.compression_id_hash))
    });
    events.truncate(context_limit(limit));
    events
}

fn cosine_similarity(left: &[f32], right: &[f32]) -> f32 {
    if left.is_empty() || left.len() != right.len() {
        return 0.0;
    }
    let mut dot = 0.0_f32;
    let mut left_norm = 0.0_f32;
    let mut right_norm = 0.0_f32;
    for (l, r) in left.iter().zip(right.iter()) {
        dot += l * r;
        left_norm += l * l;
        right_norm += r * r;
    }
    if left_norm <= 0.0 || right_norm <= 0.0 {
        return 0.0;
    }
    dot / (left_norm.sqrt() * right_norm.sqrt())
}

#[allow(clippy::too_many_arguments)]
fn traverse_context_tree(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    shard_id: ShardId,
    shard: &ShardState,
    tenant_hash: u64,
    start_node_hash: u64,
    query_vector: &[f32],
    max_depth: Option<u32>,
    top_k_per_depth: Option<usize>,
    max_children_scored_per_parent: Option<usize>,
    max_candidate_nodes: Option<usize>,
    leaf_only: bool,
) -> Vec<ContextTraversedNode> {
    let max_depth = max_depth.unwrap_or(6).min(CONTEXT_MAX_TRAVERSAL_DEPTH);
    let top_k = top_k_per_depth
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_TOP_K)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let child_limit = max_children_scored_per_parent
        .unwrap_or(CONTEXT_MAX_LIMIT)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let candidate_limit = max_candidate_nodes
        .unwrap_or(CONTEXT_DEFAULT_TRAVERSAL_CANDIDATES)
        .max(1)
        .min(CONTEXT_MAX_LIMIT);
    let mut frontier = vec![ContextTraversedNode {
        node_hash: start_node_hash,
        depth: 0,
        score: 1.0,
    }];
    let mut results = Vec::new();
    for depth in 1..=max_depth {
        let mut scored_layer = Vec::new();
        for parent in &frontier {
            let child_key = context_child_key(tenant_hash, parent.node_hash);
            let mut children =
                load_context_children(cache, page_store, shard_id, shard, &child_key);
            children.sort_by_key(|child_ref| (child_ref.updated_at_ms, child_ref.child_hash));
            children.truncate(child_limit);
            for child in children {
                let Some(embedding) = load_context_embedding(
                    cache,
                    page_store,
                    shard_id,
                    shard,
                    tenant_hash,
                    child.child_hash,
                ) else {
                    continue;
                };
                let score = cosine_similarity(query_vector, &embedding.vector);
                if score > 0.0 {
                    scored_layer.push(ContextTraversedNode {
                        node_hash: child.child_hash,
                        depth,
                        score,
                    });
                }
            }
        }
        scored_layer.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.node_hash.cmp(&right.node_hash))
        });
        scored_layer.truncate(top_k);
        let mut next_frontier = Vec::new();
        for node in scored_layer {
            let child_key = context_child_key(tenant_hash, node.node_hash);
            let is_leaf =
                load_context_children(cache, page_store, shard_id, shard, &child_key).is_empty();
            next_frontier.push(node.clone());
            if !leaf_only || is_leaf {
                results.push(node);
                if results.len() >= candidate_limit {
                    return results;
                }
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    results
}

fn build_context_compression_event(
    tenant_hash: u64,
    node_hash: u64,
    source_start_ms: u64,
    source_end_ms: u64,
    compressed_time_ms: u64,
    selected: &[ContextEvent],
    truncated: bool,
) -> ContextCompressionEvent {
    let mut summary = format!("Temporal compression window {source_start_ms}-{source_end_ms}:");
    for event in selected {
        let mut text = event.text.clone();
        if text.len() > CONTEXT_MAX_COMPRESSION_SNIPPET_BYTES {
            text.truncate(CONTEXT_MAX_COMPRESSION_SNIPPET_BYTES);
        }
        summary.push(' ');
        summary.push_str(&text);
    }
    if truncated {
        summary.push_str(" additional source events truncated.");
    }
    if summary.len() > CONTEXT_MAX_SUMMARY_BYTES {
        summary.truncate(CONTEXT_MAX_SUMMARY_BYTES);
    }
    ContextCompressionEvent {
        compression_id_hash: stable_object_hash(&format!(
            "{tenant_hash}:{node_hash}:{source_start_ms}:{source_end_ms}:{}",
            selected
                .iter()
                .map(|event| event.event_id_hash.to_string())
                .collect::<Vec<_>>()
                .join(",")
        )),
        node_hash,
        source_start_ms,
        source_end_ms,
        compressed_time_ms: if compressed_time_ms == 0 {
            source_end_ms
        } else {
            compressed_time_ms
        },
        summary,
    }
}

fn cached_response(
    cache: &MultiLayerCache,
    key: CacheKey,
    source: impl FnOnce() -> CommandResponse,
) -> CommandResponse {
    if let Ok(Some(bytes)) = cache.get(&key) {
        if let Ok(response) = serde_json::from_slice::<CommandResponse>(&bytes) {
            return response;
        }
        let _ = cache.invalidate(&key);
    }
    let response = source();
    if let Ok(bytes) = serde_json::to_vec(&response) {
        cache.put_memory_only(key, bytes);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::golden::{
        cpp_api_golden_corpus_report, cpp_feature_sequence_golden_corpus_report,
    };
    use crate::page_store::PageStoreZoneState;
    use crate::types::{parse_cpp_feature_filters, ContextAuditRef};

    fn wait_for_fresh_admission_second() {
        loop {
            let elapsed = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock should be after unix epoch");
            if elapsed.subsec_millis() < 100 {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    // shared-corpus: context_events_segments_entities_child_refs context_event_index_audit_dirty_models
    #[test]
    fn context_models_match_cpp_keys_timeline_pages_and_filters() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let node = ContextNode {
            node_hash: 42,
            parent_hash: 7,
            kind: 3,
            canonical_name: "checkout".to_string(),
            l0: "service".to_string(),
            status: 1,
            last_event_time_ms: 1_000,
            summary_dirty: true,
            l1_ref: "l1://summary".to_string(),
            raw_metadata_ref: "raw://node".to_string(),
        };
        let upsert = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertNode {
                tenant_hash: 11,
                node: node.clone(),
            },
        });
        assert!(upsert.status.ok);
        assert!(matches!(
            upsert.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:node:11:42"
        ));

        let get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash: 11,
                node_hash: 42,
            },
        });
        assert!(matches!(
            get.response,
            CommandResponse::ContextNode { node: Some(ref stored), .. } if stored == &node
        ));
        let meta = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashGet {
                key: "ctx:node:11:42".to_string(),
                field: CONTEXT_NODE_FIELD.to_string(),
            },
        });
        assert!(matches!(
            meta.response,
            CommandResponse::Bytes { value: Some(ref bytes) }
                if ContextNode::decode_context_value(bytes).as_ref() == Some(&node)
        ));

        let entity = ContextEntity {
            entity_hash: 7001,
            node_hash: 42,
            entity_type: 1,
            name: "gpu_purchase_request".to_string(),
            value: "approved".to_string(),
            updated_at_ms: 1_000,
            valid_from_ms: 1_000,
            confidence: 0.97,
            source_event_hashes: vec![5],
        };
        let entity_upsert = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextUpsertEntity {
                tenant_hash: 11,
                entity: entity.clone(),
            },
        });
        assert!(entity_upsert.status.ok);
        assert!(matches!(
            entity_upsert.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:entity:11:42:7001"
        ));
        let entity_get = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetEntity {
                tenant_hash: 11,
                node_hash: 42,
                entity_hash: 7001,
            },
        });
        assert!(matches!(
            entity_get.response,
            CommandResponse::ContextEntity { entity: Some(ref stored), .. } if stored == &entity
        ));
        let entity_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEntities {
                tenant_hash: 11,
                node_hash: 42,
                entity_hashes: vec![7001, 8888],
                limit: Some(10),
            },
        });
        assert!(matches!(
            entity_query.response,
            CommandResponse::ContextEntities { ref entities, .. }
                if entities == &vec![entity.clone()]
        ));

        let event_a = ContextEvent {
            event_id_hash: 5,
            event_time_ms: 1_000,
            kind: 9,
            event_type: 2,
            actor_hash: 77,
            status: 1,
            valid_until_ms: 0,
            confidence: 0.9,
            importance: 0.7,
            text: "first".to_string(),
            source_ref: "src://a".to_string(),
            related_node_hashes: vec![42],
            compact_attrs: vec![1, 2, 3],
        };
        let mut event_b = event_a.clone();
        event_b.event_id_hash = 6;
        event_b.text = "second".to_string();

        for event in [event_a.clone(), event_b.clone()] {
            let write = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash: 11,
                    node_hash: 42,
                    event,
                    first_write_only: true,
                },
            });
            assert!(write.status.ok);
            assert!(matches!(
                write.response,
                CommandResponse::ContextObjectKey { ref object_key }
                    if object_key == "ctx:event:11:42"
            ));
        }
        let duplicate = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: ContextEvent {
                    text: "ignored".to_string(),
                    ..event_a.clone()
                },
                first_write_only: true,
            },
        });
        assert!(duplicate.status.ok);

        let queried = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEvents {
                tenant_hash: 11,
                node_hash: 42,
                start_time_ms: 999,
                end_time_ms: 1_001,
                limit: Some(10),
                current_valid_only: true,
                as_of_ms: 0,
                kinds: vec![9],
                statuses: vec![1],
                min_confidence: 0.8,
                min_importance: 0.6,
            },
        });
        assert!(matches!(
            queried.response,
            CommandResponse::ContextEvents { ref object_key, ref events }
                if object_key == "ctx:event:11:42"
                    && events.iter().map(|event| event.text.as_str()).collect::<Vec<_>>()
                        == vec!["first", "second"]
        ));

        let index_ref = ContextIndexRef {
            primary_node_hash: 42,
            primary_event_time_ms: 1_000,
            event_id_hash: 5,
        };
        let index_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteIndexRef {
                tenant_hash: 11,
                index_name: "actor".to_string(),
                index_value_hash: 77,
                scope_hash: 3,
                event_time_ms: 1_000,
                index_ref: index_ref.clone(),
            },
        });
        assert!(matches!(
            index_write.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctxidx:11:actor:77:3"
        ));
        let index_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryIndex {
                tenant_hash: 11,
                index_name: "actor".to_string(),
                index_value_hash: 77,
                scope_hash: 3,
                start_time_ms: 999,
                end_time_ms: 1_001,
                limit: None,
            },
        });
        assert!(matches!(
            index_query.response,
            CommandResponse::ContextIndexRefs { refs, .. } if refs == vec![index_ref]
        ));

        let extracted_event = ContextEvent {
            event_id_hash: 445,
            event_time_ms: 1_781_500_000_000,
            kind: 7,
            event_type: 7,
            actor_hash: 0,
            status: 1,
            valid_until_ms: 0,
            confidence: 0.96,
            importance: 0.88,
            text: "Finance confirmed the Project 1 GPU purchase approval.".to_string(),
            source_ref: "cursor://701".to_string(),
            related_node_hashes: vec![42],
            compact_attrs: Vec::new(),
        };
        let extracted = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteExtractedEvent {
                tenant_hash: 11,
                node_hash: 42,
                event: extracted_event.clone(),
                indexes: ContextExtractedEventIndexes {
                    scope_hash: 3001,
                    entity_hashes: vec![501, 502],
                    status_hash: 601,
                    source_hash: 701,
                    event_time_bucket_ms: 1_781_500_000_000,
                    disabled_indexes: Vec::new(),
                },
                first_write_only: true,
            },
        });
        assert!(matches!(
            extracted.response,
            CommandResponse::ContextExtractedEventWrite {
                ref event_object_key,
                written_index_count: 6,
                ref index_object_keys,
            } if event_object_key == "ctx:event:11:42" && index_object_keys.len() == 6
        ));
        for (index_name, value_hash, start_time_ms, end_time_ms) in [
            ("event_kind", 7, 1_781_499_999_999, 1_781_500_000_001),
            ("entity", 501, 1_781_499_999_999, 1_781_500_000_001),
            ("entity", 502, 1_781_499_999_999, 1_781_500_000_001),
            ("status", 601, 1_781_499_999_999, 1_781_500_000_001),
            ("source", 701, 1_781_499_999_999, 1_781_500_000_001),
            (
                "event_time_bucket",
                1_781_500_000_000,
                1_781_499_999_999,
                1_781_500_000_001,
            ),
        ] {
            let query = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQueryIndex {
                    tenant_hash: 11,
                    index_name: index_name.to_string(),
                    index_value_hash: value_hash,
                    scope_hash: 3001,
                    start_time_ms,
                    end_time_ms,
                    limit: Some(10),
                },
            });
            assert!(matches!(
                query.response,
                CommandResponse::ContextIndexRefs { refs, .. }
                    if refs.len() == 1
                        && refs[0].primary_node_hash == 42
                        && refs[0].primary_event_time_ms == extracted_event.event_time_ms
                        && refs[0].event_id_hash == extracted_event.event_id_hash
            ));
        }

        let disabled_source = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteExtractedEvent {
                tenant_hash: 11,
                node_hash: 43,
                event: ContextEvent {
                    event_id_hash: 446,
                    event_time_ms: 1_781_500_000_010,
                    kind: 8,
                    event_type: 8,
                    actor_hash: 0,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 0.9,
                    importance: 0.8,
                    text: "A low-noise event that should not be source-indexed.".to_string(),
                    source_ref: "cursor://701".to_string(),
                    related_node_hashes: vec![43],
                    compact_attrs: Vec::new(),
                },
                indexes: ContextExtractedEventIndexes {
                    scope_hash: 3001,
                    entity_hashes: Vec::new(),
                    status_hash: 602,
                    source_hash: 701,
                    event_time_bucket_ms: 1_781_500_000_000,
                    disabled_indexes: vec![InternalContextIndex::Source],
                },
                first_write_only: false,
            },
        });
        assert!(matches!(
            disabled_source.response,
            CommandResponse::ContextExtractedEventWrite {
                written_index_count: 3,
                ..
            }
        ));
        let disabled_source_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryIndex {
                tenant_hash: 11,
                index_name: "source".to_string(),
                index_value_hash: 701,
                scope_hash: 3001,
                start_time_ms: 1_781_500_000_009,
                end_time_ms: 1_781_500_000_011,
                limit: Some(10),
            },
        });
        assert!(matches!(
            disabled_source_query.response,
            CommandResponse::ContextIndexRefs { refs, .. } if refs.is_empty()
        ));

        let audit = ContextPackAudit {
            query_id: "q1".to_string(),
            session_hash: 99,
            request_time_ms: 2_000,
            query_hash: 123,
            max_prompt_tokens: 4096,
            selected_tokens: 128,
            selected_refs: vec![ContextAuditRef {
                node_hash: 42,
                event_time_ms: 1_000,
                reason: "ranked".to_string(),
            }],
            blocked_refs: Vec::new(),
        };
        let audit_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWritePackAudit {
                tenant_hash: 11,
                audit: audit.clone(),
            },
        });
        assert!(matches!(
            audit_write.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:audit:11:99"
        ));
        let audit_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryPackAudit {
                tenant_hash: 11,
                session_hash: 99,
                start_time_ms: 1_999,
                end_time_ms: 2_001,
                limit: None,
            },
        });
        assert!(matches!(
            audit_query.response,
            CommandResponse::ContextPackAudits { audits, .. } if audits == vec![audit]
        ));

        let marker = ContextSummaryDirtyMarker {
            node_hash: 42,
            event_time_ms: 3_000,
            reason: 4,
            propagate_depth: 2,
        };
        let dirty_write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkSummaryDirty {
                tenant_hash: 11,
                marker: marker.clone(),
            },
        });
        assert!(matches!(
            dirty_write.response,
            CommandResponse::ContextObjectKey { ref object_key }
                if object_key == "ctx:dirty:11:42"
        ));
        let dirty_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash: 11,
                node_hash: 42,
                start_time_ms: 2_999,
                end_time_ms: 3_001,
                limit: None,
            },
        });
        assert!(matches!(
            dirty_query.response,
            CommandResponse::ContextSummaryDirtyMarkers { markers, .. } if markers == vec![marker]
        ));

        assert!(
            engine
                .slot_storage_summaries(1)
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum::<u64>()
                >= 5
        );
        let recovery = engine.storage_recovery_report(1);
        assert!(
            recovery.total_page_refs >= 5,
            "context pages should be visible to recovery accounting"
        );
    }

    // shared-corpus: context_tree_embedding_summary_compression
    #[test]
    fn context_tree_embedding_summary_and_compression_match_cpp_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        const TENANT: u64 = 1001;
        const ROOT: u64 = 10;
        const GPU: u64 = 20;
        const COST: u64 = 30;
        const EVENT_TIME: u64 = 1_781_500_000_000;

        for node in [
            ContextNode {
                node_hash: ROOT,
                parent_hash: 0,
                kind: 1,
                canonical_name: "company_a".to_string(),
                l0: "Company A context root.".to_string(),
                status: 0,
                last_event_time_ms: 0,
                summary_dirty: false,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
            },
            ContextNode {
                node_hash: GPU,
                parent_hash: ROOT,
                kind: 2,
                canonical_name: "gpu_purchase".to_string(),
                l0: "GPU purchase leaf node.".to_string(),
                status: 0,
                last_event_time_ms: 0,
                summary_dirty: false,
                l1_ref: String::new(),
                raw_metadata_ref: String::new(),
            },
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode {
                    tenant_hash: TENANT,
                    node,
                },
            });
            assert!(response.status.ok);
        }

        let child_gpu = ContextChildRef {
            parent_hash: ROOT,
            child_hash: GPU,
            updated_at_ms: EVENT_TIME,
        };
        for (child_ref, created, count) in [
            (child_gpu.clone(), true, 1),
            (
                ContextChildRef {
                    parent_hash: ROOT,
                    child_hash: COST,
                    updated_at_ms: EVENT_TIME,
                },
                true,
                2,
            ),
            (child_gpu.clone(), false, 2),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertChildRef {
                    tenant_hash: TENANT,
                    child_ref,
                },
            });
            assert!(matches!(
                response.response,
                CommandResponse::ContextChildRefs {
                    ref object_key,
                    created: Some(actual_created),
                    parent_child_count: Some(actual_count),
                    ..
                } if object_key == "ctx:child:1001:10"
                    && actual_created == created
                    && actual_count == count
            ));
        }
        let children = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryChildren {
                tenant_hash: TENANT,
                parent_hash: ROOT,
                limit: Some(10),
            },
        });
        assert!(matches!(
            children.response,
            CommandResponse::ContextChildRefs { refs, .. }
                if refs.len() == 2 && refs[0].child_hash == GPU
        ));

        for (ref_hash, first, second) in [(GPU, 1.0, 0.0), (COST, 0.0, 1.0)] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertEmbedding {
                    tenant_hash: TENANT,
                    embedding: ContextEmbedding {
                        ref_hash,
                        level: 1,
                        vector: vec![first, second],
                        updated_at_ms: EVENT_TIME,
                    },
                },
            });
            assert!(response.status.ok);
        }
        let traversal = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextTraverseTree {
                tenant_hash: TENANT,
                start_node_hash: ROOT,
                query_vector: vec![1.0, 0.0],
                max_depth: Some(2),
                top_k_per_depth: Some(1),
                max_children_scored_per_parent: Some(10),
                max_candidate_nodes: Some(4),
                leaf_only: true,
            },
        });
        assert!(matches!(
            traversal.response,
            CommandResponse::ContextTraversedNodes { ref nodes }
                if nodes.len() == 1 && nodes[0].node_hash == GPU && nodes[0].score > 0.99
        ));

        for (text, valid_from_ms) in [
            ("L0 GPU purchase summary.", EVENT_TIME),
            ("Latest overall GPU purchase summary.", EVENT_TIME + 5),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertSummary {
                    tenant_hash: TENANT,
                    summary: ContextSummary {
                        node_hash: GPU,
                        level: 1,
                        text: text.to_string(),
                        valid_from_ms,
                    },
                },
            });
            assert!(response.status.ok);
        }
        let summaries = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaries {
                tenant_hash: TENANT,
                node_hash: GPU,
                level: 1,
                as_of_ms: EVENT_TIME + 1,
                limit: Some(10),
            },
        });
        assert!(matches!(
            summaries.response,
            CommandResponse::ContextSummaries { ref summaries, .. }
                if summaries.len() == 1 && summaries[0].text == "L0 GPU purchase summary."
        ));

        let compression = ContextCompressionEvent {
            compression_id_hash: 5001,
            node_hash: GPU,
            source_start_ms: EVENT_TIME - 1000,
            source_end_ms: EVENT_TIME,
            compressed_time_ms: EVENT_TIME,
            summary: "Older GPU purchase timeline compressed into one summary.".to_string(),
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextWriteCompressionEvent {
                tenant_hash: TENANT,
                event: compression.clone(),
            },
        });
        assert!(response.status.ok);
        let compression_query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryCompressionEvents {
                tenant_hash: TENANT,
                node_hashes: vec![GPU],
                start_time_ms: EVENT_TIME - 2000,
                end_time_ms: EVENT_TIME + 1,
                limit: Some(10),
            },
        });
        assert!(matches!(
            compression_query.response,
            CommandResponse::ContextCompressionEvents { ref events, .. }
                if events == &vec![compression.clone()]
        ));

        let node_context = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryNodeContext {
                tenant_hash: TENANT,
                node_hash: GPU,
                summary_level: Some(1),
                as_of_ms: EVENT_TIME + 10,
                cold_start_time_ms: EVENT_TIME - 2000,
                cold_end_time_ms: EVENT_TIME + 1,
                compression_limit: Some(10),
            },
        });
        assert!(matches!(
            node_context.response,
            CommandResponse::ContextNodeContext {
                node_exists: true,
                overall_summary_exists: true,
                overall_summary: Some(ref summary),
                ref cold_window_summaries,
                ..
            } if summary.text == "Latest overall GPU purchase summary."
                && cold_window_summaries.len() == 1
                && cold_window_summaries[0].summary == compression.summary
        ));
    }

    // shared-corpus: context_temporal_compression_replayable_summary
    #[test]
    fn context_temporal_compression_builds_replayable_summary_without_deleting_sources() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            16 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        const TENANT: u64 = 3003;
        const NODE: u64 = 9100;
        const START: u64 = 1_781_400_000_000;
        const COMPRESSED_AT: u64 = 1_781_500_000_000;

        for (offset_ms, event_id, text) in [
            (0, 7001, "Week-old approval was created."),
            (10, 7002, "Week-old approval was reviewed by finance."),
            (20, 7003, "Week-old approval was confirmed by infra."),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash: TENANT,
                    node_hash: NODE,
                    event: ContextEvent {
                        event_id_hash: event_id,
                        event_time_ms: START + offset_ms,
                        kind: 7,
                        event_type: 7,
                        actor_hash: 0,
                        status: 0,
                        valid_until_ms: 0,
                        confidence: 0.96,
                        importance: 0.82,
                        text: text.to_string(),
                        source_ref: String::new(),
                        related_node_hashes: Vec::new(),
                        compact_attrs: Vec::new(),
                    },
                    first_write_only: false,
                },
            });
            assert!(response.status.ok);
        }

        let compressed = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextCompressEvents {
                tenant_hash: TENANT,
                node_hash: NODE,
                source_start_ms: START,
                source_end_ms: START + 20,
                compressed_time_ms: COMPRESSED_AT,
                max_source_events: Some(2),
                min_confidence: 0.9,
                min_importance: 0.8,
            },
        });
        assert!(matches!(
            compressed.response,
            CommandResponse::ContextCompressionEvents {
                ref object_key,
                ref events,
                source_event_count: Some(2),
                truncated_source_events: Some(true),
            } if object_key == "ctx:compress:3003:9100"
                && events.len() == 1
                && events[0].summary.contains("Temporal compression window")
                && events[0].summary.contains("Week-old approval was created")
        ));

        let raw_events = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEvents {
                tenant_hash: TENANT,
                node_hash: NODE,
                start_time_ms: START,
                end_time_ms: START + 20,
                limit: Some(10),
                current_valid_only: false,
                as_of_ms: 0,
                kinds: Vec::new(),
                statuses: Vec::new(),
                min_confidence: 0.0,
                min_importance: 0.0,
            },
        });
        assert!(matches!(
            raw_events.response,
            CommandResponse::ContextEvents { ref events, .. } if events.len() == 3
        ));
    }

    #[test]
    fn live_page_segment_ids_scan_all_index_backed_data_models() {
        let mut shard = ShardState::default();
        shard.strings.insert(
            "string".to_string(),
            PageAddress {
                page_segment_id: 7,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.hashes.entry("hash".to_string()).or_default().insert(
            "field".to_string(),
            PageAddress {
                page_segment_id: 8,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.sets.entry("set".to_string()).or_default().insert(
            b"member".to_vec(),
            PageAddress {
                page_segment_id: 9,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard
            .features
            .entry("feature".to_string())
            .or_default()
            .insert(
                10,
                PageAddress {
                    page_segment_id: 10,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
            );
        shard
            .sequences
            .entry("sequence".to_string())
            .or_default()
            .insert(
                11,
                PageAddress {
                    page_segment_id: 11,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
            );
        shard.ips.entry("ips".to_string()).or_default().insert(
            12,
            PageAddress {
                page_segment_id: 12,
                offset: 0,
                length: 1,
                page_id: None,
                object_id: None,
                routing_slot: None,
                zone_id: None,
                sha256: None,
            },
        );
        shard.ips_meta.entry("ips".to_string()).or_default().insert(
            13,
            IpsPointMeta {
                address: PageAddress {
                    page_segment_id: 13,
                    offset: 0,
                    length: 1,
                    page_id: None,
                    object_id: None,
                    routing_slot: None,
                    zone_id: None,
                    sha256: None,
                },
                action_type: Some(1),
                table_id: Some(2),
                request_id: Some("r".to_string()),
            },
        );
        shard
            .risk
            .entry("risk".to_string())
            .or_default()
            .insert(14, 1);

        let ids = collect_live_page_segment_ids(&shard)
            .into_iter()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![7, 8, 9, 10, 11, 12]);
    }

    #[test]
    fn page_compaction_rewrites_live_addresses_and_allows_old_segment_gc() {
        let page_dir = unique_temp_path("compact-pages");
        let index_dir = unique_temp_path("compact-index");
        let page_store = LocalPageStore::new(&page_dir);
        let engine = TemporalEngine::with_cache_page_store_and_index_dir(
            MultiLayerCache::default(),
            page_store.clone(),
            &index_dir,
        );
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v1".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v2".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(engine.live_page_segment_ids(1), vec![0]);

        let report = engine.compact_shard_pages(1).unwrap();
        assert_eq!(report.previous_page_segment_id, 0);
        assert_eq!(report.compacted_page_segment_id, 1);
        assert_eq!(report.rewritten_page_refs, 2);
        assert_eq!(report.stale_page_segment_ids, vec![0]);
        assert_eq!(report.before.live_page_segment_count, 1);
        assert_eq!(report.before.total_page_count, 3);
        assert_eq!(report.before.live_page_refs, 2);
        assert_eq!(report.before.stale_page_estimate, 1);
        assert_eq!(report.before.live_ref_density_basis_points, 6_666);
        assert_eq!(report.after.live_page_segment_count, 1);
        assert_eq!(report.after.total_page_count, 2);
        assert_eq!(report.after.live_page_refs, 2);
        assert_eq!(report.after.stale_page_estimate, 0);
        assert_eq!(report.after.live_ref_density_basis_points, 10_000);
        assert_eq!(engine.live_page_segment_ids(1), vec![1]);
        {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let string_address = shard.strings.get("k").expect("string address");
            let hash_address = shard
                .hashes
                .get("h")
                .and_then(|fields| fields.get("f"))
                .expect("hash address");
            assert_eq!(
                string_address.object_id,
                Some(stable_page_object_id(1, "string", "k", None))
            );
            assert_eq!(
                string_address.routing_slot,
                Some(page_routing_slot("k", 0, u32::MAX))
            );
            assert_eq!(
                hash_address.object_id,
                Some(stable_page_object_id(1, "hash", "h", Some("f")))
            );
            assert_eq!(
                hash_address.routing_slot,
                Some(page_routing_slot("h", 0, u32::MAX))
            );
        }

        let gc = page_store
            .gc_segments_before_with_live_refs(1, engine.live_page_segment_ids(1))
            .unwrap();
        assert_eq!(gc.removed_page_segment_ids, vec![0]);
        assert_eq!(page_store.segment_ids().unwrap(), vec![1]);

        let restarted = TemporalEngine::with_cache_page_store_and_index_dir(
            MultiLayerCache::default(),
            page_store,
            &index_dir,
        );
        restarted.load_shard(1);
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v2".to_vec())
            }
        );
        assert_eq!(
            restarted
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashGet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"hv".to_vec())
            }
        );
    }

    #[test]
    fn recovery_reports_owner_mismatch_and_compaction_refuses_it() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "owned".to_string(),
                        value: b"value".to_vec(),
                    },
                })
                .status
                .ok
        );

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let address = shard.strings.get_mut("owned").expect("string address");
            address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
        }

        let recovery = engine.storage_recovery_report(1);
        assert_eq!(recovery.owner_mismatch_page_refs.len(), 1);
        assert!(!recovery.segment_integrity.integrity_ok);
        assert_eq!(recovery.segment_integrity.owner_mismatch_page_ref_count, 1);
        assert_eq!(recovery.segment_integrity.missing_owner_page_ref_count, 0);
        assert_eq!(recovery.object_lifecycle.live_object_ids, 1);
        assert_eq!(recovery.object_lifecycle.live_page_refs, 1);
        assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
        assert_eq!(
            recovery.owner_mismatch_page_refs[0].expected_object_id,
            stable_page_object_id(1, "string", "owned", None)
        );
        assert_eq!(recovery.boundary.owner_mismatch_page_refs.len(), 1);
        assert_eq!(
            recovery.boundary.object_lifecycle.owner_mismatch_page_refs,
            1
        );

        let err = engine.compact_shard_pages(1).unwrap_err();
        assert_eq!(err.code, "page_compaction_owner_mismatch");
    }

    #[test]
    fn recovery_reports_reused_object_id_conflicts() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for key in ["first", "second"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: key.as_bytes().to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let reused_object_id = {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let first_object_id = shard
                .strings
                .get("first")
                .and_then(|address| address.object_id)
                .expect("first object id");
            let second = shard.strings.get_mut("second").expect("second address");
            second.object_id = Some(first_object_id);
            first_object_id
        };

        let recovery = engine.storage_recovery_report(1);
        assert_eq!(recovery.object_lifecycle.live_object_ids, 2);
        assert_eq!(recovery.object_lifecycle.live_page_refs, 2);
        assert_eq!(recovery.object_lifecycle.reused_object_id_conflicts, 1);
        assert_eq!(
            recovery.object_lifecycle.reused_object_ids,
            vec![reused_object_id]
        );
        assert_eq!(recovery.object_lifecycle.owner_mismatch_page_refs, 1);
        assert_eq!(
            recovery
                .boundary
                .object_lifecycle
                .reused_object_id_conflicts,
            1
        );
    }

    #[test]
    fn crash_recovery_report_covers_oplog_index_page_and_zone_manifest() {
        let cache_dir = unique_temp_path("recovery-cache");
        let page_dir = unique_temp_path("recovery-pages");
        let index_dir = unique_temp_path("recovery-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v1".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );

        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);

        assert!(report.index_bytes > 0);
        assert!(report.index_write_atomic);
        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_segment_ids, vec![0, 1]);
        assert_eq!(report.live_page_segment_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
        assert_eq!(report.readable_page_refs, 2);
        assert!(report.all_live_pages_readable);
        assert!(report.segment_integrity.integrity_ok);
        assert!(!report.segment_integrity.reclaim_required);
        assert_eq!(report.segment_integrity.indexed_page_segment_count, 2);
        assert_eq!(report.segment_integrity.discovered_page_segment_count, 2);
        assert_eq!(report.segment_integrity.live_page_segment_count, 2);
        assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
        assert_eq!(report.zone_descriptors.len(), 2);
        assert_eq!(report.zone_descriptors[0].state, PageStoreZoneState::Sealed);
        assert_eq!(report.zone_descriptors[1].state, PageStoreZoneState::Active);
        assert_eq!(report.zone_summary.sealed_zones, 1);
        assert_eq!(report.zone_summary.active_zones, 1);
        assert_eq!(report.zone_summary.delayed_destroy_zones, 0);
        assert_eq!(
            report.zone_summary.sealed_physical_bytes,
            report.zone_descriptors[0].physical_bytes
        );
        assert_eq!(
            report.zone_summary.active_physical_bytes,
            report.zone_descriptors[1].physical_bytes
        );
        assert_eq!(
            report.zone_summary.live_physical_bytes,
            report.zone_descriptors[0].physical_bytes + report.zone_descriptors[1].physical_bytes
        );
        assert_eq!(report.page_segment_live_reports.len(), 2);
        assert_eq!(report.page_segment_live_reports[0].page_segment_id, 0);
        assert_eq!(report.page_segment_live_reports[0].page_count, 1);
        assert_eq!(report.page_segment_live_reports[0].live_page_refs, 1);
        assert_eq!(
            report.page_segment_live_reports[0].readable_live_page_refs,
            1
        );
        assert_eq!(
            report.page_segment_live_reports[0].unreadable_live_page_refs,
            0
        );
        assert_eq!(report.page_segment_live_reports[0].stale_page_estimate, 0);
        assert_eq!(
            report.page_segment_live_reports[0].live_ref_density_basis_points,
            10_000
        );
        assert_eq!(report.page_segment_live_reports[0].live_object_count, 1);
        assert_eq!(
            report.page_segment_live_reports[0].live_routing_slot_count,
            1
        );
        assert_eq!(report.page_segment_live_reports[0].live_logical_bytes, 2);
        assert!(report.page_segment_live_reports[0].live_physical_bytes > 0);

        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashGet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"hv".to_vec())
            }
        );
    }

    #[test]
    fn crash_recovery_report_marks_stale_segment_density_after_overwrite() {
        let cache_dir = unique_temp_path("recovery-density-cache");
        let page_dir = unique_temp_path("recovery-density-pages");
        let index_dir = unique_temp_path("recovery-density-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "hot".to_string(),
                        value: b"old".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "hot".to_string(),
                        value: b"new".to_vec(),
                    },
                })
                .status
                .ok
        );

        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);
        let segment = report
            .page_segment_live_reports
            .iter()
            .find(|segment| segment.page_segment_id == 0)
            .expect("segment 0 live-density report");

        assert_eq!(segment.page_count, 2);
        assert_eq!(segment.live_page_refs, 1);
        assert_eq!(segment.readable_live_page_refs, 1);
        assert_eq!(segment.stale_page_estimate, 1);
        assert_eq!(segment.live_ref_density_basis_points, 5_000);
        assert_eq!(segment.live_logical_bytes, 3);
        assert_eq!(segment.live_object_count, 1);
        assert_eq!(segment.live_routing_slot_count, 1);
    }

    #[test]
    fn crash_recovery_rebuilds_missing_zone_manifest_from_page_stream() {
        let cache_dir = unique_temp_path("recovery-rebuild-cache");
        let page_dir = unique_temp_path("recovery-rebuild-pages");
        let index_dir = unique_temp_path("recovery-rebuild-index");
        let engine = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "before".to_string(),
                        value: b"before".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "after".to_string(),
                        value: b"after".to_vec(),
                    },
                })
                .status
                .ok
        );

        fs::remove_file(page_dir.join("page_zone_manifest.json")).unwrap();
        let recovered = TemporalEngine::with_local_dirs(256, &cache_dir, &page_dir, &index_dir);
        recovered.load_shard(1);
        let report = recovered.storage_recovery_report(1);

        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_segment_ids, vec![0, 1]);
        assert_eq!(report.live_page_segment_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
        assert!(report.all_live_pages_readable);
        assert_eq!(report.zone_descriptors.len(), 2);
        assert_eq!(report.zone_descriptors[0].state, PageStoreZoneState::Sealed);
        assert_eq!(report.zone_descriptors[1].state, PageStoreZoneState::Active);
        assert_eq!(report.zone_summary.sealed_zones, 1);
        assert_eq!(report.zone_summary.active_zones, 1);
        assert!(report.zone_summary.live_physical_bytes > 0);
        assert!(page_dir.join("page_zone_manifest.json").exists());
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "before".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"before".to_vec())
            }
        );
        assert_eq!(
            recovered
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "after".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"after".to_vec())
            }
        );
    }

    #[test]
    fn durable_writes_stamp_stable_object_ids_on_page_addresses() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    table_name: "table".to_string(),
                    shard_uri: "local://1".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    load_version: 1,
                    local_node_id: None,
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashSet {
                        key: "h".to_string(),
                        field: "f".to_string(),
                        value: b"hv".to_vec(),
                    },
                })
                .status
                .ok
        );

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("loaded shard");
        let string_address = shard.strings.get("k").expect("string address");
        let hash_address = shard
            .hashes
            .get("h")
            .and_then(|fields| fields.get("f"))
            .expect("hash address");

        assert_eq!(
            string_address.object_id,
            Some(stable_page_object_id(1, "string", "k", None))
        );
        assert_eq!(
            string_address.routing_slot,
            Some(page_routing_slot("k", 10, 20))
        );
        assert_eq!(string_address.zone_id, Some(string_address.page_segment_id));
        assert_eq!(
            hash_address.object_id,
            Some(stable_page_object_id(1, "hash", "h", Some("f")))
        );
        assert_eq!(
            hash_address.routing_slot,
            Some(page_routing_slot("h", 10, 20))
        );
        assert_eq!(hash_address.zone_id, Some(hash_address.page_segment_id));
        assert_ne!(string_address.object_id, hash_address.object_id);
    }

    #[test]
    fn string_setex_sets_value_and_ttl() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                        ttl_ms: 60_000,
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        let ttl = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonTtl {
                key: "k".to_string(),
            },
        });
        let CommandResponse::Integer { value } = ttl.response else {
            panic!("expected ttl integer response");
        };
        assert!(value > 0);
    }

    #[test]
    fn expiry_sweep_removes_expired_records_without_lazy_read() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: "expire-me".to_string(),
                        value: b"gone".to_vec(),
                        ttl_ms: 1,
                    },
                })
                .status
                .ok
        );
        std::thread::sleep(std::time::Duration::from_millis(5));

        let report = engine.sweep_expired_records(1).unwrap();
        assert_eq!(report.expired_records_removed, 1);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "expire-me".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert_eq!(
            engine
                .sweep_expired_records(1)
                .unwrap()
                .expired_records_removed,
            0
        );
    }

    // shared-corpus: storage_manager_expire_cursor_scan_limits
    #[test]
    fn expiry_sweep_uses_hot_cursors_limits_and_cold_load_policy() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for key in ["expire-hot-a", "expire-hot-b", "expire-hot-c"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSetEx {
                            key: key.to_string(),
                            value: key.as_bytes().to_vec(),
                            ttl_ms: 1,
                        },
                    })
                    .status
                    .ok
            );
        }
        {
            let mut shards = engine.shards.write().expect("shards lock poisoned");
            let shard = shards.get_mut(&1).unwrap();
            shard
                .expires_at_ms
                .insert("expire-cold-a".to_string(), now_ms().saturating_sub(1));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));

        let first = engine
            .sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                max_hot_slots_per_round: 2,
                max_cold_slots_per_round: 1,
                load_cold_slots: false,
                ..ShardExpirySweepRequest::default()
            })
            .unwrap();
        assert_eq!(first.hot_slots_scanned, 2);
        assert_eq!(first.cold_slots_scanned, 1);
        assert_eq!(first.scanned_records, 3);
        assert_eq!(first.expired_records_removed, 2);
        assert_eq!(first.loaded_for_expire, 0);
        assert_eq!(first.skipped_records, 1);
        assert_eq!(first.next_hot_cursor.as_deref(), Some("expire-hot-b"));
        assert!(first.load_on_expire_only_when_needed);

        let second = engine
            .sweep_expired_records_with_request(ShardExpirySweepRequest {
                shard_id: 1,
                hot_cursor: first.next_hot_cursor,
                cold_cursor: first.next_cold_cursor,
                max_hot_slots_per_round: 2,
                max_cold_slots_per_round: 1,
                load_cold_slots: true,
            })
            .unwrap();
        assert_eq!(second.hot_slots_scanned, 1);
        assert_eq!(second.cold_slots_scanned, 1);
        assert_eq!(second.expired_records_removed, 2);
        assert_eq!(second.loaded_for_expire, 1);
    }

    #[test]
    fn string_get_uses_memory_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let engine = TemporalEngine::new(cache.clone());
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        let stats = cache.stats();
        assert_eq!(stats.misses, 2);
        assert!(stats.memory_hits >= 1);
        assert!(stats.puts >= 2);
    }

    #[test]
    fn memory_miss_reads_local_page_file_using_index_address() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(page_store.stats().writes, 1);

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);

        let second = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            second.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().memory_hits, 1);

        cache.clear_memory_for_test();
        let third = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            third.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().disk_hits, 1);
    }

    #[test]
    fn three_layer_cache_reads_memory_then_block_cache_then_local_file() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 1);
        assert_eq!(cache.stats().puts, 2);
        assert!(cache.stats().memory_bytes > 0);
        assert!(cache.stats().disk_bytes > 0);

        let memory = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            memory.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert!(cache.stats().memory_hits >= 1);
        assert_eq!(page_store.stats().reads, 1);

        cache.clear_memory_for_test();
        let block_cache = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            block_cache.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(cache.stats().disk_hits, 1);
        assert_eq!(page_store.stats().reads, 1);

        cache.invalidate_shard(1).unwrap();
        let local_file = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            local_file.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(page_store.stats().reads, 2);
        assert!(cache.stats().puts >= 4);
        assert!(cache.stats().memory_bytes > 0);
        assert!(cache.stats().disk_bytes > 0);

        let observation = engine.rust_storage_observation(1).unwrap();
        assert!(observation.observed_memory_hit);
        assert!(observation.observed_block_cache_hit);
        assert!(observation.observed_local_file_read);
        assert!(observation.observed_cache_invalidation);
        assert!(observation.cache_memory_bytes > 0);
        assert!(observation.cache_disk_bytes > 0);
        assert!(observation.local_page_bytes_written > 0);
        assert!(observation.local_page_bytes_read > 0);
    }

    #[test]
    fn tiny_memory_cache_eviction_refills_from_persistence_then_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        let cache = engine.cache();
        let page_store = engine.page_store();
        engine.load_shard(1);

        let target_value = b"target-value-0123456789".to_vec();
        for (key, value) in [
            ("target", target_value.clone()),
            ("evict-a", b"eviction-value-a-0123456789".to_vec()),
            ("evict-b", b"eviction-value-b-0123456789".to_vec()),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        let first_read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            first_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(page_store.stats().reads, 1);

        for key in ["evict-a", "evict-b"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert!(
                response.status.ok,
                "eviction pressure read should pass: {response:?}"
            );
        }
        assert!(
            cache.stats().memory_evictions > 0,
            "reading multiple persisted blocks through a tiny memory cache should evict older blocks"
        );
        assert!(
            cache.stats().disk_bytes > 0,
            "persistent page read should populate block-cache files"
        );

        let target_page_key = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .expect("shard should exist")
                .strings
                .get("target")
                .expect("target address should exist");
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };
        assert_eq!(
            cache.get_memory(&target_page_key),
            None,
            "target page block should have been evicted from memory"
        );

        let disk_hits_before = cache.stats().disk_hits;
        let file_reads_before_block_hit = page_store.stats().reads;
        let second_read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            second_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            page_store.stats().reads,
            file_reads_before_block_hit,
            "memory miss should hit disk block cache instead of rereading page store"
        );
        assert!(
            cache.stats().disk_hits > disk_hits_before,
            "block cache should serve the read and promote it to memory"
        );
        assert_eq!(
            cache.get_memory(&target_page_key),
            Some(target_value),
            "disk block hit should promote the page block into memory"
        );
    }

    #[test]
    fn restarted_engine_refills_tiny_memory_cache_from_persistent_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let original =
            TemporalEngine::with_local_dirs(32, dir.path().join("cache-a"), &page_dir, &index_dir);
        original.load_shard(1);
        let target_value = b"restart-target-value-0123456789".to_vec();
        let write = original.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "target".to_string(),
                value: target_value.clone(),
            },
        });
        assert!(write.status.ok, "{write:?}");
        assert_eq!(original.page_store().stats().writes, 1);

        let restarted =
            TemporalEngine::with_local_dirs(32, dir.path().join("cache-b"), &page_dir, &index_dir);
        restarted.load_shard(1);
        let restarted_cache = restarted.cache();
        let restarted_page_store = restarted.page_store();
        let target_page_key = {
            let shards = restarted.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .expect("shard should exist after index replay")
                .strings
                .get("target")
                .expect("target address should be restored from index")
                .clone();
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };

        let first_read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            first_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            restarted_page_store.stats().reads,
            1,
            "restart should miss memory and load the persisted page once"
        );
        assert_eq!(
            restarted_cache.get_memory(&target_page_key),
            Some(target_value.clone()),
            "persistent page read should refill the memory cache"
        );
        assert!(
            restarted_cache.stats().disk_bytes > 0,
            "persistent page read should also write the disk block cache"
        );

        restarted_cache.clear_memory_for_test();
        assert_eq!(restarted_cache.get_memory(&target_page_key), None);
        let disk_hits_before = restarted_cache.stats().disk_hits;
        let page_reads_before = restarted_page_store.stats().reads;
        let second_read = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            second_read.response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        assert_eq!(
            restarted_page_store.stats().reads,
            page_reads_before,
            "memory miss after restart should use the disk block cache"
        );
        assert!(
            restarted_cache.stats().disk_hits > disk_hits_before,
            "disk block cache should serve the second read"
        );
        assert_eq!(
            restarted_cache.get_memory(&target_page_key),
            Some(target_value),
            "disk block hit should promote the page block back into memory"
        );
    }

    #[test]
    fn page_reads_fill_compressed_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::with_block_options(
            1024 * 1024,
            dir.path().join("cache"),
            crate::cache::CacheBlockOptions {
                compression: crate::cache::CacheCompression::Zstd { level: 1 },
                min_compress_bytes: 16,
            },
        );
        let engine = TemporalEngine::with_cache_page_store_and_index_dir(
            cache.clone(),
            LocalPageStore::new(dir.path().join("pages")),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let value = vec![b'x'; 4096];
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "large".to_string(),
                value: value.clone(),
            },
        });

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large".to_string(),
            },
        });
        assert_eq!(
            first.response,
            CommandResponse::Bytes { value: Some(value) }
        );
        assert!(cache.stats().compressed_puts >= 1);
        assert!(cache.stats().compression_bytes_saved > 0);

        cache.clear_memory_for_test();
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large".to_string(),
            },
        });
        assert!(cache.stats().compressed_hits >= 1);
    }

    #[test]
    fn local_dirs_constructor_applies_page_store_compression_options() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs_and_page_store_options(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
            PageStoreOptions {
                compression_enabled: false,
                ..PageStoreOptions::default()
            },
        );
        engine.load_shard(1);
        let value = b"engine-page-policy-".repeat(80);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "large-policy".to_string(),
                value: value.clone(),
            },
        });

        let page_store = engine.page_store();
        let stats = page_store.stats();
        assert_eq!(stats.writes, 1);
        assert_eq!(stats.compressed_records_written, 0);
        assert_eq!(stats.compression_bytes_saved, 0);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "large-policy".to_string(),
            },
        });
        assert_eq!(read.response, CommandResponse::Bytes { value: Some(value) });
    }

    #[test]
    fn write_invalidates_cached_string() {
        let dir = tempfile::tempdir().unwrap();
        let cache = MultiLayerCache::new(1024, dir.path());
        let engine = TemporalEngine::new(cache.clone());
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"old".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"new".to_vec(),
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"new".to_vec())
            }
        );
        assert!(cache.stats().invalidations >= 2);
    }

    #[test]
    fn async_storage_string_write_stays_on_hot_memory_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    },
                })
                .ok
        );

        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "hot".to_string(),
                value: b"value".to_vec(),
            },
        });
        assert!(write.status.ok);
        assert_eq!(engine.page_store().stats().writes, 0);
        assert_eq!(engine.oplog_store().stats(1).writes, 0);
        assert_eq!(engine.index_log_store().stats(1).writes, 0);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "hot".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"value".to_vec())
            }
        );
        assert_eq!(engine.page_store().stats().reads, 0);
        assert!(engine.cache().stats().memory_hits >= 1);
    }

    #[test]
    fn durable_execute_overrides_async_storage_for_raft_local_file_path() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 1,
                    config: Config {
                        version: 2,
                        async_storage: true,
                        ..Config::default()
                    },
                })
                .ok
        );

        let write = engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "raft".to_string(),
                value: b"value".to_vec(),
            },
        });
        assert!(write.status.ok);
        assert_eq!(engine.page_store().stats().writes, 1);
        assert_eq!(engine.oplog_store().stats(1).writes, 1);
        assert_eq!(engine.index_log_store().stats(1).writes, 1);

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "raft".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"value".to_vec())
            }
        );
    }

    #[test]
    fn durable_index_survives_restart_and_points_to_page_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache-a");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"persisted".to_vec(),
            },
        });

        let restarted = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache-b"),
            &page_dir,
            &index_dir,
        );
        restarted.load_shard(1);
        let response = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"persisted".to_vec())
            }
        );
        assert_eq!(restarted.page_store().stats().reads, 1);
    }

    #[test]
    fn hash_incrby_rejects_non_integer_and_overflow_like_cpp() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashMultiSet {
                key: "h".to_string(),
                entries: vec![
                    ("alpha".to_string(), b"abc".to_vec()),
                    ("mixed".to_string(), b"123abc".to_vec()),
                    ("max".to_string(), i64::MAX.to_string().into_bytes()),
                    ("min".to_string(), i64::MIN.to_string().into_bytes()),
                ],
            },
        });

        for field in ["alpha", "mixed"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashIncrBy {
                    key: "h".to_string(),
                    field: field.to_string(),
                    increment: 1,
                },
            });
            assert_eq!(response.status.code, "unmatched");
            assert_eq!(response.response, CommandResponse::Empty);
        }

        let overflow = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: "max".to_string(),
                increment: 1,
            },
        });
        assert_eq!(overflow.status.code, "out_of_range");
        let underflow = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashIncrBy {
                key: "h".to_string(),
                field: "min".to_string(),
                increment: -1,
            },
        });
        assert_eq!(underflow.status.code, "out_of_range");
    }

    #[test]
    fn feature_append_packs_many_timestamp_values_into_one_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let first = SequenceFeatureRow {
            timestamp_ms: 10,
            gid: 1,
            action_type: 2,
            duration: 3,
            author_id: 4,
        };
        let second = SequenceFeatureRow {
            timestamp_ms: 20,
            gid: 5,
            action_type: 6,
            duration: 7,
            author_id: 8,
        };
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "packed-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: second.timestamp_ms,
                        value: second.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: first.timestamp_ms,
                        value: first.encode_cpp_feature_value(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let (first_address, second_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("packed-feature"))
                .expect("feature series should exist");
            (
                series.get(&10).expect("first point").clone(),
                series.get(&20).expect("second point").clone(),
            )
        };
        assert_eq!(first_address, second_address);
        assert_eq!(
            first_address.object_id,
            Some(stable_page_object_id(1, "feature", "packed-feature", None))
        );
        let packed_bytes = engine.page_store().read(&first_address).unwrap();
        let packed_points = decode_feature_page(&packed_bytes).expect("packed feature page");
        assert_eq!(packed_points.len(), 2);
        assert_eq!(packed_points[0].timestamp_ms, 10);
        assert_eq!(packed_points[1].timestamp_ms, 20);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: first.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: second.encode_cpp_feature_value(),
                    },
                ]
            }
        );

        let filtered = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
                filters: vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 5,
                }],
            },
        });
        assert_eq!(
            filtered.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 20,
                    value: second.encode_cpp_feature_value(),
                }]
            }
        );

        let agg = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAggQuery {
                key: "packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                aggregator: "count".to_string(),
                count: None,
            },
        });
        assert_eq!(agg.response, CommandResponse::Aggregate { value: 2 });
    }

    #[test]
    fn feature_append_chunks_and_persists_timestamped_kv_pages() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let points = (0..10)
            .map(|offset| FeaturePoint {
                timestamp_ms: 1_000 + offset,
                value: vec![b'a' + offset as u8; 10 * 1024],
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "chunked-feature".to_string(),
                points: points.clone(),
            },
        });
        assert!(response.status.ok);

        let addresses = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("chunked-feature"))
                .expect("feature series should exist");
            unique_timestamped_kv_page_addresses(series)
        };
        assert!(
            addresses.len() > 1,
            "large timestamped KV batch should be split into page chunks"
        );
        let mut persisted_timestamps = Vec::new();
        for address in &addresses {
            assert_eq!(
                address.object_id,
                Some(stable_page_object_id(1, "feature", "chunked-feature", None))
            );
            let bytes = engine.page_store().read(address).unwrap();
            let chunk = decode_feature_page(&bytes).expect("persisted packed page chunk");
            assert!(!chunk.is_empty());
            assert!(bytes.len() <= TIMESTAMPED_KV_PAGE_TARGET_BYTES + 12 * 1024);
            persisted_timestamps.extend(chunk.into_iter().map(|point| point.timestamp_ms));
        }
        persisted_timestamps.sort_unstable();
        assert_eq!(
            persisted_timestamps,
            points
                .iter()
                .map(|point| point.timestamp_ms)
                .collect::<Vec<_>>()
        );

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "chunked-feature".to_string(),
                start_ms: 0,
                end_ms: 2_000,
                count: None,
            },
        });
        assert_eq!(query.response, CommandResponse::FeaturePoints { points });
    }

    #[test]
    fn feature_append_keeps_oversized_single_timestamped_value_readable() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let points = vec![FeaturePoint {
            timestamp_ms: 1_000,
            value: vec![b'x'; TIMESTAMPED_KV_PAGE_TARGET_BYTES + 8 * 1024],
        }];
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "oversized-single-feature".to_string(),
                points: points.clone(),
            },
        });
        assert!(response.status.ok);

        let addresses = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("oversized-single-feature"))
                .expect("feature series should exist");
            unique_timestamped_kv_page_addresses(series)
        };
        assert_eq!(addresses.len(), 1);
        let bytes = engine.page_store().read(&addresses[0]).unwrap();
        assert!(bytes.len() > TIMESTAMPED_KV_PAGE_TARGET_BYTES);
        assert_eq!(decode_feature_page(&bytes).unwrap(), points);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "oversized-single-feature".to_string(),
                start_ms: 0,
                end_ms: 2_000,
                count: None,
            },
        });
        assert_eq!(query.response, CommandResponse::FeaturePoints { points });
        assert!(
            engine
                .storage_production_readiness_report(1)
                .production_ready
        );
    }

    #[test]
    fn feature_recovery_validates_packed_page_layout() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let report = engine.storage_recovery_report(1);
        assert_eq!(report.feature_page_layout.indexed_feature_points, 2);
        assert_eq!(report.feature_page_layout.unique_feature_page_refs, 1);
        assert_eq!(report.feature_page_layout.packed_feature_pages, 1);
        assert_eq!(report.feature_page_layout.legacy_feature_value_pages, 0);
        assert!(report
            .feature_page_layout
            .corrupt_packed_feature_pages
            .is_empty());
        assert!(report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty());
    }

    #[test]
    fn feature_recovery_reports_index_timestamp_missing_from_packed_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let series = shards
                .get_mut(&1)
                .and_then(|shard| shard.features.get_mut("layout-feature"))
                .expect("feature series should exist");
            let address = series.get(&10).expect("packed page").clone();
            series.insert(30, address);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .missing_indexed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![30]
        );
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_packed_timestamp_orphaned_from_index() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "layout-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let series = shards
                .get_mut(&1)
                .and_then(|shard| shard.features.get_mut("layout-feature"))
                .expect("feature series should exist");
            series.remove(&20);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .orphan_packed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![20]
        );
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_duplicate_timestamps_inside_packed_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let duplicate_page = encode_feature_page(&[
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 10,
                value: b"ten-duplicate".to_vec(),
            },
            FeaturePoint {
                timestamp_ms: 20,
                value: b"twenty".to_vec(),
            },
        ]);
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &duplicate_page,
                Some(stable_page_object_id(1, "feature", "layout-feature", None)),
                Some(page_routing_slot("layout-feature", 0, u32::MAX)),
            )
            .expect("duplicate packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            let series = shard
                .features
                .entry("layout-feature".to_string())
                .or_default();
            series.insert(10, address.clone());
            series.insert(20, address);
        }

        let report = engine.storage_recovery_report(1);
        assert_eq!(
            report
                .feature_page_layout
                .duplicate_packed_timestamps
                .iter()
                .map(|mismatch| mismatch.timestamp_ms)
                .collect::<Vec<_>>(),
            vec![10]
        );
        assert!(report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty());
        let readiness = engine.storage_production_readiness_report(1);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.feature_page_layout_mismatch_count, 1);
    }

    #[test]
    fn feature_recovery_reports_corrupt_packed_timestamped_page() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut corrupt_page = FEATURE_PAGE_MAGIC.to_vec();
        corrupt_page.extend_from_slice(br#"{"version":1,"points":"not-a-point-list"}"#);
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &corrupt_page,
                Some(stable_page_object_id(1, "feature", "corrupt-feature", None)),
                Some(page_routing_slot("corrupt-feature", 0, u32::MAX)),
            )
            .expect("corrupt packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            shard
                .features
                .entry("corrupt-feature".to_string())
                .or_default()
                .insert(10, address);
        }

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "corrupt-feature".to_string(),
                start_ms: 0,
                end_ms: 20,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints { points: vec![] }
        );

        let readiness = engine.storage_production_readiness_report(1);
        assert!(!readiness.production_ready);
        assert!(readiness
            .blockers
            .contains(&"feature_page_layout_mismatch".to_string()));
        assert_eq!(readiness.corrupt_feature_page_count, 1);
        assert!(
            readiness.feature_page_layout.corrupt_packed_feature_pages[0]
                .error
                .contains("invalid packed feature page payload")
        );
    }

    #[test]
    fn feature_recovery_reports_unsupported_packed_timestamped_page_version() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let page = PackedFeaturePage {
            version: 2,
            points: vec![FeaturePoint {
                timestamp_ms: 10,
                value: b"ten".to_vec(),
            }],
        };
        let mut bytes = FEATURE_PAGE_MAGIC.to_vec();
        bytes.extend_from_slice(&serde_json::to_vec(&page).unwrap());
        let address = engine
            .page_store()
            .append_with_page_metadata(
                &bytes,
                Some(stable_page_object_id(
                    1,
                    "feature",
                    "versioned-feature",
                    None,
                )),
                Some(page_routing_slot("versioned-feature", 0, u32::MAX)),
            )
            .expect("unsupported packed page append");

        {
            let mut shards = engine.shards.write().expect("engine lock poisoned");
            let shard = shards.get_mut(&1).expect("loaded shard");
            shard
                .features
                .entry("versioned-feature".to_string())
                .or_default()
                .insert(10, address);
        }

        let readiness = engine.storage_production_readiness_report(1);
        assert!(!readiness.production_ready);
        assert_eq!(readiness.corrupt_feature_page_count, 1);
        assert!(
            readiness.feature_page_layout.corrupt_packed_feature_pages[0]
                .error
                .contains("unsupported packed feature page version 2")
        );
    }

    #[test]
    fn feature_compaction_rewrites_shared_packed_page_once() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "compact-packed-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ],
            },
        });
        assert!(response.status.ok);

        let before = engine.storage_recovery_report(1);
        assert_eq!(before.total_page_refs, 1);
        let report = engine.compact_shard_pages(1).unwrap();
        assert_eq!(report.rewritten_page_refs, 1);
        assert_eq!(report.after.live_page_refs, 1);

        let (first_address, second_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let series = shards
                .get(&1)
                .and_then(|shard| shard.features.get("compact-packed-feature"))
                .expect("feature series should exist");
            (
                series.get(&10).expect("first point").clone(),
                series.get(&20).expect("second point").clone(),
            )
        };
        assert_eq!(first_address, second_address);

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "compact-packed-feature".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"ten".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"twenty".to_vec(),
                    },
                ]
            }
        );
        let after = engine.storage_recovery_report(1);
        assert_eq!(after.total_page_refs, 1);
        assert_eq!(after.object_lifecycle.live_page_refs, 1);
        assert_eq!(after.object_lifecycle.reused_object_id_conflicts, 0);
    }

    #[test]
    fn feature_append_rejects_cpp_hard_size_limit_before_mutation() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "huge-feature".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"kept".to_vec(),
                }],
            },
        });

        let oversized_points = (0..FEATURE_ADD_HARD_MAX_SIZE)
            .map(|offset| FeaturePoint {
                timestamp_ms: 10 + offset as u64,
                value: b"x".to_vec(),
            })
            .collect::<Vec<_>>();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "huge-feature".to_string(),
                points: oversized_points,
            },
        });
        assert_eq!(response.status.ok, false);
        assert_eq!(response.status.code, "invalid_argument");
        assert!(response
            .status
            .message
            .contains("huge-feature size bigger than 100000"));

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "huge-feature".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(10),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"kept".to_vec(),
                }]
            }
        );
    }

    // shared-corpus: feature_nested_proto_aggregate_semantics
    #[test]
    fn feature_query_filtered_matches_cpp_protobuf_feature_point() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let matching = SequenceFeatureRow {
            timestamp_ms: 777,
            gid: 1,
            action_type: 2,
            duration: 3,
            author_id: 1,
        };
        let other = SequenceFeatureRow {
            timestamp_ms: 778,
            gid: 2,
            action_type: 2,
            duration: 5,
            author_id: 9,
        };
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "9".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: matching.timestamp_ms,
                        value: matching.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: other.timestamp_ms,
                        value: other.encode_cpp_feature_value(),
                    },
                    FeaturePoint {
                        timestamp_ms: 779,
                        value: b"not-protobuf".to_vec(),
                    },
                ],
            },
        });

        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters: vec![FeatureFilter {
                    field: "gid".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 1,
                }],
            },
        });

        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);
        assert_eq!(
            SequenceFeatureRow::decode_cpp_feature_value(points[0].timestamp_ms, &points[0].value),
            Some(matching)
        );

        let filters = parse_cpp_feature_filters(["gid = 1", "duration < 4"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);

        let filters = parse_cpp_feature_filters(["gid >= 1", "duration <= 3"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 777);

        let filters = parse_cpp_feature_filters(["gid = 1", "gid != 1"]).unwrap();
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQueryFiltered {
                key: "9".to_string(),
                start_ms: 0,
                end_ms: 100_000,
                count: Some(1_000),
                filters,
            },
        });
        let CommandResponse::FeaturePoints { points } = response.response else {
            panic!("expected feature points");
        };
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].timestamp_ms, 778);

        assert!(FeatureFilter::parse_cpp_filter("unknown = 1").is_err());
        assert!(FeatureFilter::parse_cpp_filter("gid = nope").is_err());
    }

    #[test]
    fn cpp_feature_sequence_golden_corpus_passes() {
        let report = cpp_feature_sequence_golden_corpus_report();
        assert_eq!(report.corpus, "feature_sequence_cpp_proto_v1");
        assert_eq!(report.total_cases, 8);
        assert_eq!(report.passed_cases, report.total_cases);
        assert_eq!(report.failed_cases, 0);
        assert!(report.passed(), "{report:#?}");
    }

    #[test]
    fn cpp_api_golden_corpus_passes() {
        let report = cpp_api_golden_corpus_report();
        assert_eq!(report.corpus, "cpp_api_golden_corpus_v1");
        assert_eq!(report.total_cases, 16);
        assert!(report.passed(), "{report:#?}");
        assert_eq!(report.passed_cases, report.total_cases);
        assert_eq!(report.failed_cases, 0);
    }

    #[test]
    fn feature_replace_delete_and_agg_query() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "f".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"2".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"3".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"4".to_vec(),
                    },
                ],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "sum".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 9 }
        );
        for (aggregator, count, expected) in [
            ("avg", None, 3),
            ("first", None, 2),
            ("last", None, 4),
            ("events", None, 3),
            ("last", Some(2), 3),
        ] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::FeatureAggQuery {
                            key: "f".to_string(),
                            start_ms: 0,
                            end_ms: 40,
                            aggregator: aggregator.to_string(),
                            count,
                        },
                    })
                    .response,
                CommandResponse::Aggregate { value: expected },
                "{aggregator} aggregate should match C++ window semantics"
            );
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 100,
                        end_ms: 200,
                        aggregator: "avg".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 0 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureReplace {
                key: "f".to_string(),
                start_ms: 0,
                end_ms: 20,
                points: vec![FeaturePoint {
                    timestamp_ms: 15,
                    value: b"10".to_vec(),
                }],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "sum".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 14 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureDelete {
                key: "f".to_string(),
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAggQuery {
                        key: "f".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "count".to_string(),
                        count: None,
                    },
                })
                .response,
            CommandResponse::Aggregate { value: 0 }
        );
    }

    #[test]
    fn common_delete_removes_all_data_types_for_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "k".to_string(),
                member: b"m".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonDelete {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SetMembers {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Members {
                members: Vec::new()
            }
        );
    }

    #[test]
    fn common_delete_removes_cpp_risk_family_records_for_logical_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (family, amount) in [
            (RiskFamily::H, 5),
            (RiskFamily::Cpc, 7),
            (RiskFamily::Fol, 11),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskSet {
                    family,
                    key: "risk-cpp".to_string(),
                    timestamp_ms: 10,
                    amount,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExists {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::Empty
        );
        for family in [RiskFamily::H, RiskFamily::Cpc, RiskFamily::Fol] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFamilyQuery {
                            family,
                            key: "risk-cpp".to_string(),
                            start_ms: 0,
                            end_ms: 20,
                            aggregator: "sum".to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: 0 }
            );
        }
    }

    #[test]
    fn common_expire_and_ttl_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "k".to_string(),
                ttl_ms: 0,
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Integer { value: -2 }
        );
    }

    #[test]
    fn common_expire_missing_key_matches_cpp_not_found() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "missing".to_string(),
                ttl_ms: 1000,
            },
        });
        assert_eq!(response.status.code, "not_found");
    }

    #[test]
    fn common_expire_and_ttl_cover_cpp_risk_family_records_for_logical_key() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskSet {
                family: RiskFamily::Cpc,
                key: "risk-expire".to_string(),
                timestamp_ms: 10,
                amount: 3,
            },
        });
        assert!(response.status.ok, "{response:?}");

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-expire".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: -1 }
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonExpire {
                        key: "risk-expire".to_string(),
                        ttl_ms: 0,
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-expire".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: -2 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::Cpc,
                        key: "risk-expire".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
    }

    // shared-corpus: sequence_cpp_feature_rows sequence_batch_filter_groups
    #[test]
    fn sequence_query_filters_typed_rows() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: "seq".to_string(),
                rows: vec![
                    SequenceFeatureRow {
                        timestamp_ms: 1,
                        gid: 10,
                        action_type: 1,
                        duration: 30,
                        author_id: 7,
                    },
                    SequenceFeatureRow {
                        timestamp_ms: 2,
                        gid: 11,
                        action_type: 3,
                        duration: 120,
                        author_id: 8,
                    },
                ],
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceQuery {
                key: "seq".to_string(),
                start_ms: 0,
                end_ms: 10,
                count: 10,
                filters: vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::SequenceRows {
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 2,
                    gid: 11,
                    action_type: 3,
                    duration: 120,
                    author_id: 8,
                }]
            }
        );
    }

    #[test]
    fn long_sequence_query_keeps_timestamp_order_and_applies_random_filters() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let base_ts = 1_700_000_000_000_u64;
        let row_count = 5_000_u64;
        let key = "long-sequence".to_string();

        let ordered_rows = (0..row_count)
            .map(|offset| SequenceFeatureRow {
                timestamp_ms: base_ts + offset,
                gid: 10_000 + offset,
                action_type: (offset % 7) as u32,
                duration: (50 + (offset * 37) % 1_000) as u32,
                author_id: 500 + (offset * 17) % 97,
            })
            .collect::<Vec<_>>();
        let shuffled_rows = (0..row_count)
            .map(|i| ordered_rows[((i * 2_919) % row_count) as usize].clone())
            .collect::<Vec<_>>();

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SequenceAdd {
                key: key.clone(),
                rows: shuffled_rows,
            },
        });

        for seed in 0..20_u64 {
            let start_offset = (seed * 313) % 4_400;
            let end_offset = (start_offset + 250 + (seed * 97) % 700).min(row_count - 1);
            let count = 25 + (seed as usize % 40);
            let filters = vec![
                FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::NotEqual,
                    value: seed % 7,
                },
                FeatureFilter {
                    field: "duration".to_string(),
                    op: FeatureFilterOp::GreaterOrEqual,
                    value: 100 + (seed * 29) % 500,
                },
                FeatureFilter {
                    field: "author_id".to_string(),
                    op: FeatureFilterOp::LessOrEqual,
                    value: 560 + (seed * 11) % 30,
                },
            ];

            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceQuery {
                    key: key.clone(),
                    start_ms: base_ts + start_offset,
                    end_ms: base_ts + end_offset,
                    count,
                    filters: filters.clone(),
                },
            });
            let CommandResponse::SequenceRows { rows } = response.response else {
                panic!("expected sequence rows");
            };
            let expected = ordered_rows
                .iter()
                .filter(|row| row.timestamp_ms >= base_ts + start_offset)
                .filter(|row| row.timestamp_ms <= base_ts + end_offset)
                .take(count)
                .filter(|row| {
                    filters
                        .iter()
                        .all(|filter| sequence_filter_matches(row, filter))
                })
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(rows, expected, "seed {seed}");
            assert!(rows
                .windows(2)
                .all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
            assert!(rows.len() <= count);
        }
    }

    #[test]
    fn ips_query_last_returns_recent_instances() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, value) in [(1, b"a".to_vec()), (2, b"b".to_vec()), (3, b"c".to_vec())] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: "ips".to_string(),
                    timestamp_ms,
                    instance: value,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryLast {
                        key: "ips".to_string(),
                        count: 2,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 3,
                        value: b"c".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"b".to_vec(),
                    }
                ]
            }
        );
    }

    #[test]
    fn risk_count_sums_window() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 1), (20, 2), (30, 4)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskCount {
                        key: "risk".to_string(),
                        start_ms: 15,
                        end_ms: 30,
                    },
                })
                .response,
            CommandResponse::Integer { value: 6 }
        );
    }

    #[test]
    fn control_api_load_config_info_stats_membership_and_unload() {
        let engine = TemporalEngine::default();
        assert_eq!(
            engine.set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    feature_max_size: 123,
                    ..Config::default()
                },
            }),
            Status::error("shard_not_found", "shard is not loaded")
        );
        assert_eq!(engine.get_config(7).status.code, "shard_not_found");
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 7,
                    load_version: 42,
                    local_node_id: Some(2),
                    shard_uri: "file:///tmp/shard-7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );
        let duplicate_load = engine.load_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 43,
            local_node_id: Some(2),
            shard_uri: "file:///tmp/shard-7-duplicate".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 20,
            readonly: false,
            table_name: "table".to_string(),
        });
        assert!(!duplicate_load.status.ok);
        assert_eq!(duplicate_load.status.code, "already_exists");
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 7,
                    config: Config {
                        version: 2,
                        feature_max_size: 123,
                        maxmemory_bytes: Some(3000),
                        extend_config: BTreeMap::from([(
                            "test_config".to_string(),
                            "test_value".to_string(),
                        )]),
                        ..Config::default()
                    },
                })
                .ok
        );
        let config = engine.get_config(7).config;
        assert_eq!(config.feature_max_size, 123);
        assert_eq!(config.maxmemory_bytes, Some(3000));
        assert_eq!(
            config.extend_config.get("test_config"),
            Some(&"test_value".to_string())
        );
        assert_eq!(
            engine.set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 1,
                    feature_max_size: 456,
                    ..Config::default()
                },
            }),
            Status::error("failed_precondition", "legacy config version")
        );
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 7,
                    config: Config {
                        version: 2,
                        feature_max_size: 456,
                        ..Config::default()
                    },
                })
                .ok
        );
        assert_eq!(engine.get_config(7).config.feature_max_size, 123);
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 3,
                    replica_membership_version: 4,
                    replica_node_ids: vec![1, 2, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.load_version, 42);
        assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
        assert_eq!(info.membership_version, 3);
        assert_eq!(info.replica_membership_version, 4);
        assert!(info.membership_valid);
        assert_eq!(
            engine.update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 2,
                replica_membership_version: 5,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            }),
            Status::error("failed_precondition", "legacy membership info")
        );
        assert_eq!(
            engine.update_membership(MembershipUpdateRequest {
                shard_id: 7,
                membership_version: 3,
                replica_membership_version: 3,
                replica_node_ids: vec![1, 3],
                leader_node_id: Some(1),
            }),
            Status::error("failed_precondition", "legacy membership unit info")
        );
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 4,
                    replica_membership_version: 5,
                    replica_node_ids: vec![1, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.replica_node_ids, vec![1, 3]);
        assert!(!info.membership_valid);

        engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let stats = engine.get_stats(7).stats.unwrap();
        assert_eq!(stats.string_records, 1);
        assert_eq!(stats.total_records, 1);
        assert_eq!(stats.load_version, 42);
        assert!(!stats.readonly);
        assert!(stats.storage_bytes > 0);
        assert_eq!(stats.page_store.writes, 1);

        assert!(
            engine
                .unload_shard_with(UnloadShardRequest { shard_id: 7 })
                .status
                .ok
        );
        let after_unload = engine.get_info(7);
        assert!(!after_unload.status.ok);
        assert_eq!(after_unload.status.code, "shard_not_found");
        assert_eq!(engine.get_config(7).status.code, "shard_not_found");
        let second_unload = engine.unload_shard_with(UnloadShardRequest { shard_id: 7 });
        assert!(!second_unload.status.ok);
        assert_eq!(second_unload.status.code, "shard_not_found");
    }

    #[test]
    fn engine_reload_shard_updates_metadata_and_rejects_stale_version() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 7,
                    load_version: 42,
                    local_node_id: Some(2),
                    shard_uri: "file:///tmp/shard-7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "old_table".to_string(),
                })
                .status
                .ok
        );
        assert!(
            engine
                .update_membership(MembershipUpdateRequest {
                    shard_id: 7,
                    membership_version: 3,
                    replica_membership_version: 4,
                    replica_node_ids: vec![1, 2, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );

        let stale = engine.reload_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 41,
            local_node_id: Some(9),
            shard_uri: "file:///tmp/stale".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 200,
            readonly: true,
            table_name: "stale_table".to_string(),
        });
        assert!(!stale.status.ok);
        assert_eq!(stale.status.code, "stale_load_version");
        let unchanged = engine.get_info(7).info.unwrap();
        assert_eq!(unchanged.load_version, 42);
        assert_eq!(unchanged.table_name, "old_table");
        assert!(!unchanged.readonly);

        let reload = engine.reload_shard_with(LoadShardRequest {
            shard_id: 7,
            load_version: 43,
            local_node_id: Some(9),
            shard_uri: "file:///tmp/shard-7-reloaded".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 200,
            readonly: true,
            table_name: "new_table".to_string(),
        });
        assert!(reload.status.ok, "{reload:?}");
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.load_version, 43);
        assert_eq!(info.local_node_id, Some(9));
        assert_eq!(info.table_name, "new_table");
        assert_eq!(info.start_routing_slot, 100);
        assert_eq!(info.end_routing_slot, 200);
        assert!(info.readonly);
        assert_eq!(info.replica_node_ids, vec![1, 2, 3]);
        assert_eq!(info.membership_version, 3);
        assert_eq!(info.replica_membership_version, 4);
        assert!(info.membership_valid);

        let write = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(write.status.code, "readonly_shard");
    }

    #[test]
    fn control_api_reads_page_and_index_streams() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"stream-value".to_vec(),
            },
        });

        let page = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            offset: 0,
            size: 12,
        });
        assert_eq!(page.data, b"stream-value".to_vec());

        let index = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Index,
            page_segment_id: 0,
            offset: 0,
            size: 32,
        });
        assert!(index.status.ok);
        assert!(!index.data.is_empty());

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 12,
            max_bytes: 12,
        });
        assert_eq!(scan.records.len(), 1);
        assert_eq!(scan.records[0].data, b"stream-value".to_vec());

        let invalid = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Page,
            page_segment_id: 0,
            start_offset: 12,
            end_offset: 1,
            max_bytes: 12,
        });
        assert_eq!(invalid.status.code, "invalid_stream_range");
        assert!(invalid.records.is_empty());
    }

    #[test]
    fn control_api_reads_and_scans_oplog_stream() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
        });

        let stream = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::Oplog,
            page_segment_id: 0,
            offset: 0,
            size: 4096,
        });
        assert!(stream.status.ok);
        let text = String::from_utf8(stream.data).unwrap();
        assert!(text.contains("\"sequence\":1"));
        assert!(text.contains("\"sequence\":2"));

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::Oplog,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 4096,
            max_bytes: 4096,
        });
        assert_eq!(scan.records.len(), 2);
        assert_eq!(engine.get_stats(1).stats.unwrap().oplog.last_sequence, 2);
    }

    #[test]
    fn control_api_reads_and_scans_index_log_stream() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"hv".to_vec(),
            },
        });

        let stream = engine.read_stream(StreamReadRequest {
            shard_id: 1,
            stream_kind: StreamKind::IndexLog,
            page_segment_id: 0,
            offset: 0,
            size: 8192,
        });
        assert!(stream.status.ok);
        let text = String::from_utf8(stream.data).unwrap();
        assert!(text.contains("\"sequence\":1"));
        assert!(text.contains("\"sequence\":2"));
        assert!(text.contains("\"strings\""));
        assert!(text.contains("\"hashes\""));

        let scan = engine.scan_stream(ScanStreamRequest {
            shard_id: 1,
            stream_kind: StreamKind::IndexLog,
            page_segment_id: 0,
            start_offset: 0,
            end_offset: 8192,
            max_bytes: 8192,
        });
        assert_eq!(scan.records.len(), 2);

        let last_record: crate::index_log::IndexLogRecord =
            serde_json::from_slice(&scan.records[1].data).unwrap();
        assert_eq!(last_record.sequence, 2);
        assert_eq!(
            last_record.index["hashes"]["h"]["f"]["page_segment_id"],
            serde_json::json!(0)
        );
        assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
    }

    #[test]
    fn readonly_shard_rejects_writes_but_allows_reads() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    load_version: 1,
                    local_node_id: None,
                    shard_uri: "file:///tmp/readonly".to_string(),
                    start_routing_slot: 0,
                    end_routing_slot: 99,
                    readonly: true,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );

        let write = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(!write.status.ok);
        assert_eq!(write.status.code, "readonly_shard");

        let read = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert!(read.status.ok);
        assert_eq!(read.response, CommandResponse::Bytes { value: None });
    }

    #[test]
    fn checked_execute_rejects_stale_load_version() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 1,
                    load_version: 7,
                    local_node_id: None,
                    shard_uri: "file:///tmp/versioned".to_string(),
                    start_routing_slot: 0,
                    end_routing_slot: 99,
                    readonly: false,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );

        let stale = engine.execute_checked(CheckedExecuteRequest {
            shard_id: 1,
            load_version: 6,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert_eq!(stale.status.code, "load_version_mismatch");

        let current = engine.execute_checked(CheckedExecuteRequest {
            shard_id: 1,
            load_version: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(current.status.ok);
    }

    #[test]
    fn loaded_shard_stats_reports_per_shard_load() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.load_shard(2);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "a".to_string(),
                value: b"1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::HashSet {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"2".to_vec(),
            },
        });

        let stats = engine.loaded_shard_stats();
        assert_eq!(stats.len(), 2);
        assert!(stats
            .iter()
            .any(|stat| stat.shard_id == 1 && stat.string_records == 1));
        assert!(stats
            .iter()
            .any(|stat| stat.shard_id == 2 && stat.hash_records == 1));
    }

    #[test]
    fn string_set_conditional_supports_nx_xx_and_get() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        let first = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v1".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfNotExists,
                return_old: false,
            },
        });
        assert_eq!(first.response, CommandResponse::Integer { value: 1 });

        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v2".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfNotExists,
                return_old: false,
            },
        });
        assert_eq!(rejected.response, CommandResponse::Integer { value: 0 });

        let old = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSetConditional {
                key: "k".to_string(),
                value: b"v3".to_vec(),
                ttl_ms: None,
                condition: StringSetCondition::IfExists,
                return_old: true,
            },
        });
        assert_eq!(
            old.response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"v3".to_vec())
            }
        );
    }

    #[test]
    fn ips_remove_delete_and_count_are_supported() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for timestamp_ms in [10, 20, 30] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: "ips".to_string(),
                    timestamp_ms,
                    instance: timestamp_ms.to_string().into_bytes(),
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsCount {
                        key: "ips".to_string(),
                        start_ms: 0,
                        end_ms: 25,
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsRemove {
                        key: "ips".to_string(),
                        timestamp_ms: 20,
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsDelete {
                        key: "ips".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
    }

    // shared-corpus: ips_options_range ips_snapshot_stat_filter_batch
    #[test]
    fn ips_range_and_batch_queries_match_cpp_style_read_shapes() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (key, timestamp_ms) in [
            ("ips-a", 10),
            ("ips-a", 20),
            ("ips-a", 30),
            ("ips-b", 15),
            ("ips-b", 25),
        ] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAdd {
                    key: key.to_string(),
                    timestamp_ms,
                    instance: format!("{key}-{timestamp_ms}").into_bytes(),
                },
            });
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryRange {
                        key: "ips-a".to_string(),
                        start_ms: 15,
                        end_ms: 35,
                        count: Some(1),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 20,
                    value: b"ips-a-20".to_vec(),
                }]
            }
        );

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsBatchQueryLast {
                        keys: vec!["ips-a".to_string(), "ips-b".to_string()],
                        count: 1,
                    },
                })
                .response,
            CommandResponse::FeaturePointGroups {
                groups: vec![
                    (
                        "ips-a".to_string(),
                        vec![FeaturePoint {
                            timestamp_ms: 30,
                            value: b"ips-a-30".to_vec(),
                        }],
                    ),
                    (
                        "ips-b".to_string(),
                        vec![FeaturePoint {
                            timestamp_ms: 25,
                            value: b"ips-b-25".to_vec(),
                        }],
                    ),
                ],
            }
        );
    }

    #[test]
    fn ips_pages_store_timestamp_keys_with_values() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsLoad {
                        key: "packed-ips".to_string(),
                        points: vec![
                            FeaturePoint {
                                timestamp_ms: 10,
                                value: b"ten".to_vec(),
                            },
                            FeaturePoint {
                                timestamp_ms: 20,
                                value: b"twenty".to_vec(),
                            },
                        ],
                    },
                })
                .status
                .ok
        );

        let (first_address, second_address, meta_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let series = shard.ips.get("packed-ips").expect("IPS series");
            let meta = shard.ips_meta.get("packed-ips").expect("IPS metadata");
            (
                series.get(&10).expect("first IPS point").clone(),
                series.get(&20).expect("second IPS point").clone(),
                meta.get(&20).expect("second IPS metadata").address.clone(),
            )
        };
        assert_eq!(first_address, second_address);
        assert_eq!(second_address, meta_address);
        assert_eq!(
            first_address.object_id,
            Some(stable_page_object_id(1, "ips", "packed-ips", None))
        );

        let bytes = engine.page_store().read(&first_address).unwrap();
        let packed_points = decode_feature_page(&bytes).expect("packed IPS page");
        assert_eq!(
            packed_points,
            vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ]
        );

        let query = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsQueryRange {
                key: "packed-ips".to_string(),
                start_ms: 0,
                end_ms: 30,
                count: None,
            },
        });
        assert_eq!(
            query.response,
            CommandResponse::FeaturePoints {
                points: packed_points
            }
        );
    }

    #[test]
    fn recovery_validates_all_timestamped_kv_page_families() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            8 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);

        let feature_points = (0..8)
            .map(|idx| FeaturePoint {
                timestamp_ms: 1_000 + idx,
                value: vec![b'f'; 10 * 1024],
            })
            .collect::<Vec<_>>();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppend {
                        key: "all-family-feature".to_string(),
                        points: feature_points.clone(),
                    },
                })
                .status
                .ok
        );

        let sequence_rows = (0..8)
            .map(|idx| SequenceFeatureRow {
                timestamp_ms: 2_000 + idx,
                gid: idx,
                action_type: 7,
                duration: 11,
                author_id: 13,
            })
            .collect::<Vec<_>>();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SequenceAdd {
                        key: "all-family-sequence".to_string(),
                        rows: sequence_rows.clone(),
                    },
                })
                .status
                .ok
        );

        let ips_points = (0..8)
            .map(|idx| FeaturePoint {
                timestamp_ms: 3_000 + idx,
                value: vec![b'i'; 10 * 1024],
            })
            .collect::<Vec<_>>();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsLoad {
                        key: "all-family-ips".to_string(),
                        points: ips_points.clone(),
                    },
                })
                .status
                .ok
        );

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextWriteEvent {
                        tenant_hash: 44,
                        node_hash: 55,
                        event: ContextEvent {
                            event_id_hash: 66,
                            event_time_ms: 4_000,
                            kind: 1,
                            event_type: 2,
                            actor_hash: 77,
                            status: 1,
                            valid_until_ms: 0,
                            confidence: 0.99,
                            importance: 0.75,
                            text: "context event".to_string(),
                            source_ref: "local://test".to_string(),
                            related_node_hashes: vec![55],
                            compact_attrs: vec![1, 2, 3],
                        },
                        first_write_only: false,
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextWriteIndexRef {
                        tenant_hash: 44,
                        index_name: "actor".to_string(),
                        index_value_hash: 77,
                        scope_hash: 1,
                        event_time_ms: 4_000,
                        index_ref: ContextIndexRef {
                            primary_node_hash: 55,
                            primary_event_time_ms: 4_000,
                            event_id_hash: 66,
                        },
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextWritePackAudit {
                        tenant_hash: 44,
                        audit: ContextPackAudit {
                            query_id: "q-all-family".to_string(),
                            session_hash: 88,
                            request_time_ms: 4_100,
                            query_hash: 99,
                            max_prompt_tokens: 128,
                            selected_tokens: 32,
                            selected_refs: vec![ContextAuditRef {
                                node_hash: 55,
                                event_time_ms: 4_000,
                                reason: "selected".to_string(),
                            }],
                            blocked_refs: Vec::new(),
                        },
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextMarkSummaryDirty {
                        tenant_hash: 44,
                        marker: ContextSummaryDirtyMarker {
                            node_hash: 55,
                            event_time_ms: 4_200,
                            reason: 9,
                            propagate_depth: 2,
                        },
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_recovery_report(1);
        assert_eq!(report.feature_page_layout.indexed_timestamped_points, 28);
        assert!(report.feature_page_layout.packed_timestamped_pages >= 10);
        assert!(
            report
                .feature_page_layout
                .unique_timestamped_page_refs
                .saturating_sub(report.feature_page_layout.packed_timestamped_pages)
                <= report.feature_page_layout.legacy_timestamped_value_pages
        );
        assert!(report
            .feature_page_layout
            .corrupt_packed_feature_pages
            .is_empty());
        assert!(report
            .feature_page_layout
            .missing_indexed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .orphan_packed_timestamps
            .is_empty());
        assert!(report
            .feature_page_layout
            .duplicate_packed_timestamps
            .is_empty());

        let families = report
            .feature_page_layout
            .families
            .iter()
            .map(|family| (family.kind.as_str(), family))
            .collect::<BTreeMap<_, _>>();
        for kind in [
            "feature",
            "sequence",
            "ips",
            "context_event",
            "context_index",
            "context_audit",
            "context_dirty",
        ] {
            let family = families.get(kind).expect("timestamped family report");
            assert!(family.indexed_points > 0, "{kind}");
            assert!(family.packed_pages > 0, "{kind}");
            assert_eq!(family.corrupt_pages, 0, "{kind}");
            assert_eq!(family.mismatch_count, 0, "{kind}");
        }
        assert!(
            families
                .get("feature")
                .expect("feature family")
                .unique_page_refs
                > 1
        );
        assert!(families.get("ips").expect("ips family").unique_page_refs > 1);

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureQuery {
                        key: "all-family-feature".to_string(),
                        start_ms: 1_000,
                        end_ms: 1_010,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: feature_points
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SequenceQuery {
                        key: "all-family-sequence".to_string(),
                        start_ms: 2_000,
                        end_ms: 2_010,
                        count: 16,
                        filters: Vec::new(),
                    },
                })
                .response,
            CommandResponse::SequenceRows {
                rows: sequence_rows
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryRange {
                        key: "all-family-ips".to_string(),
                        start_ms: 3_000,
                        end_ms: 3_010,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints { points: ips_points }
        );
    }

    #[test]
    fn ips_compaction_rewrites_shared_timestamped_page_once() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsLoad {
                        key: "compact-ips".to_string(),
                        points: vec![
                            FeaturePoint {
                                timestamp_ms: 10,
                                value: b"ten".to_vec(),
                            },
                            FeaturePoint {
                                timestamp_ms: 20,
                                value: b"twenty".to_vec(),
                            },
                        ],
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );

        let report = engine.compact_shard_pages(1).unwrap();
        assert_eq!(report.rewritten_page_refs, 1);

        let (first_address, second_address, meta_address) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("loaded shard");
            let series = shard.ips.get("compact-ips").expect("IPS series");
            let meta = shard.ips_meta.get("compact-ips").expect("IPS metadata");
            (
                series.get(&10).expect("first IPS point").clone(),
                series.get(&20).expect("second IPS point").clone(),
                meta.get(&20).expect("second IPS metadata").address.clone(),
            )
        };
        assert_eq!(first_address, second_address);
        assert_eq!(second_address, meta_address);
        let bytes = engine.page_store().read(&first_address).unwrap();
        assert_eq!(
            decode_feature_page(&bytes).expect("packed IPS page"),
            vec![
                FeaturePoint {
                    timestamp_ms: 10,
                    value: b"ten".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 20,
                    value: b"twenty".to_vec(),
                },
            ]
        );
    }

    // shared-corpus: risk_counter_window risk_family_query_and_delete risk_manager_debug_fol
    #[test]
    fn risk_query_supports_sum_min_max_and_event_count() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        for (aggregator, expected) in [("sum", 10), ("min", -2), ("max", 7), ("events", 3)] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskQuery {
                            key: "risk".to_string(),
                            start_ms: 0,
                            end_ms: 40,
                            aggregator: aggregator.to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: expected }
            );
        }
    }

    #[test]
    fn risk_change_matches_cpp_distinct_field_semantics() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, value) in [(10, "device-a"), (20, "device-a"), (30, "device-b")] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskChangeAdd {
                    key: "risk-change".to_string(),
                    timestamp_ms,
                    value: value.as_bytes().to_vec(),
                    precision_ms: Some(10),
                    ttl_ms: None,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskQuery {
                        key: "risk-change".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "change".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );

        for (timestamp_ms, value) in [(10, "buyer-1"), (20, "buyer-1"), (30, "buyer-2")] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskChangeAdd {
                    key: risk_family_key(RiskFamily::H, "risk-change"),
                    timestamp_ms,
                    value: value.as_bytes().to_vec(),
                    precision_ms: None,
                    ttl_ms: None,
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::H,
                        key: "risk-change".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        aggregator: "change".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
    }

    #[test]
    fn risk_query_supports_first_last_and_detail_list() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (timestamp_ms, amount) in [(10, 5), (20, -2), (30, 7)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::RiskIncrement {
                    key: "risk".to_string(),
                    timestamp_ms,
                    amount,
                },
            });
        }
        for (aggregator, expected) in [("first", 5), ("last", 7)] {
            assert_eq!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskQuery {
                            key: "risk".to_string(),
                            start_ms: 0,
                            end_ms: 40,
                            aggregator: aggregator.to_string(),
                        },
                    })
                    .response,
                CommandResponse::Integer { value: expected }
            );
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskDetail {
                        key: "risk".to_string(),
                        start_ms: 15,
                        end_ms: 40,
                        count: Some(2),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"-2".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"7".to_vec(),
                    },
                ]
            }
        );
    }

    #[test]
    fn risk_fol_matches_cpp_first_last_string_semantics() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        for (occur_time_ms, value) in [(20, "middle"), (10, "first"), (30, "last")] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFolSet {
                            key: "risk-fol-first".to_string(),
                            value: value.as_bytes().to_vec(),
                            occur_time_ms,
                            ttl_ms: 60_000,
                            fol_type: RiskFolType::First,
                        },
                    })
                    .status
                    .ok
            );
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskFolSet {
                            key: "risk-fol-last".to_string(),
                            value: value.as_bytes().to_vec(),
                            occur_time_ms,
                            ttl_ms: 60_000,
                            fol_type: RiskFolType::Last,
                        },
                    })
                    .status
                    .ok
            );
        }

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolQuery {
                        key: "risk-fol-first".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"first".to_vec()),
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFolQuery {
                        key: "risk-fol-last".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"last".to_vec()),
            }
        );
    }

    #[test]
    fn feature_write_policy_sequence_batch_ips_dimensions_and_risk_precision_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "feature-policy".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 10,
                    value: b"old".to_vec(),
                }],
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppendWithPolicy {
                        key: "feature-policy".to_string(),
                        points: vec![FeaturePoint {
                            timestamp_ms: 10,
                            value: b"ignored".to_vec(),
                        }],
                        policy: FeatureWritePolicy::InsertIfAbsent,
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureAppendWithPolicy {
                        key: "feature-policy".to_string(),
                        points: vec![FeaturePoint {
                            timestamp_ms: 10,
                            value: b"new".to_vec(),
                        }],
                        policy: FeatureWritePolicy::ReplaceExisting,
                    },
                })
                .response,
            CommandResponse::Integer { value: 1 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::FeatureQuery {
                        key: "feature-policy".to_string(),
                        start_ms: 0,
                        end_ms: 20,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 10,
                    value: b"new".to_vec(),
                }]
            }
        );

        for (key, gid, action_type) in [("seq-a", 1, 7), ("seq-b", 2, 8)] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::SequenceAdd {
                    key: key.to_string(),
                    rows: vec![SequenceFeatureRow {
                        timestamp_ms: 100,
                        gid,
                        action_type,
                        duration: 5,
                        author_id: 9,
                    }],
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::SequenceBatchQuery {
                        queries: vec![
                            SequenceQuerySpec {
                                key: "seq-a".to_string(),
                                start_ms: 0,
                                end_ms: 200,
                                count: 10,
                                filters: vec![FeatureFilter {
                                    field: "action_type".to_string(),
                                    op: FeatureFilterOp::Equal,
                                    value: 7,
                                }],
                            },
                            SequenceQuerySpec {
                                key: "seq-b".to_string(),
                                start_ms: 0,
                                end_ms: 200,
                                count: 10,
                                filters: Vec::new(),
                            },
                        ],
                    },
                })
                .response,
            CommandResponse::SequenceRowGroups {
                groups: vec![
                    (
                        "seq-a".to_string(),
                        vec![SequenceFeatureRow {
                            timestamp_ms: 100,
                            gid: 1,
                            action_type: 7,
                            duration: 5,
                            author_id: 9,
                        }],
                    ),
                    (
                        "seq-b".to_string(),
                        vec![SequenceFeatureRow {
                            timestamp_ms: 100,
                            gid: 2,
                            action_type: 8,
                            duration: 5,
                            author_id: 9,
                        }],
                    ),
                ],
            }
        );

        for (timestamp_ms, value, action_type, request_id) in [
            (10, b"a10".to_vec(), Some(1), Some("r1".to_string())),
            (20, b"a20".to_vec(), Some(2), Some("r2".to_string())),
            (30, b"a30".to_vec(), Some(1), Some("r3".to_string())),
        ] {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::IpsAddWithOptions {
                    key: "ips-dim".to_string(),
                    timestamp_ms,
                    instance: value,
                    action_type,
                    table_id: Some(99),
                    request_id,
                },
            });
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsAddWithOptions {
                        key: "ips-dim".to_string(),
                        timestamp_ms: 40,
                        instance: b"dup".to_vec(),
                        action_type: Some(1),
                        table_id: Some(99),
                        request_id: Some("r1".to_string()),
                    },
                })
                .response,
            CommandResponse::Integer { value: 0 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsQueryRangeWithOptions {
                        key: "ips-dim".to_string(),
                        start_ms: 0,
                        end_ms: 40,
                        count: None,
                        action_type: Some(1),
                        table_id: Some(99),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"a10".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"a30".to_vec(),
                    },
                ]
            }
        );

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrementWithOptions {
                key: "risk-bucket".to_string(),
                timestamp_ms: 1_234,
                amount: 3,
                precision_ms: Some(1_000),
                ttl_ms: Some(60_000),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::RiskIncrementWithOptions {
                key: "risk-bucket".to_string(),
                timestamp_ms: 1_999,
                amount: 4,
                precision_ms: Some(1_000),
                ttl_ms: None,
            },
        });
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskDetail {
                        key: "risk-bucket".to_string(),
                        start_ms: 0,
                        end_ms: 2_000,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1_000,
                    value: b"7".to_vec(),
                }]
            }
        );
        assert!(matches!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonTtl {
                        key: "risk-bucket".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value } if value > 0
        ));
    }

    #[test]
    fn maxmemory_config_rejects_writes_when_storage_budget_is_exhausted() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                maxmemory_bytes: Some(0),
                ..Config::default()
            },
        });

        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "first".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "storage_quota_exceeded");
    }

    #[test]
    fn write_qps_config_rejects_writes_after_admission_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                write_qps: Some(1),
                ..Config::default()
            },
        });
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "second".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "write_qps limit exceeded");
    }

    #[test]
    fn read_qps_config_rejects_reads_after_admission_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.set_config(SetConfigRequest {
            shard_id: 1,
            config: Config {
                version: 2,
                read_qps: Some(1),
                ..Config::default()
            },
        });
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "first".to_string(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "first".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "read_qps limit exceeded");
    }

    #[test]
    fn table_write_qps_config_is_shared_across_loaded_table_shards() {
        let engine = TemporalEngine::default();
        for shard_id in [1, 2] {
            assert!(
                engine
                    .load_shard_with(LoadShardRequest {
                        shard_id,
                        load_version: 1,
                        local_node_id: Some(1),
                        shard_uri: format!("local://feature_table/{shard_id}"),
                        start_routing_slot: 0,
                        end_routing_slot: u32::MAX,
                        readonly: false,
                        table_name: "feature_table".to_string(),
                    })
                    .status
                    .ok
            );
            engine.set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    table_write_qps: Some(1),
                    ..Config::default()
                },
            });
        }
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "first".to_string(),
                        value: b"x".to_vec(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::StringSet {
                key: "second".to_string(),
                value: b"y".to_vec(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "table_write_qps limit exceeded");
    }

    #[test]
    fn tenant_read_qps_config_is_shared_across_tables() {
        let engine = TemporalEngine::default();
        for (shard_id, table_name, key) in [(1, "feature_table", "k1"), (2, "risk_table", "k2")] {
            assert!(
                engine
                    .load_shard_with(LoadShardRequest {
                        shard_id,
                        load_version: 1,
                        local_node_id: Some(1),
                        shard_uri: format!("local://{table_name}/{shard_id}"),
                        start_routing_slot: 0,
                        end_routing_slot: u32::MAX,
                        readonly: false,
                        table_name: table_name.to_string(),
                    })
                    .status
                    .ok
            );
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: b"value".to_vec(),
                        },
                    })
                    .status
                    .ok
            );
            engine.set_config(SetConfigRequest {
                shard_id,
                config: Config {
                    version: 2,
                    tenant_name: Some("tenant-a".to_string()),
                    tenant_read_qps: Some(1),
                    ..Config::default()
                },
            });
        }
        wait_for_fresh_admission_second();

        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "k1".to_string(),
                    },
                })
                .status
                .ok
        );
        let rejected = engine.execute(ExecuteRequest {
            shard_id: 2,
            command: Command::StringGet {
                key: "k2".to_string(),
            },
        });
        assert_eq!(rejected.status.code, "admission_rejected");
        assert_eq!(rejected.status.message, "tenant_read_qps limit exceeded");
    }

    #[test]
    fn stats_include_cpp_style_partition_and_object_manager_accounting() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 9,
                    load_version: 77,
                    local_node_id: Some(3),
                    shard_uri: "local://table/shard-9".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "feature_table".to_string(),
                })
                .status
                .ok
        );
        for command in [
            Command::StringSet {
                key: "string-key".to_string(),
                value: b"v".to_vec(),
            },
            Command::HashSet {
                key: "hash-key".to_string(),
                field: "a".to_string(),
                value: b"1".to_vec(),
            },
            Command::HashSet {
                key: "hash-key".to_string(),
                field: "b".to_string(),
                value: b"2".to_vec(),
            },
            Command::SetAdd {
                key: "set-key".to_string(),
                member: b"m1".to_vec(),
            },
            Command::SetAdd {
                key: "set-key".to_string(),
                member: b"m2".to_vec(),
            },
            Command::FeatureAppend {
                key: "feature-key".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 1,
                        value: b"f1".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"f2".to_vec(),
                    },
                ],
            },
            Command::SequenceAdd {
                key: "sequence-key".to_string(),
                rows: vec![
                    SequenceFeatureRow {
                        timestamp_ms: 10,
                        gid: 1,
                        action_type: 2,
                        duration: 3,
                        author_id: 4,
                    },
                    SequenceFeatureRow {
                        timestamp_ms: 20,
                        gid: 5,
                        action_type: 6,
                        duration: 7,
                        author_id: 8,
                    },
                ],
            },
            Command::IpsAdd {
                key: "ips-key".to_string(),
                timestamp_ms: 30,
                instance: b"i".to_vec(),
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 9,
                        command,
                    })
                    .status
                    .ok
            );
        }

        let stats = engine.get_stats(9).stats.unwrap();
        assert_eq!(stats.total_records, 7);
        assert_eq!(stats.object_manager.object_count, 7);
        assert_eq!(stats.object_manager.page_ref_count, 10);
        assert_eq!(stats.object_manager.dirty_object_count, 7);
        assert!(stats.object_manager.dirty_slot_count > 0);
        assert!(stats.object_manager.dirty_slot_count <= 7);
        assert_eq!(stats.object_manager.routing_slot_count, 11);
        assert_eq!(stats.partition_info.table_name, "feature_table");
        assert_eq!(stats.partition_info.shard_uri, "local://table/shard-9");
        assert_eq!(stats.partition_info.start_routing_slot, 10);
        assert_eq!(stats.partition_info.end_routing_slot, 20);
        assert_eq!(stats.partition_info.object_manager, stats.object_manager);
        assert!(stats.page_store_zones.active_zones >= 1);
        assert!(stats.page_store_zones.active_physical_bytes > 0);
        assert_eq!(
            stats.page_store_zones.live_physical_bytes,
            stats.page_store_zones.active_physical_bytes
                + stats.page_store_zones.sealed_physical_bytes
        );
    }

    #[test]
    fn prometheus_metrics_include_records_cache_page_and_oplog() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        engine.page_store().roll_segment().unwrap();

        let metrics = engine.prometheus_metrics();
        assert!(metrics.contains("temporalstore_shard_records{shard_id=\"1\",kind=\"string\"} 1"));
        assert!(metrics.contains("temporalstore_cache_operations_total"));
        assert!(metrics.contains(
            "temporalstore_cache_operations_total{shard_id=\"1\",kind=\"memory_evictions\"}"
        ));
        assert!(metrics.contains("temporalstore_page_store_operations_total"));
        assert!(metrics
            .contains("temporalstore_page_store_zone_count{shard_id=\"1\",state=\"sealed\"} 1"));
        assert!(
            metrics.contains("temporalstore_page_store_zone_bytes{shard_id=\"1\",kind=\"live\"}")
        );
        assert!(metrics
            .contains("temporalstore_page_store_zone_bytes{shard_id=\"1\",kind=\"total_known\"}"));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_unix_ms{shard_id=\"1\",scope=\"known\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_unix_ms{shard_id=\"1\",scope=\"live\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_age_ms{shard_id=\"1\",scope=\"known\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_page_store_zone_oldest_age_ms{shard_id=\"1\",scope=\"live\"}"
        ));
        assert!(metrics.contains("temporalstore_oplog_records_total{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_objects{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_page_refs{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_dirty_objects{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_storage_slot_page_refs{shard_id=\"1\""));
        assert!(metrics.contains("temporalstore_storage_slot_bytes{shard_id=\"1\""));
        assert!(metrics.contains("temporalstore_storage_slot_dirty_objects{shard_id=\"1\""));
        assert!(
            metrics.contains("temporalstore_partition_routing_slots{shard_id=\"1\"} 4294967295")
        );
    }

    #[test]
    fn slot_storage_summaries_track_live_refs_dirty_slots_and_manifest_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard_with(LoadShardRequest {
            shard_id: 1,
            load_version: 0,
            local_node_id: None,
            shard_uri: String::new(),
            start_routing_slot: 10,
            end_routing_slot: 12,
            readonly: false,
            table_name: String::new(),
        });
        for key in ["alpha", "beta", "gamma"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value: key.as_bytes().to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let summaries = engine.slot_storage_summaries(1);
        assert!(!summaries.is_empty());
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.page_ref_count)
                .sum::<u64>(),
            3
        );
        assert_eq!(
            summaries
                .iter()
                .map(|summary| summary.dirty_object_count)
                .sum::<u64>(),
            3
        );
        let dirty_slot = summaries
            .iter()
            .find(|summary| summary.dirty_object_count > 0)
            .unwrap()
            .routing_slot;
        let manifest = engine
            .create_slot_dump_manifest(1, [dirty_slot])
            .expect("slot dump manifest should persist");
        engine.validate_slot_dump_manifest(&manifest).unwrap();
        let summaries = engine.slot_storage_summaries(1);
        assert!(summaries
            .iter()
            .filter(|summary| summary.routing_slot == dirty_slot)
            .all(|summary| summary.last_dump_sequence == manifest.index_log_sequence));
    }

    // shared-corpus: storage_slot_first_physical_index
    #[test]
    fn storage_physical_index_report_is_slot_first_and_page_index_complete() {
        let engine = TemporalEngine::default();
        engine.load_shard_with(LoadShardRequest {
            shard_id: 9,
            load_version: 77,
            local_node_id: Some(3),
            shard_uri: "local://table/shard-9".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 20,
            readonly: false,
            table_name: "physical_index_table".to_string(),
        });
        for command in [
            Command::StringSet {
                key: "string-key".to_string(),
                value: b"v".to_vec(),
            },
            Command::HashSet {
                key: "hash-key".to_string(),
                field: "a".to_string(),
                value: b"1".to_vec(),
            },
            Command::SetAdd {
                key: "set-key".to_string(),
                member: b"m1".to_vec(),
            },
            Command::FeatureAppend {
                key: "feature-key".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"f1".to_vec(),
                }],
            },
            Command::SequenceAdd {
                key: "sequence-key".to_string(),
                rows: vec![SequenceFeatureRow {
                    timestamp_ms: 10,
                    gid: 1,
                    action_type: 2,
                    duration: 3,
                    author_id: 4,
                }],
            },
            Command::IpsAdd {
                key: "ips-key".to_string(),
                timestamp_ms: 30,
                instance: b"i".to_vec(),
            },
            Command::RiskSet {
                family: RiskFamily::Cpc,
                key: "risk-key".to_string(),
                timestamp_ms: 40,
                amount: 5,
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 9,
                        command,
                    })
                    .status
                    .ok
            );
        }

        let report = engine.storage_physical_index_report(9);
        assert!(report.slot_first);
        assert!(report.slot_index_authority);
        assert!(report.slot_count > 0);
        assert_eq!(report.page_index_count, 7);
        assert_eq!(report.missing_object_id_count, 0);
        assert_eq!(report.missing_routing_slot_count, 0);
        assert_eq!(report.missing_page_id_count, 0);
        assert_eq!(report.missing_checksum_count, 0);
        assert_eq!(report.cpp_packed_page_index_size, 17);
        assert_eq!(report.cpp_packed_slot_node_size, 24);
        assert!(report.cpp_packed_layout_compatible);
        assert!(report.dirty_slot_count > 0);
        assert_eq!(
            report
                .slot_nodes
                .iter()
                .map(|slot| slot.page_indexes.len())
                .sum::<usize>(),
            report.page_index_count
        );
        assert!(report.slot_nodes.iter().all(|slot| {
            slot.page_ref_count == slot.page_indexes.len() as u64
                && slot.routing_slot >= 10
                && slot.routing_slot <= 20
                && slot.cpp_packed_slot_node_len == 24
                && slot.cpp_packed_slot_node_hex.len() == 48
        }));
        assert!(report
            .slot_nodes
            .iter()
            .filter(|slot| !slot.page_indexes.is_empty())
            .all(|slot| slot.meta_loaded && slot.in_memory));
        let page_indexes = report
            .slot_nodes
            .iter()
            .flat_map(|slot| slot.page_indexes.iter())
            .collect::<Vec<_>>();
        assert!(page_indexes.iter().all(|page| page.object_id.is_some()
            && page.page_id.is_some()
            && page.cpp_packed_page_index_len == 17
            && page.cpp_packed_page_index_hex.len() == 34));
        assert!(page_indexes.iter().all(|page| page
            .checksum
            .as_ref()
            .is_some_and(|checksum| !checksum.is_empty())));
        assert!(page_indexes
            .iter()
            .all(|page| page.log_backed && !page.deleted && page.dirty));
        assert!(page_indexes.iter().any(|page| page.model_id == "string"));
        assert!(page_indexes.iter().any(|page| page.model_id == "hash"));
        assert!(page_indexes.iter().any(|page| page.model_id == "set"));
        assert!(page_indexes
            .iter()
            .any(|page| page.object_key == "feature-key"));
        assert!(page_indexes
            .iter()
            .any(|page| page.object_key == "sequence-key"));
        assert!(page_indexes.iter().any(|page| page.object_key == "ips-key"));
        assert!(page_indexes
            .iter()
            .any(|page| { page.model_id == "risk" && page.object_key == "risk:cpc:risk-key" }));
    }

    // shared-corpus: storage_object_manager_slotstore_runtime_authority
    #[test]
    fn storage_object_manager_and_slotstore_runtime_modules_are_authoritative() {
        let engine = TemporalEngine::default();
        engine.load_shard_with(LoadShardRequest {
            shard_id: 94,
            load_version: 7,
            local_node_id: Some(4),
            shard_uri: "local://table/shard-94".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 110,
            readonly: false,
            table_name: "runtime_authority_table".to_string(),
        });
        for command in [
            Command::StringSet {
                key: "runtime-string".to_string(),
                value: b"value".to_vec(),
            },
            Command::HashSet {
                key: "runtime-hash".to_string(),
                field: "field".to_string(),
                value: b"hash".to_vec(),
            },
            Command::FeatureAppend {
                key: "runtime-feature".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 1,
                        value: b"one".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"two".to_vec(),
                    },
                ],
            },
            Command::ContextWriteEvent {
                tenant_hash: 94,
                node_hash: 940,
                event: ContextEvent {
                    event_id_hash: 9_400,
                    event_time_ms: 123,
                    kind: 1,
                    event_type: 2,
                    actor_hash: 3,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 1.0,
                    importance: 0.8,
                    text: "runtime authority context event".to_string(),
                    source_ref: "local://runtime-authority".to_string(),
                    related_node_hashes: vec![940],
                    compact_attrs: vec![1, 2, 3],
                },
                first_write_only: false,
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 94,
                        command,
                    })
                    .status
                    .ok
            );
        }

        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&94).expect("shard should exist");
        let object_manager = object_manager::runtime_report(shard);
        let slot_store = slot_store::runtime_report(shard);
        let physical = engine.storage_physical_index_report(94);

        assert!(object_manager.object_manager_runtime_module);
        assert!(object_manager.slot_index_authority);
        assert_eq!(object_manager.missing_object_owner_refs, 0);
        assert_eq!(
            object_manager.live_page_ref_count,
            physical.page_index_count
        );
        assert!(object_manager.live_object_count >= 4);
        assert!(slot_store.slot_store_runtime_module);
        assert!(slot_store.slot_index_authority);
        assert_eq!(slot_store.page_ref_count, physical.page_index_count);
        assert_eq!(slot_store.slot_count, physical.slot_count);
        assert!(slot_store.dirty_slot_count > 0);
        assert_eq!(
            slot_store.empty_slots
                + slot_store.single_object_slots
                + slot_store.single_page_object_slots
                + slot_store.multi_page_object_slots
                + slot_store.multi_object_slots,
            slot_store.slot_count
        );
    }

    // shared-corpus: storage_slot_layout_transitions
    #[test]
    fn storage_slot_layout_transitions_cover_growth_compaction_delete_dump_load_restart() {
        fn single_slot_layout(engine: &TemporalEngine, shard_id: ShardId) -> String {
            let report = engine.storage_physical_index_report(shard_id);
            let non_empty_slots = report
                .slot_nodes
                .iter()
                .filter(|slot| !slot.page_indexes.is_empty())
                .collect::<Vec<_>>();
            assert_eq!(non_empty_slots.len(), 1, "{report:?}");
            non_empty_slots[0].layout.clone()
        }

        fn load_single_slot_shard(engine: &TemporalEngine) {
            engine.load_shard_with(LoadShardRequest {
                shard_id: 51,
                load_version: 1,
                local_node_id: Some(1),
                shard_uri: "local://slot-layout/shard-51".to_string(),
                start_routing_slot: 7,
                end_routing_slot: 7,
                readonly: false,
                table_name: "slot_layout_table".to_string(),
            });
        }

        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let pages_dir = dir.path().join("pages");
        let indexes_dir = dir.path().join("indexes");
        let points = (0..10)
            .map(|offset| FeaturePoint {
                timestamp_ms: 10_000 + offset,
                value: vec![b'a' + offset as u8; 10 * 1024],
            })
            .collect::<Vec<_>>();

        {
            let engine = TemporalEngine::with_local_dirs(
                8 * 1024 * 1024,
                cache_dir.clone(),
                pages_dir.clone(),
                indexes_dir.clone(),
            );
            load_single_slot_shard(&engine);
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 51,
                        command: Command::StringSet {
                            key: "slot-layout-a".to_string(),
                            value: b"v1".to_vec(),
                        },
                    })
                    .status
                    .ok
            );
            assert_eq!(single_slot_layout(&engine, 51), "single_page_object");

            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 51,
                        command: Command::StringSet {
                            key: "slot-layout-b".to_string(),
                            value: b"v2".to_vec(),
                        },
                    })
                    .status
                    .ok
            );
            assert_eq!(single_slot_layout(&engine, 51), "multi_object");

            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 51,
                        command: Command::CommonDelete {
                            key: "slot-layout-b".to_string(),
                        },
                    })
                    .status
                    .ok
            );
            assert_eq!(single_slot_layout(&engine, 51), "single_page_object");

            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 51,
                        command: Command::CommonDelete {
                            key: "slot-layout-a".to_string(),
                        },
                    })
                    .status
                    .ok
            );
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 51,
                        command: Command::FeatureAppend {
                            key: "slot-layout-feature".to_string(),
                            points: points.clone(),
                        },
                    })
                    .status
                    .ok
            );
            assert_eq!(single_slot_layout(&engine, 51), "multi_page_object");

            let compact_report = engine.compact_shard_pages(51).unwrap();
            assert!(compact_report.rewritten_page_refs > 0);
            assert_eq!(single_slot_layout(&engine, 51), "multi_page_object");

            let manifest = engine.create_slot_dump_manifest(51, [7]).unwrap();
            assert_eq!(manifest.slot_ids, vec![7]);
            engine.validate_slot_dump_manifest(&manifest).unwrap();
            engine.install_slot_dump_manifest(&manifest).unwrap();
            assert_eq!(single_slot_layout(&engine, 51), "multi_page_object");
        }

        let restored =
            TemporalEngine::with_local_dirs(8 * 1024 * 1024, cache_dir, pages_dir, indexes_dir);
        load_single_slot_shard(&restored);
        assert_eq!(single_slot_layout(&restored, 51), "multi_page_object");
        assert_eq!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 51,
                    command: Command::FeatureQuery {
                        key: "slot-layout-feature".to_string(),
                        start_ms: 0,
                        end_ms: 20_000,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints { points }
        );
    }

    // shared-corpus: storage_model_layout_compaction_policies
    #[test]
    fn storage_compaction_reports_model_layout_policies_and_density() {
        let engine = TemporalEngine::default();
        engine.load_shard(61);
        for command in [
            Command::StringSet {
                key: "compact-string".to_string(),
                value: b"value".to_vec(),
            },
            Command::HashSet {
                key: "compact-hash".to_string(),
                field: "field-a".to_string(),
                value: b"hash-value".to_vec(),
            },
            Command::SetAdd {
                key: "compact-set".to_string(),
                member: b"member-a".to_vec(),
            },
            Command::FeatureAppend {
                key: "compact-feature".to_string(),
                points: (0..4)
                    .map(|offset| FeaturePoint {
                        timestamp_ms: 1_000 + offset,
                        value: vec![b'f' + offset as u8; 8 * 1024],
                    })
                    .collect(),
            },
            Command::ContextWriteEvent {
                tenant_hash: 44,
                node_hash: 55,
                event: ContextEvent {
                    event_id_hash: 66,
                    event_time_ms: 4_000,
                    kind: 1,
                    event_type: 2,
                    actor_hash: 77,
                    status: 1,
                    valid_until_ms: 0,
                    confidence: 0.99,
                    importance: 0.75,
                    text: "context event for compaction".to_string(),
                    source_ref: "local://compaction-test".to_string(),
                    related_node_hashes: vec![55],
                    compact_attrs: vec![1, 2, 3],
                },
                first_write_only: false,
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 61,
                        command,
                    })
                    .status
                    .ok
            );
        }

        let report = engine.compact_shard_pages(61).unwrap();
        assert!(report.rewritten_page_refs >= 5);
        let policies = report
            .before
            .model_policies
            .iter()
            .map(|policy| {
                (
                    policy.model_id.as_str(),
                    policy.layout_policy.as_str(),
                    policy,
                )
            })
            .collect::<Vec<_>>();
        for (model_id, layout_policy) in [
            ("string", "single_page_object"),
            ("hash", "component_page_object"),
            ("set", "component_page_object"),
            ("feature", "timestamped_chunked_pages"),
            ("context_event", "context_timeline_or_sidecar_pages"),
        ] {
            let (_, _, policy) = policies
                .iter()
                .find(|(actual_model, actual_policy, _)| {
                    actual_model == &model_id && actual_policy == &layout_policy
                })
                .unwrap_or_else(|| {
                    panic!("missing policy {model_id}/{layout_policy}: {policies:?}")
                });
            assert!(policy.live_page_refs > 0);
            assert!(policy.total_segment_pages >= policy.live_page_refs);
            assert!(policy.stale_density_basis_points <= 10_000);
            assert!(policy.tombstone_density_basis_points <= 10_000);
        }
        assert!(report
            .after
            .model_policies
            .iter()
            .any(|policy| policy.model_id == "feature"
                && policy.layout_policy == "timestamped_chunked_pages"));
    }

    // shared-corpus: storage_merged_dump_load_lifecycle
    #[test]
    fn storage_merged_dump_load_tracks_rollback_handoff_and_conflicts() {
        fn key_for_slot(engine: &TemporalEngine, shard_id: ShardId, slot: u32) -> String {
            (0..10_000)
                .map(|idx| format!("merged-slot-{slot}-{idx}"))
                .find(|key| engine.routing_slot_for_key(shard_id, key) == slot)
                .expect("test should find key for routing slot")
        }

        let engine = TemporalEngine::default();
        engine.load_shard_with(LoadShardRequest {
            shard_id: 91,
            load_version: 5,
            local_node_id: Some(1),
            shard_uri: "local://merged-dump/shard-91".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 12,
            readonly: false,
            table_name: "merged_dump_table".to_string(),
        });
        let key_a = key_for_slot(&engine, 91, 10);
        let key_b = key_for_slot(&engine, 91, 11);
        for (key, value) in [(&key_a, b"a".as_slice()), (&key_b, b"b".as_slice())] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 91,
                        command: Command::StringSet {
                            key: key.clone(),
                            value: value.to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let manifest_a = engine.create_slot_dump_manifest(91, [10]).unwrap();
        let manifest_b = engine.create_slot_dump_manifest(91, [11]).unwrap();
        let merged = engine
            .create_merged_slot_dump_manifest(
                91,
                [10, 11],
                vec![
                    manifest_b.manifest_id.clone(),
                    manifest_a.manifest_id.clone(),
                    manifest_a.manifest_id.clone(),
                ],
                Some(6),
            )
            .unwrap();
        assert_eq!(merged.manifest_kind, "merged_slot_dump");
        assert_eq!(
            merged.source_manifest_ids,
            vec![
                manifest_a.manifest_id.clone(),
                manifest_b.manifest_id.clone()
            ]
        );
        assert_eq!(merged.slot_ids, vec![10, 11]);
        assert_eq!(
            merged.load_version_handoff,
            Some(SlotDumpLoadVersionHandoff {
                previous_load_version: 5,
                next_load_version: 6,
                applied: false,
            })
        );

        let install = engine.install_merged_slot_dump_manifest(&merged);
        assert!(install.installed, "{install:?}");
        assert!(install.rollback_marker_written);
        assert!(install.prepare_marker_written);
        assert!(install.install_marker_written);
        assert!(install.commit_marker_written);
        assert_eq!(install.status_code, "ok");
        assert_eq!(
            install.load_version_handoff,
            Some(SlotDumpLoadVersionHandoff {
                previous_load_version: 5,
                next_load_version: 6,
                applied: true,
            })
        );
        assert_eq!(
            engine
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&91)
                .map(|info| info.load_version),
            Some(6)
        );

        let stale = engine
            .create_merged_slot_dump_manifest(91, [10], vec![merged.manifest_id.clone()], Some(7))
            .unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 91,
                    command: Command::StringSet {
                        key: key_a.clone(),
                        value: b"new-a".to_vec(),
                    },
                })
                .status
                .ok
        );
        let preflight = engine.slot_dump_install_preflight_report(&stale);
        assert!(!preflight.install_safe);
        assert!(preflight.stale_manifest);
        assert!(preflight.stale_page_conflict_count > 0);
        assert!(preflight
            .blockers
            .contains(&"stale_page_conflicts".to_string()));
    }

    // shared-corpus: storage_object_manager_cold_hot_reload storage_page_address_disk_cache_shared_store_fallback
    #[test]
    fn storage_object_manager_cold_hot_reload_and_page_address_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            32,
            cache_dir.clone(),
            page_dir.clone(),
            index_dir.clone(),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "cold-hot".to_string(),
                        value: b"object-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "cold-hot".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"object-value".to_vec())
            }
        );
        let hot_stats = engine.cache().stats();
        assert!(hot_stats.puts > 0);

        engine.cache().clear_memory_for_test();
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "cold-hot".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"object-value".to_vec())
            }
        );
        assert!(engine.cache().stats().disk_fills > 0);

        engine.cache().invalidate_shard(1).unwrap();
        let reads_before = engine.page_store().stats().reads;
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "cold-hot".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"object-value".to_vec())
            }
        );
        assert!(engine.page_store().stats().reads > reads_before);

        let restored = TemporalEngine::with_local_dirs(32, cache_dir, page_dir, index_dir);
        restored.load_shard(1);
        let report = restored.storage_physical_index_report(1);
        assert!(report.slot_index_authority);
        assert!(report
            .slot_nodes
            .iter()
            .flat_map(|slot| slot.page_indexes.iter())
            .any(|page| page.model_id == "string"
                && page.object_key == "cold-hot"
                && page.object_id.is_some()));
        assert_eq!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "cold-hot".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"object-value".to_vec())
            }
        );
    }

    // shared-corpus: storage_tombstone_compaction storage_stale_page_density_compaction
    #[test]
    fn storage_compaction_reports_tombstones_and_stale_density() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "tombstone-me".to_string(),
                        value: b"gone".to_vec(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::CommonDelete {
                        key: "tombstone-me".to_string(),
                    },
                })
                .status
                .ok
        );
        for value in ["v1", "v2", "v3", "v4"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: "dense".to_string(),
                            value: value.as_bytes().to_vec(),
                        },
                    })
                    .status
                    .ok
            );
        }
        let recovery = engine.storage_recovery_report(1);
        assert!(recovery.object_lifecycle.tombstoned_object_ids > 0);
        assert!(recovery
            .object_lifecycle
            .tombstoned_object_keys
            .contains(&"tombstone-me".to_string()));

        let compact = engine.compact_shard_pages(1).unwrap();
        let string_policy = compact
            .before
            .model_policies
            .iter()
            .find(|policy| policy.model_id == "string")
            .expect("string compaction policy should exist");
        assert!(string_policy.stale_page_estimate > 0);
        assert!(string_policy.stale_density_basis_points > 0);
        assert!(compact.rewritten_page_refs > 0);
    }

    // shared-corpus: storage_merged_dump_load_restart_interruption
    #[test]
    fn storage_merged_dump_load_restart_interruption_reports_rollback_marker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard_with(LoadShardRequest {
            shard_id: 92,
            load_version: 3,
            local_node_id: Some(1),
            shard_uri: "local://merged-dump-interrupt/shard-92".to_string(),
            start_routing_slot: 0,
            end_routing_slot: 10,
            readonly: false,
            table_name: "merged_dump_interrupt_table".to_string(),
        });
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 92,
                    command: Command::StringSet {
                        key: "interrupt".to_string(),
                        value: b"safe".to_vec(),
                    },
                })
                .status
                .ok
        );
        let source = engine.create_slot_dump_manifest(92, Vec::new()).unwrap();
        let merged = engine
            .create_merged_slot_dump_manifest(
                92,
                Vec::new(),
                vec![source.manifest_id.clone()],
                Some(4),
            )
            .unwrap();
        engine
            .persist_slot_dump_install_marker(&merged, "rollback")
            .unwrap();
        engine
            .persist_slot_dump_install_marker(&merged, "prepare")
            .unwrap();

        let all_markers = list_slot_dump_install_markers_at(&engine.index_dir, 92).unwrap();
        assert!(all_markers
            .iter()
            .any(|marker| marker.manifest_id == merged.manifest_id && marker.phase == "rollback"));
        let interrupted = engine.interrupted_slot_dump_installs(92);
        let prepare_marker = interrupted
            .iter()
            .find(|marker| marker.manifest_id == merged.manifest_id && marker.phase == "prepare")
            .expect("prepare marker should survive restart interruption scan");
        let roll_forward = engine.slot_dump_install_roll_forward_report(prepare_marker);
        assert_eq!(roll_forward.shard_id, 92);
        assert!(!roll_forward.completed_commit);
    }

    // shared-corpus: storage_risk_context_page_backed_parity
    #[test]
    fn storage_risk_and_context_page_backed_restart_parity() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(
            4096,
            cache_dir.clone(),
            page_dir.clone(),
            index_dir.clone(),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskSet {
                        family: RiskFamily::Cpc,
                        key: "risk-page".to_string(),
                        timestamp_ms: 100,
                        amount: 9,
                    },
                })
                .status
                .ok
        );
        let entity = ContextEntity {
            entity_hash: 7007,
            node_hash: 42,
            entity_type: 1,
            name: "risk_context_entity".to_string(),
            value: "present".to_string(),
            updated_at_ms: 100,
            valid_from_ms: 100,
            confidence: 0.95,
            source_event_hashes: vec![707],
        };
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextUpsertEntity {
                        tenant_hash: 9,
                        entity: entity.clone(),
                    },
                })
                .status
                .ok
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextWriteEvent {
                        tenant_hash: 9,
                        node_hash: 42,
                        event: ContextEvent {
                            event_id_hash: 707,
                            event_time_ms: 100,
                            kind: 2,
                            event_type: 3,
                            actor_hash: 4,
                            status: 1,
                            valid_until_ms: 0,
                            confidence: 0.91,
                            importance: 0.8,
                            text: "risk context event".to_string(),
                            source_ref: "local://risk-context".to_string(),
                            related_node_hashes: vec![42],
                            compact_attrs: vec![1],
                        },
                        first_write_only: false,
                    },
                })
                .status
                .ok
        );
        let physical = engine.storage_physical_index_report(1);
        let pages = physical
            .slot_nodes
            .iter()
            .flat_map(|slot| slot.page_indexes.iter())
            .collect::<Vec<_>>();
        assert!(pages.iter().any(|page| page.model_id == "risk"
            && page.object_key == "risk:cpc:risk-page"
            && page.object_id.is_some()));
        assert!(pages.iter().any(|page| page.model_id == "context_entity"
            && page.object_key == "ctx:entity:9:42:7007"
            && page.object_id.is_some()));
        assert!(pages.iter().any(|page| page.model_id == "context_event"
            && page.object_key == "ctx:event:9:42"
            && page.object_id.is_some()));

        let restored = TemporalEngine::with_local_dirs(4096, cache_dir, page_dir, index_dir);
        restored.load_shard(1);
        assert_eq!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::Cpc,
                        key: "risk-page".to_string(),
                        start_ms: 0,
                        end_ms: 200,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 9 }
        );
        assert!(matches!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextGetEntity {
                        tenant_hash: 9,
                        node_hash: 42,
                        entity_hash: 7007,
                    },
                })
                .response,
            CommandResponse::ContextEntity { entity: Some(ref stored), .. } if stored == &entity
        ));
        assert!(matches!(
            restored
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextQueryEvents {
                        tenant_hash: 9,
                        node_hash: 42,
                        start_time_ms: 0,
                        end_time_ms: 200,
                        limit: Some(10),
                        current_valid_only: true,
                        as_of_ms: 0,
                        kinds: vec![2],
                        statuses: vec![1],
                        min_confidence: 0.0,
                        min_importance: 0.0,
                    },
                })
                .response,
            CommandResponse::ContextEvents { ref events, .. } if events.len() == 1
        ));
    }

    #[test]
    fn slot_dump_manifest_validation_rejects_checksum_and_missing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let mut manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        manifest.logical_bytes = manifest.logical_bytes.saturating_add(1);
        assert!(
            !engine
                .validate_slot_dump_manifest(&manifest)
                .unwrap_err()
                .ok
        );

        let mut missing = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        missing.page_segment_ids.push(999_999);
        missing.checksum = slot_dump_manifest_checksum(&missing).unwrap();
        let missing_preflight = engine.slot_dump_install_preflight_report(&missing);
        assert!(!missing_preflight.install_safe);
        assert_eq!(missing_preflight.missing_page_segment_ids, vec![999_999]);
        assert!(missing_preflight
            .blockers
            .contains(&"missing_page_segments".to_string()));
        assert!(!engine.validate_slot_dump_manifest(&missing).unwrap_err().ok);

        let mut incomplete = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        incomplete.page_segment_ids.clear();
        incomplete.checksum = slot_dump_manifest_checksum(&incomplete).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&incomplete)
                .unwrap_err()
                .code,
            "slot_dump_page_segment_mismatch"
        );

        let corrupt = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        let segment_id = corrupt.page_segment_ids[0];
        let mut segment = engine.page_store().read_segment(segment_id).unwrap();
        *segment.last_mut().unwrap() ^= 0xff;
        let _ = engine.page_store().install_segment(segment_id, &segment);
        let corrupt_preflight = engine.slot_dump_install_preflight_report(&corrupt);
        assert!(!corrupt_preflight.install_safe);
        assert!(corrupt_preflight
            .corrupt_page_segment_ids
            .contains(&segment_id));
        assert!(corrupt_preflight.unreadable_page_ref_count > 0);
        assert!(corrupt_preflight.unreadable_page_bytes > 0);
        assert!(corrupt_preflight
            .blockers
            .contains(&"unreadable_page_refs".to_string()));
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&corrupt)
                .unwrap_err()
                .code,
            "slot_dump_unreadable_page_refs"
        );
    }

    #[test]
    fn slot_dump_manifest_install_restores_index_and_rejects_partial_or_stale() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "restore-me".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");

        let restore_engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("restore-cache"),
            dir.path().join("pages"),
            dir.path().join("restore-indexes"),
        );
        restore_engine.load_shard(1);
        let safe_preflight = restore_engine.slot_dump_install_preflight_report(&manifest);
        assert!(safe_preflight.install_safe, "{safe_preflight:?}");
        assert!(safe_preflight.blockers.is_empty());
        assert_eq!(
            safe_preflight.manifest_index_log_sequence,
            manifest.index_log_sequence
        );
        restore_engine
            .install_slot_dump_manifest(&manifest)
            .expect("manifest should install");
        assert!(
            fs::read_dir(dir.path().join("restore-indexes"))
                .unwrap()
                .filter_map(Result::ok)
                .all(|entry| !entry.file_name().to_string_lossy().contains(".tmp-")),
            "slot dump install should not leave atomic index temp files"
        );
        assert!(restore_engine.interrupted_slot_dump_installs(1).is_empty());
        let markers = list_slot_dump_install_markers_at(&restore_engine.index_dir, 1).unwrap();
        assert!(markers.iter().any(|marker| marker.phase == "prepare"));
        assert!(markers.iter().any(|marker| marker.phase == "install"));
        assert!(markers.iter().any(|marker| marker.phase == "commit"));
        let response = restore_engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "restore-me".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"v1".to_vec())
            }
        );

        let mut partial = manifest.clone();
        partial.index_bytes.clear();
        partial.checksum = slot_dump_manifest_checksum(&partial).unwrap();
        assert_eq!(
            restore_engine
                .install_slot_dump_manifest(&partial)
                .unwrap_err()
                .code,
            "slot_dump_partial_manifest"
        );

        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "newer".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let stale_preflight = engine.slot_dump_install_preflight_report(&manifest);
        assert!(!stale_preflight.install_safe);
        assert!(stale_preflight.stale_manifest);
        assert!(stale_preflight
            .blockers
            .contains(&"stale_manifest_sequence".to_string()));
        assert_eq!(
            engine
                .install_slot_dump_manifest(&manifest)
                .unwrap_err()
                .code,
            "slot_dump_stale_manifest"
        );
    }

    #[test]
    fn slot_dump_install_markers_report_interrupted_prepare() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "marker".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: "interrupted".to_string(),
                phase: "prepare".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let interrupted = engine.interrupted_slot_dump_installs(1);
        assert_eq!(interrupted.len(), 1);
        assert_eq!(interrupted[0].phase, "prepare");
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.interrupted_slot_dump_installs, interrupted);
        assert_eq!(boundary.prepared_slot_dump_install_count, 1);
        assert_eq!(boundary.installed_slot_dump_install_count, 0);
        assert_eq!(boundary.unknown_slot_dump_install_count, 0);
        let readiness = engine.storage_production_readiness_report(1);
        assert_eq!(readiness.interrupted_slot_dump_install_count, 1);
        assert_eq!(readiness.prepared_slot_dump_install_count, 1);
        assert_eq!(readiness.installed_slot_dump_install_count, 0);
        assert_eq!(readiness.unknown_slot_dump_install_count, 0);
    }

    #[test]
    fn slot_dump_install_roll_forward_completes_safe_installed_marker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "roll".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                phase: "install".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let dry_run = engine.slot_dump_install_roll_forward_reports(1);
        assert_eq!(dry_run.len(), 1);
        assert!(dry_run[0].can_roll_forward);
        assert_eq!(dry_run[0].reason, "commit_ready");

        let applied = engine.roll_forward_slot_dump_installs(1);
        assert_eq!(applied.len(), 1);
        assert!(applied[0].completed_commit);
        assert!(applied[0].obsolete_marker_files_removed > 0);
        assert!(engine.interrupted_slot_dump_installs(1).is_empty());
        let marker_files =
            slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
        assert!(marker_files
            .iter()
            .all(|(marker, _)| marker.phase == "commit"));
    }

    #[test]
    fn slot_dump_install_roll_forward_retries_safe_prepare_marker() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "retry-prepare".to_string(),
                value: b"value".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: manifest.shard_id,
                manifest_id: manifest.manifest_id.clone(),
                phase: "prepare".to_string(),
                oplog_sequence: manifest.oplog_sequence,
                index_log_sequence: manifest.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let dry_run = engine.slot_dump_install_roll_forward_reports(1);
        assert_eq!(dry_run.len(), 1);
        assert!(dry_run[0].can_retry_install);
        assert!(!dry_run[0].can_roll_forward);
        assert_eq!(dry_run[0].reason, "install_retry_ready");

        let applied = engine.roll_forward_slot_dump_installs(1);
        assert_eq!(applied.len(), 1);
        assert!(applied[0].completed_install);
        assert!(applied[0].completed_commit);
        assert!(applied[0].obsolete_marker_files_removed > 0);
        assert!(engine.interrupted_slot_dump_installs(1).is_empty());
        let marker_files =
            slot_dump_install_marker_files_at(&engine.index_dir, 1).expect("marker files");
        assert!(marker_files
            .iter()
            .all(|(marker, _)| marker.phase == "commit"));
    }

    #[test]
    fn slot_dump_recovery_reports_broken_manifest_parent_chain() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "chain".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("parent manifest should persist");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "chain".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("child manifest should persist");
        assert_eq!(child.parent_manifest_id, Some(parent.manifest_id.clone()));

        fs::remove_file(slot_dump_manifest_path(
            &engine.index_dir,
            1,
            &parent.manifest_id,
        ))
        .unwrap();
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.manifest_chain_issues.len(), 1);
        assert_eq!(
            boundary.manifest_chain_issues[0].manifest_id,
            child.manifest_id
        );
        assert_eq!(
            boundary.manifest_chain_issues[0].reason,
            "missing_parent_manifest"
        );
    }

    #[test]
    fn slot_dump_manifest_prune_keeps_latest_parent_chain_and_removes_obsolete_fork() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "prune".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "prune".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let mut fork = parent.clone();
        fork.manifest_id = format!("{}-fork", fork.manifest_id);
        fork.parent_manifest_id = None;
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        engine.persist_slot_dump_manifest(&fork).unwrap();
        write_slot_dump_install_marker(
            &engine.index_dir,
            &SlotDumpInstallMarker {
                shard_id: 1,
                manifest_id: fork.manifest_id.clone(),
                phase: "commit".to_string(),
                oplog_sequence: fork.oplog_sequence,
                index_log_sequence: fork.index_log_sequence,
                created_unix_ms: now_ms(),
            },
        )
        .unwrap();

        let plan = engine.slot_dump_manifest_prune_plan(1);
        assert!(plan.retained_manifest_ids.contains(&parent.manifest_id));
        assert!(plan.retained_manifest_ids.contains(&child.manifest_id));
        assert_eq!(plan.prunable_manifest_ids, vec![fork.manifest_id.clone()]);
        assert_eq!(
            plan.prunable_marker_manifest_ids,
            vec![fork.manifest_id.clone()]
        );

        let lifecycle = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: true,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        let report = lifecycle
            .manifest_prune_report
            .expect("lifecycle should apply manifest prune");
        assert_eq!(report.removed_manifest_ids, vec![fork.manifest_id.clone()]);
        assert_eq!(report.removed_marker_files, 1);
        assert_eq!(
            lifecycle.manifest_prune_plan.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );
        assert!(slot_dump_manifest_path(&engine.index_dir, 1, &parent.manifest_id).exists());
        assert!(slot_dump_manifest_path(&engine.index_dir, 1, &child.manifest_id).exists());
        assert!(!slot_dump_manifest_path(&engine.index_dir, 1, &fork.manifest_id).exists());
    }

    #[test]
    fn slot_dump_manifest_prune_is_blocked_by_lagging_follower_cursor() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "cursor".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "cursor".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let mut fork = parent.clone();
        fork.manifest_id = format!("{}-follower-anchor", fork.manifest_id);
        fork.parent_manifest_id = None;
        fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        engine.persist_slot_dump_manifest(&fork).unwrap();

        let no_cursor = engine.slot_dump_manifest_prune_plan(1);
        assert_eq!(
            no_cursor.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );

        let lagging_cursor = SlotDumpFollowerReplayCursor {
            follower_id: "follower-a".to_string(),
            shard_id: 1,
            oplog_sequence: fork.oplog_sequence,
            index_log_sequence: fork.index_log_sequence,
        };
        let blocked = engine
            .slot_dump_manifest_prune_plan_with_follower_cursors(1, vec![lagging_cursor.clone()]);
        assert!(blocked.prunable_manifest_ids.is_empty());
        assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
        assert_eq!(blocked.follower_blocks.len(), 1);
        assert_eq!(blocked.follower_blocks[0].follower_id, "follower-a");
        assert_eq!(blocked.follower_blocks[0].manifest_id, fork.manifest_id);
        assert!(blocked
            .reasons
            .contains(&"follower_cursor_blocks_prune".to_string()));
        let cycle = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            dry_run: true,
            follower_replay_cursors: vec![lagging_cursor.clone()],
            ..StorageManagerCycleRequest::default()
        });
        assert_eq!(cycle.pressure_signals.follower_cursor_retention_blockers, 1);
        let index_gc = cycle
            .stages
            .iter()
            .find(|stage| stage.stage == "index_gc")
            .expect("index_gc stage");
        assert!(index_gc.pressure_score >= 1);

        let caught_up = engine.slot_dump_manifest_prune_plan_with_follower_cursors(
            1,
            vec![SlotDumpFollowerReplayCursor {
                oplog_sequence: child.oplog_sequence,
                index_log_sequence: child.index_log_sequence,
                ..lagging_cursor
            }],
        );
        assert_eq!(
            caught_up.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );
    }

    // shared-corpus: storage_manager_wal_reclaim_slot_generation_retention
    #[test]
    fn storage_manager_wal_reclaim_requires_durable_slot_generation_frontier() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "wal-reclaim-a".to_string(),
                value: b"a1".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "wal-reclaim-b".to_string(),
                value: b"b1".to_vec(),
            },
        });
        let _full_manifest = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "wal-reclaim-b".to_string(),
                value: b"b2".to_vec(),
            },
        });

        let blocked = engine.storage_wal_reclaim_plan(1, Vec::new(), Vec::new());
        assert!(!blocked.safe_to_reclaim, "{blocked:#?}");
        assert_eq!(blocked.uncovered_slot_count, 1);
        assert!(blocked
            .blocker_reasons
            .contains(&"slot_generation_without_durable_dump".to_string()));

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_slots_per_round: 16,
            ..StorageManagerCycleRequest::default()
        });
        let reclaim = report.wal_reclaim_report.as_ref().unwrap();
        assert!(reclaim.plan.safe_to_reclaim, "{report:#?}");
        assert!(reclaim.applied, "{report:#?}");
        assert!(reclaim.oplog_records_removed >= 1);
        assert!(reclaim.index_log_records_removed >= 1);
        assert!(!reclaim.plan.retained_manifest_ids.is_empty());

        for key in ["wal-reclaim-a", "wal-reclaim-b"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            });
            assert!(response.status.ok, "{response:?}");
        }
    }

    // shared-corpus: storage_manager_wal_reclaim_slot_generation_retention
    #[test]
    fn storage_manager_wal_reclaim_honors_follower_cursor_frontier() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "wal-cursor".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let first_manifest = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "wal-cursor".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let _manifest = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let cursor = SlotDumpFollowerReplayCursor {
            follower_id: "lagging-follower".to_string(),
            shard_id: 1,
            oplog_sequence: first_manifest.oplog_sequence,
            index_log_sequence: first_manifest.index_log_sequence,
        };

        let plan = engine.storage_wal_reclaim_plan(1, vec![cursor.clone()], Vec::new());
        assert!(plan.safe_to_reclaim, "{plan:#?}");
        assert_eq!(plan.follower_cursor_block_count, 1);
        assert_eq!(
            plan.retain_from_oplog_sequence,
            cursor.oplog_sequence.saturating_add(1)
        );
        assert_eq!(
            plan.retain_from_index_log_sequence,
            cursor.index_log_sequence.saturating_add(1)
        );
        assert!(plan
            .blocker_reasons
            .iter()
            .any(|reason| reason.contains("follower_cursor_retains_logs")));
    }

    #[test]
    fn slot_dump_manifest_prune_is_blocked_by_raft_snapshot_reference() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "snapshot".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let parent = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "snapshot".to_string(),
                value: b"v2".to_vec(),
            },
        });
        let child = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        let mut fork = parent.clone();
        fork.manifest_id = format!("{}-snapshot-anchor", fork.manifest_id);
        fork.parent_manifest_id = None;
        fork.created_unix_ms = parent.created_unix_ms.saturating_add(1);
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        engine.persist_slot_dump_manifest(&fork).unwrap();

        let no_snapshot = engine.slot_dump_manifest_prune_plan(1);
        assert_eq!(
            no_snapshot.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );

        let snapshot_ref = SlotDumpRaftSnapshotRef {
            snapshot_id: "raft-snapshot-0007".to_string(),
            shard_id: 1,
            last_included_index: 7,
            last_included_term: 2,
            oplog_sequence: fork.oplog_sequence,
            index_log_sequence: fork.index_log_sequence,
        };
        let blocked = engine.slot_dump_manifest_prune_plan_with_retention_refs(
            1,
            Vec::<SlotDumpFollowerReplayCursor>::new(),
            vec![snapshot_ref.clone()],
        );
        assert!(blocked.prunable_manifest_ids.is_empty());
        assert!(blocked.retained_manifest_ids.contains(&fork.manifest_id));
        assert_eq!(blocked.raft_snapshot_blocks.len(), 1);
        assert_eq!(
            blocked.raft_snapshot_blocks[0].snapshot_id,
            "raft-snapshot-0007"
        );
        assert_eq!(
            blocked.raft_snapshot_blocks[0].manifest_id,
            fork.manifest_id
        );
        assert!(blocked
            .reasons
            .contains(&"raft_snapshot_blocks_prune".to_string()));

        let advanced = engine.slot_dump_manifest_prune_plan_with_retention_refs(
            1,
            Vec::<SlotDumpFollowerReplayCursor>::new(),
            vec![SlotDumpRaftSnapshotRef {
                oplog_sequence: child.oplog_sequence,
                index_log_sequence: child.index_log_sequence,
                ..snapshot_ref
            }],
        );
        assert_eq!(
            advanced.prunable_manifest_ids,
            vec![fork.manifest_id.clone()]
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_generation_mismatch_and_conflicts() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "generation".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        assert_eq!(manifest.version, 3);
        assert!(!manifest.dump_generation_id.is_empty());
        assert_eq!(manifest.object_lifecycle.live_object_ids, 1);
        assert_eq!(manifest.object_lifecycle.live_page_refs, 1);

        let mut legacy_v2 = manifest.clone();
        legacy_v2.version = 2;
        legacy_v2.object_lifecycle = StorageObjectLifecycleReport::default();
        let legacy_generation_id = slot_dump_generation_id(&legacy_v2);
        legacy_v2.object_lifecycle.live_object_ids = 99;
        assert_eq!(slot_dump_generation_id(&legacy_v2), legacy_generation_id);

        let mut mismatched = manifest.clone();
        mismatched.dump_generation_id = "wrong-generation".to_string();
        mismatched.checksum = slot_dump_manifest_checksum(&mismatched).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&mismatched)
                .unwrap_err()
                .code,
            "slot_dump_generation_mismatch"
        );

        let restore_engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("restore-cache"),
            dir.path().join("pages"),
            dir.path().join("restore-indexes"),
        );
        restore_engine.load_shard(1);
        restore_engine
            .install_slot_dump_manifest(&manifest)
            .expect("first generation should install");

        let mut fork = manifest.clone();
        let extra_slot = fork
            .slot_ids
            .iter()
            .copied()
            .max()
            .unwrap_or_default()
            .saturating_add(1);
        fork.slot_ids.push(extra_slot);
        fork.dump_generation_id = slot_dump_generation_id(&fork);
        fork.manifest_id = format!("{}-fork", fork.manifest_id);
        fork.checksum = slot_dump_manifest_checksum(&fork).unwrap();
        assert_eq!(
            restore_engine
                .install_slot_dump_manifest(&fork)
                .unwrap_err()
                .code,
            "slot_dump_generation_conflict"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_object_lifecycle_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "lifecycle".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_lifecycle = manifest.clone();
        stale_lifecycle.object_lifecycle.live_object_ids = stale_lifecycle
            .object_lifecycle
            .live_object_ids
            .saturating_add(1);
        stale_lifecycle.dump_generation_id = slot_dump_generation_id(&stale_lifecycle);
        stale_lifecycle.checksum = slot_dump_manifest_checksum(&stale_lifecycle).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_lifecycle)
                .unwrap_err()
                .code,
            "slot_dump_object_lifecycle_mismatch"
        );

        let mut reused_owner = manifest.clone();
        {
            let mut restored = serde_json::from_slice::<ShardState>(&reused_owner.index_bytes)
                .expect("manifest index should decode");
            let address = restored
                .strings
                .get_mut("lifecycle")
                .expect("manifest string address");
            address.object_id = Some(address.object_id.unwrap_or_default().wrapping_add(1));
            reused_owner.index_bytes = serde_json::to_vec(&restored).unwrap();
            reused_owner.index_sha256 = sha256_hex_bytes(&reused_owner.index_bytes);
            reused_owner.dump_generation_id = slot_dump_generation_id(&reused_owner);
            reused_owner.checksum = slot_dump_manifest_checksum(&reused_owner).unwrap();
        }
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&reused_owner)
                .unwrap_err()
                .code,
            "slot_dump_object_lifecycle_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_slot_summary_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "slot-summary".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_summary = manifest.clone();
        let summary = stale_summary
            .slot_summaries
            .first_mut()
            .expect("slot summary should exist");
        summary.page_ref_count = summary.page_ref_count.saturating_add(1);
        stale_summary.dump_generation_id = slot_dump_generation_id(&stale_summary);
        stale_summary.checksum = slot_dump_manifest_checksum(&stale_summary).unwrap();

        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_summary)
                .unwrap_err()
                .code,
            "slot_dump_slot_summary_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_byte_accounting_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "byte-accounting".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut stale_bytes = manifest.clone();
        stale_bytes.logical_bytes = stale_bytes.logical_bytes.saturating_add(1);
        stale_bytes.checksum = slot_dump_manifest_checksum(&stale_bytes).unwrap();

        assert_eq!(
            engine
                .validate_slot_dump_manifest(&stale_bytes)
                .unwrap_err()
                .code,
            "slot_dump_byte_accounting_mismatch"
        );
    }

    #[test]
    fn slot_dump_manifest_rejects_non_canonical_slot_and_page_segment_ids() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "canonical".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("manifest should persist");
        engine
            .validate_slot_dump_manifest(&manifest)
            .expect("fresh manifest should validate");

        let mut duplicate_slot = manifest.clone();
        duplicate_slot.slot_ids.push(
            duplicate_slot
                .slot_ids
                .first()
                .copied()
                .expect("slot id should exist"),
        );
        duplicate_slot.dump_generation_id = slot_dump_generation_id(&duplicate_slot);
        duplicate_slot.checksum = slot_dump_manifest_checksum(&duplicate_slot).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&duplicate_slot)
                .unwrap_err()
                .code,
            "slot_dump_slot_ids_not_canonical"
        );

        let mut duplicate_page_segment = manifest.clone();
        duplicate_page_segment.page_segment_ids.push(
            duplicate_page_segment
                .page_segment_ids
                .first()
                .copied()
                .expect("page segment id should exist"),
        );
        duplicate_page_segment.dump_generation_id =
            slot_dump_generation_id(&duplicate_page_segment);
        duplicate_page_segment.checksum =
            slot_dump_manifest_checksum(&duplicate_page_segment).unwrap();
        assert_eq!(
            engine
                .validate_slot_dump_manifest(&duplicate_page_segment)
                .unwrap_err()
                .code,
            "slot_dump_page_segment_ids_not_canonical"
        );
    }

    #[test]
    fn storage_lifecycle_plan_and_boundary_report_cover_dirty_and_orphan_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v1".to_vec(),
            },
        });
        engine.page_store().roll_segment().unwrap();
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v2".to_vec(),
            },
        });

        let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!plan.dirty_slots.is_empty());
        assert_eq!(plan.selected_dump_slots, plan.dirty_slots);
        assert!(plan.reasons.contains(&"dirty_slot_dump".to_string()));
        assert!(plan.stale_page_segment_ids.contains(&0));
        assert!(plan
            .reasons
            .contains(&"ranked_reclaim_candidates".to_string()));
        assert!(!plan.reclaim_candidates.is_empty());
        assert_eq!(plan.reclaim_candidates[0].page_segment_id, 0);
        assert_eq!(plan.reclaim_candidates[0].reason, "orphan_segment");
        assert!(plan.reclaim_candidates[0].stale_physical_bytes > 0);
        assert!(plan.reclaim_candidates[0].reclaim_score > 0);

        let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: plan.selected_dump_slots.clone(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(report.dump_manifest.is_some());
        assert_eq!(report.object_lifecycle.live_object_ids, 1);
        assert_eq!(report.object_lifecycle.live_page_refs, 1);
        assert_eq!(report.object_lifecycle.stale_object_ids, 1);
        let boundary = engine.storage_recovery_boundary_report(1);
        assert_eq!(boundary.latest_safe_oplog_sequence, 2);
        assert_eq!(boundary.latest_dump_oplog_sequence, 2);
        assert!(boundary.orphan_page_segment_ids.contains(&0));
    }

    #[test]
    fn storage_production_readiness_reports_warnings_without_blocking_dirty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "ready-key".to_string(),
                        value: b"ready-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report(1);
        assert!(report.production_ready, "{report:?}");
        assert!(report.blockers.is_empty());
        assert_eq!(report.dirty_slot_count, 1);
        assert!(report
            .warnings
            .contains(&"dirty_slots_pending_dump".to_string()));
        assert!(report.segment_integrity.integrity_ok);
        assert_eq!(report.segment_integrity.unreadable_page_ref_count, 0);
        assert_eq!(report.unreadable_page_ref_count, 0);
        assert_eq!(report.owner_mismatch_page_ref_count, 0);
        assert!(report.log_compatibility.rust_native_replay_safe);
        assert!(!report.log_compatibility.cxx_binary_compatible);
        assert_eq!(
            report.log_compatibility.oplog_format,
            "rust-jsonl-command-v1"
        );
        assert_eq!(
            report.log_compatibility.index_log_format,
            "rust-jsonl-shard-index-v1"
        );
        assert_eq!(
            report.log_compatibility.compatibility_mode,
            "rust_native_migration_only"
        );
        assert!(report.log_compatibility.migration_required);
        assert!(report.log_compatibility.golden_conversion_required);
        assert!(!report.log_compatibility.cxx_reader_supported);
        assert!(!report.log_compatibility.cxx_writer_supported);
        assert!(report.page_format_compatibility.rust_native_read_safe);
        assert!(!report.page_format_compatibility.cxx_page_header_compatible);
        assert_eq!(
            report.page_format_compatibility.page_format,
            "rust-page-envelope-v6"
        );
        assert_eq!(
            report.page_format_compatibility.compatibility_mode,
            "rust_envelope_migration_only"
        );
        assert!(report.page_format_compatibility.migration_required);
        assert!(report.page_format_compatibility.golden_conversion_required);
        assert!(
            !report
                .page_format_compatibility
                .cxx_page_header_reader_supported
        );
        assert!(
            !report
                .page_format_compatibility
                .cxx_page_header_writer_supported
        );
        assert!(report.page_format_compatibility.checksum_protected);
        assert!(report.page_format_compatibility.object_ids_embedded);
        assert!(report.page_store_bytes_written > 0);
    }

    #[test]
    fn storage_log_compatibility_report_counts_jsonl_sequences_and_cxx_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for index in 0..2 {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: format!("log-key-{index}"),
                            value: format!("log-value-{index}").into_bytes(),
                        },
                    })
                    .status
                    .ok
            );
        }

        let report = engine.storage_log_compatibility_report(1);
        assert_eq!(report.shard_id, 1);
        assert_eq!(report.compatibility_mode, "rust_native_migration_only");
        assert!(report.migration_required);
        assert!(!report.cxx_reader_supported);
        assert!(!report.cxx_writer_supported);
        assert!(report.golden_conversion_required);
        assert_eq!(report.oplog_last_sequence, 2);
        assert_eq!(report.index_log_last_sequence, 2);
        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert!(report.oplog_bytes > 0);
        assert!(report.index_log_bytes > 0);
        assert!(report.rust_native_replay_safe);
        assert!(!report.cxx_binary_compatible);
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| { gap.contains("migration-only") && gap.contains("binary log") }));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("C++ binary/protobuf oplog")));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("golden log conversion/replay")));
    }

    #[test]
    fn storage_page_format_compatibility_report_counts_zones_and_header_gaps() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "page-format-key".to_string(),
                        value: vec![11; 512],
                    },
                })
                .status
                .ok
        );
        engine.page_store().roll_segment().unwrap();

        let report = engine.storage_page_format_compatibility_report(1);
        assert_eq!(report.shard_id, 1);
        assert_eq!(report.page_format, "rust-page-envelope-v6");
        assert_eq!(report.rust_envelope_version, 6);
        assert_eq!(report.compatibility_mode, "rust_envelope_migration_only");
        assert!(report.migration_required);
        assert!(!report.cxx_page_header_reader_supported);
        assert!(!report.cxx_page_header_writer_supported);
        assert!(report.golden_conversion_required);
        assert!(report.rust_native_read_safe);
        assert!(!report.cxx_page_header_compatible);
        assert!(report.checksum_protected);
        assert!(report.object_ids_embedded);
        assert!(report.routing_slots_embedded);
        assert!(report.compression_supported);
        assert_eq!(report.sealed_zones, 1);
        assert_eq!(report.active_zones, 1);
        assert!(report.live_physical_bytes > 0);
        assert!(report.page_store_writes > 0);
        assert!(report.page_store_bytes_written > 0);
        assert!(report.logical_bytes_written >= 512);
        assert!(report.compressed_records_written > 0);
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("migration-only") && gap.contains("page-header")));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("C++ protobuf page header")));
        assert!(report
            .compatibility_gaps
            .iter()
            .any(|gap| gap.contains("golden page conversion/replay")));
    }

    #[test]
    fn storage_production_readiness_policy_can_block_dirty_dump_lag_and_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "policy-key".to_string(),
                        value: b"policy-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report_with_policy(
            1,
            StorageProductionReadinessPolicy {
                max_dirty_slots: Some(0),
                max_undumped_oplog_records: Some(0),
                require_slot_dump_manifest: true,
                ..StorageProductionReadinessPolicy::default()
            },
        );

        assert!(!report.production_ready, "{report:?}");
        assert_eq!(report.policy.max_dirty_slots, Some(0));
        assert_eq!(report.dirty_slot_count, 1);
        assert!(report.undumped_oplog_records > 0);
        assert!(report
            .blockers
            .contains(&"dirty_slots_exceed_policy".to_string()));
        assert!(report
            .blockers
            .contains(&"undumped_oplog_records_exceed_policy".to_string()));
        assert!(report
            .blockers
            .contains(&"slot_dump_manifest_required".to_string()));
    }

    #[test]
    fn storage_production_readiness_policy_can_promote_warnings_to_blockers() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "warn-key".to_string(),
                        value: b"warn-value".to_vec(),
                    },
                })
                .status
                .ok
        );

        let report = engine.storage_production_readiness_report_with_policy(
            1,
            StorageProductionReadinessPolicy {
                block_on_warnings: true,
                ..StorageProductionReadinessPolicy::default()
            },
        );

        assert!(!report.production_ready, "{report:?}");
        assert!(report
            .warnings
            .contains(&"dirty_slots_pending_dump".to_string()));
        assert!(report
            .blockers
            .contains(&"warnings_exceed_policy".to_string()));
    }

    #[test]
    fn storage_production_readiness_blocks_corrupt_live_page_segments() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "corrupt-key".to_string(),
                        value: b"corrupt-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        let segment_id = engine.live_page_segment_ids(1)[0];
        let mut bytes = engine.page_store().read_segment(segment_id).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        engine
            .page_store()
            .install_segment(segment_id, &bytes)
            .unwrap();

        let report = engine.storage_production_readiness_report(1);
        assert!(!report.production_ready, "{report:?}");
        assert!(report
            .blockers
            .contains(&"corrupt_page_segments".to_string()));
        assert!(report
            .blockers
            .contains(&"unreadable_live_page_refs".to_string()));
        assert!(report
            .blockers
            .contains(&"storage_segment_integrity_failed".to_string()));
        assert!(!report.segment_integrity.integrity_ok);
        assert!(report.segment_integrity.corrupt_page_segment_count > 0);
        assert!(report.segment_integrity.unreadable_page_ref_count > 0);
        assert!(report.corrupt_page_segment_count > 0);
        assert!(report.unreadable_page_ref_count > 0);
    }

    #[test]
    fn storage_lifecycle_apply_warms_cache_from_page_index() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            32,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "warm-me".to_string(),
                value: vec![7; 128],
            },
        });
        engine.cache().invalidate_shard(1).unwrap();
        let plan = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        let report = engine.apply_storage_lifecycle(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: plan.selected_dump_slots,
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 0,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: true,
        });
        assert!(report.cache_warmup_page_refs >= 1);
        assert_eq!(
            report.cache_warmup.warmed_page_refs,
            report.cache_warmup_page_refs
        );
        assert!(report.cache_warmup.considered_page_refs >= 1);
        assert!(report.cache_warmup.page_store_reads >= 1);
        assert!(report.cache_warmup.warmed_bytes >= 128);
        assert_eq!(report.cache_warmup.failed_page_refs, 0);
        assert!(engine.cache().stats().puts >= 1);
    }

    // shared-corpus: storage_byteraft_dump_load_atomicity storage_byteraft_cache_refill_pressure storage_manager_real_pressure_signals storage_manager_metrics_admin_phase_reports
    #[test]
    fn storage_manager_cycle_reports_cpp_order_without_mutating_on_dry_run() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "manager-dry-run".to_string(),
                value: b"v1".to_vec(),
            },
        });
        let before_manifests = engine.list_slot_dump_manifests(1).len();

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            dry_run: true,
            max_dump_slots_per_round: 8,
            ..StorageManagerCycleRequest::default()
        });

        assert!(report.completed, "{report:#?}");
        assert!(report.production_parity_slice, "{report:#?}");
        assert_eq!(
            report.cxx_stage_order,
            vec![
                "prepare",
                "reclaim_oplog",
                "expire",
                "evict",
                "reclaim_page",
                "index_gc",
                "compact",
                "reap_metrics",
            ]
        );
        assert_eq!(report.stages.len(), report.cxx_stage_order.len());
        for stage in &report.stages {
            assert!(stage.last_run_unix_ms > 0, "{stage:#?}");
            assert!(stage.errors.is_empty(), "{stage:#?}");
            if stage.skipped {
                assert!(!stage.skipped_reason.is_empty(), "{stage:#?}");
            }
        }
        assert!(report.plan.dirty_slots.len() >= 1);
        assert!(report.pressure_signals.dirty_slot_count >= 1);
        assert!(report.pressure_signals.wal_bytes > 0);
        assert!(report.pressure_signals.index_log_bytes > 0);
        assert!(report.pressure_signals.total_pressure_score > 0);
        let prepare = report
            .stages
            .iter()
            .find(|stage| stage.stage == "prepare")
            .expect("prepare stage");
        assert_eq!(
            prepare.pressure_signal,
            "dirty_slots+wal_bytes+index_log_bytes+stale_density+cache_pressure+expire_debt+delayed_destroy+retention_blockers+model_compaction_debt"
        );
        assert!(prepare.pressure_triggered, "{report:#?}");
        assert_eq!(
            prepare.pressure_score,
            report.pressure_signals.total_pressure_score
        );
        assert_eq!(engine.list_slot_dump_manifests(1).len(), before_manifests);
        assert!(report.lifecycle_report.is_none());
        assert!(report.compaction_report.is_none());
        assert!(report.merged_dump_load_policy.dry_run);
        assert!(report.merged_dump_load_policy.production_slice_ready);
    }

    // shared-corpus: storage_byteraft_dump_load_atomicity storage_byteraft_cache_refill_pressure storage_gc_eviction_cold_reads storage_manager_real_pressure_signals
    #[test]
    fn storage_manager_cycle_applies_dump_expire_evict_reclaim_index_gc_and_compact() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            128,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for idx in 0..4 {
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: format!("manager-live-{idx}"),
                    value: format!("value-{idx}").into_bytes(),
                },
            });
        }
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "manager-expire".to_string(),
                value: b"gone".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::CommonExpire {
                key: "manager-expire".to_string(),
                ttl_ms: 0,
            },
        });
        let _ = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "manager-live-0".to_string(),
            },
        });
        let compact_before = engine.compact_shard_pages(1).unwrap();
        assert!(compact_before.rewritten_page_refs >= 1);
        assert!(!compact_before.stale_page_segment_ids.is_empty());

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            max_dump_slots_per_round: 16,
            warm_cache: true,
            max_expire_hot_slots_per_round: 8,
            max_expire_cold_slots_per_round: 8,
            load_cold_slots_for_expire: true,
            ..StorageManagerCycleRequest::default()
        });

        assert!(report.completed, "{report:#?}");
        assert!(report.production_parity_slice, "{report:#?}");
        assert!(
            report.merged_dump_load_policy.production_slice_ready,
            "{report:#?}"
        );
        assert!(report.pressure_signals.dirty_slot_count >= 1);
        assert!(report.pressure_signals.undumped_wal_records >= 1);
        assert!(report.pressure_signals.wal_bytes > 0);
        assert!(report.pressure_signals.index_log_bytes > 0);
        assert!(report.pressure_signals.stale_page_bytes > 0);
        assert!(
            report
                .pressure_signals
                .page_segment_stale_density_basis_points
                > 0
        );
        assert!(report.pressure_signals.memory_cache_pressure_score > 0);
        assert!(report.pressure_signals.expired_slot_object_scan_debt >= 1);
        assert!(report.pressure_signals.compaction_debt_model_count >= 1);
        assert!(report.pressure_signals.compaction_debt_score > 0);
        assert!(report.pressure_signals.total_pressure_score > 0);
        assert!(report.merged_dump_load_policy.manifest_checksum_validated);
        assert!(report.merged_dump_load_policy.manifest_generation_validated);
        assert!(report.merged_dump_load_policy.sequence_boundaries_validated);
        assert!(report.merged_dump_load_policy.page_segments_validated);
        assert!(report.merged_dump_load_policy.live_page_refs_validated);
        assert!(report.merged_dump_load_policy.object_lifecycle_validated);
        assert!(report.merged_dump_load_policy.install_preflight_safe);
        assert!(report.merged_dump_load_policy.blockers.is_empty());
        assert!(report.lifecycle_report.is_some());
        assert!(report
            .lifecycle_report
            .as_ref()
            .and_then(|lifecycle| lifecycle.dump_manifest.as_ref())
            .is_some());
        let reclaim_oplog = report
            .stages
            .iter()
            .find(|stage| stage.stage == "reclaim_oplog")
            .expect("reclaim_oplog stage");
        assert!(reclaim_oplog.dumped_slot_count >= 1, "{report:#?}");
        assert_eq!(
            reclaim_oplog.pressure_signal,
            "durable_slot_generation_frontier+follower_snapshot_retention+wal_bytes+index_log_bytes"
        );
        assert!(reclaim_oplog.pressure_triggered, "{report:#?}");
        assert!(report.wal_reclaim_report.is_some(), "{report:#?}");
        let wal_reclaim = report.wal_reclaim_report.as_ref().unwrap();
        assert!(wal_reclaim.plan.safe_to_reclaim, "{report:#?}");
        assert!(wal_reclaim.applied, "{report:#?}");
        assert!(wal_reclaim.oplog_records_removed >= 1, "{report:#?}");
        let index_gc_report = report.index_gc_report.as_ref().expect("index GC report");
        assert!(index_gc_report.applied, "{report:#?}");
        assert!(index_gc_report.records_removed >= 1, "{report:#?}");
        assert!(index_gc_report.dirty_slots_committed_before_truncate);
        assert_eq!(
            reclaim_oplog.retain_from_wal_sequence,
            wal_reclaim.plan.retain_from_oplog_sequence
        );
        assert_eq!(
            reclaim_oplog.retain_from_index_log_sequence,
            wal_reclaim.plan.retain_from_index_log_sequence
        );
        assert_eq!(
            reclaim_oplog.wal_floor_sequence,
            wal_reclaim.plan.retain_from_oplog_sequence
        );
        assert_eq!(
            reclaim_oplog.index_log_floor_sequence,
            wal_reclaim.plan.retain_from_index_log_sequence
        );
        let expire = report
            .stages
            .iter()
            .find(|stage| stage.stage == "expire")
            .expect("expire stage");
        assert_eq!(expire.expired_records_removed, 1, "{report:#?}");
        assert_eq!(
            expire.pressure_signal,
            "expired_hot_slots+cold_slots+scan_cursors+load_on_expire_debt"
        );
        assert!(expire.candidate_count >= 1, "{report:#?}");
        assert!(expire.pressure_triggered, "{report:#?}");
        let expiry_report = report.expiry_report.as_ref().expect("expiry report");
        assert!(expiry_report.hot_slots_scanned >= 1);
        assert!(expiry_report.scanned_records >= 1);
        assert!(expiry_report.load_on_expire_only_when_needed);
        let evict = report
            .stages
            .iter()
            .find(|stage| stage.stage == "evict")
            .expect("evict stage");
        assert!(evict.cache_entries_removed >= 1, "{report:#?}");
        assert_eq!(
            evict.pressure_signal,
            "weighted_slot_object_eviction+memory_pressure_gate+batch_limit"
        );
        assert!(evict.pressure_triggered, "{report:#?}");
        assert!(evict.pressure_before > evict.pressure_after, "{report:#?}");
        assert!(evict.bytes_reclaimed >= evict.cache_disk_bytes_removed);
        assert!(report.eviction_report.is_some(), "{report:#?}");
        let eviction_report = report.eviction_report.as_ref().unwrap();
        assert!(eviction_report.pressure_gate_open, "{report:#?}");
        assert!(eviction_report.pressure_after < eviction_report.pressure_before);
        assert!(!eviction_report.selected_victims.is_empty());
        assert!(!eviction_report.cooldown);
        let reclaim_page = report
            .stages
            .iter()
            .find(|stage| stage.stage == "reclaim_page")
            .expect("reclaim_page stage");
        assert!(reclaim_page.page_segments_reclaimed >= 1, "{report:#?}");
        assert_eq!(
            reclaim_page.pressure_signal,
            "stale_page_bytes+delayed_destroy_backlog+stale_density+dependency_retention"
        );
        assert!(reclaim_page.candidate_count >= reclaim_page.page_segments_reclaimed);
        assert!(reclaim_page.bytes_reclaimed >= reclaim_page.page_bytes_reclaimed);
        let index_gc = report
            .stages
            .iter()
            .find(|stage| stage.stage == "index_gc")
            .expect("index_gc stage");
        assert_eq!(
            index_gc.pressure_signal,
            "obsolete_manifests+install_markers+index_log_bytes+usage_ratio+max_entries"
        );
        assert_eq!(
            index_gc.index_log_records_removed,
            index_gc_report.records_removed
        );
        let compact = report
            .stages
            .iter()
            .find(|stage| stage.stage == "compact")
            .expect("compact stage");
        assert_eq!(
            compact.pressure_signal,
            "model_layout_compaction_debt+stale_segment_density"
        );
        assert_eq!(compact.pages_compacted, compact.rewritten_page_refs);
        assert!(compact.pressure_before >= compact.pressure_after);
        let metrics = report
            .stages
            .iter()
            .find(|stage| stage.stage == "reap_metrics")
            .expect("reap_metrics stage");
        assert!(metrics.metrics_slot_count >= 1);
        assert!(metrics.metrics_page_ref_count >= 1);
        assert_eq!(metrics.pressure_signal, "slot_page_cache_metrics");
        assert!(metrics.before_bytes >= metrics.live_bytes);

        for idx in 0..4 {
            let key = format!("manager-live-{idx}");
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key },
            });
            assert_eq!(
                response.response,
                CommandResponse::Bytes {
                    value: Some(format!("value-{idx}").into_bytes())
                },
                "live record should remain readable after StorageManager eviction and GC: {response:?}"
            );
        }
        let expired = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "manager-expire".to_string(),
            },
        });
        assert_eq!(
            expired.response,
            CommandResponse::Bytes { value: None },
            "expired record should stay removed after StorageManager eviction and GC"
        );
    }

    // shared-corpus: storage_manager_index_gc_thresholds_recovery
    #[test]
    fn storage_manager_index_gc_thresholds_budget_dirty_commit_and_restart_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache-a");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let engine = TemporalEngine::with_local_dirs(1024, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        for idx in 0..5 {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: "index-gc-key".to_string(),
                            value: format!("value-{idx}").into_bytes(),
                        },
                    })
                    .status
                    .ok
            );
        }
        assert_eq!(engine.index_log_store().stats(1).last_sequence, 5);

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_prepare: true,
            enable_oplog_reclaim: true,
            enable_evict: false,
            enable_expire: false,
            enable_page_reclaim: false,
            enable_page_compaction: false,
            enable_index_gc: true,
            max_dump_slots_per_round: 8,
            index_gc_index_log_bytes_threshold: 1,
            index_gc_usage_ratio_trigger_basis_points: 1,
            index_gc_max_entries_per_round: 2,
            index_gc_commit_dirty_slots_before_truncation: true,
            ..StorageManagerCycleRequest::default()
        });

        assert!(report.completed, "{report:#?}");
        assert!(report
            .lifecycle_report
            .as_ref()
            .and_then(|lifecycle| lifecycle.dump_manifest.as_ref())
            .is_some());
        let index_gc = report.index_gc_report.as_ref().expect("index GC report");
        assert!(index_gc.applied, "{report:#?}");
        assert!(index_gc.threshold_triggered, "{report:#?}");
        assert!(index_gc.usage_ratio_triggered, "{report:#?}");
        assert!(index_gc.safe_to_truncate, "{report:#?}");
        assert!(index_gc.dirty_slots_committed_before_truncate);
        assert_eq!(index_gc.max_entries_per_round, 2);
        assert_eq!(index_gc.records_removed, 2);
        assert!(index_gc.budget_exhausted, "{report:#?}");
        assert!(index_gc.bytes_after < index_gc.bytes_before, "{report:#?}");
        let stage = report
            .stages
            .iter()
            .find(|stage| stage.stage == "index_gc")
            .expect("index_gc stage");
        assert_eq!(
            stage.pressure_signal,
            "obsolete_manifests+install_markers+index_log_bytes+usage_ratio+max_entries"
        );
        assert_eq!(stage.index_log_records_removed, 2);
        assert_eq!(stage.before_bytes, index_gc.bytes_before);
        assert_eq!(stage.after_bytes, index_gc.bytes_after);

        let restarted = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache-b"),
            &page_dir,
            &index_dir,
        );
        restarted.load_shard(1);
        let response = restarted.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "index-gc-key".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"value-4".to_vec())
            }
        );
        let boundary = restarted.storage_recovery_boundary_report(1);
        assert!(
            boundary.corrupt_page_segment_ids.is_empty(),
            "{boundary:#?}"
        );
        assert!(boundary.stale_index_page_refs.is_empty(), "{boundary:#?}");
    }

    // shared-corpus: storage_manager_page_gc_dependency_refusal
    #[test]
    fn storage_manager_page_gc_refuses_reclaim_with_retained_dependencies() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "page-gc-key".to_string(),
                        value: b"v1".to_vec(),
                    },
                })
                .status
                .ok
        );
        let live_segment = engine.live_page_segment_ids(1)[0];
        let live_plan = engine.storage_page_gc_dependency_plan(
            1,
            [live_segment],
            Vec::<StoragePageGcReplayCursor>::new(),
            Vec::<SlotDumpRaftSnapshotRef>::new(),
            None,
            None,
            0,
        );
        assert!(!live_plan.safe_to_reclaim, "{live_plan:#?}");
        assert!(live_plan
            .dependency_blocks
            .iter()
            .any(|block| block.dependency == "live_page_ref"));

        let manifest = engine.create_slot_dump_manifest(1, Vec::new()).unwrap();
        assert_eq!(manifest.page_segment_ids, vec![live_segment]);
        engine.page_store().roll_segment().unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "page-gc-key".to_string(),
                        value: b"v2".to_vec(),
                    },
                })
                .status
                .ok
        );
        let current_live = engine.live_page_segment_ids(1);
        assert_ne!(current_live, vec![live_segment]);
        let delayed = engine
            .page_store()
            .gc_segments_before_with_live_refs_delayed_destroy(
                live_segment.saturating_add(1),
                current_live.clone(),
            )
            .unwrap();
        assert_eq!(delayed.delayed_destroy_page_segment_ids, vec![live_segment]);

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_prepare: true,
            enable_oplog_reclaim: false,
            enable_evict: false,
            enable_expire: false,
            enable_page_reclaim: true,
            enable_page_compaction: false,
            enable_index_gc: false,
            page_gc_shared_store_cursors: vec![StoragePageGcReplayCursor {
                cursor_id: "shared-follower-a".to_string(),
                shard_id: 1,
                retain_from_page_segment_id: live_segment,
                reason: "shared replay cursor retained old page segment".to_string(),
            }],
            page_gc_checkpoint_floor_segment_id: Some(live_segment),
            page_gc_raft_install_floor_segment_id: Some(live_segment),
            page_gc_delayed_destroy_grace_ms: 60_000,
            ..StorageManagerCycleRequest::default()
        });

        assert!(report.completed, "{report:#?}");
        assert!(
            !report.page_gc_dependency_plan.safe_to_reclaim,
            "{report:#?}"
        );
        assert_eq!(
            report.page_gc_dependency_plan.blocked_page_segment_ids,
            vec![live_segment]
        );
        let dependencies = report
            .page_gc_dependency_plan
            .dependency_blocks
            .iter()
            .map(|block| block.dependency.as_str())
            .collect::<BTreeSet<_>>();
        assert!(dependencies.contains("slot_dump_manifest"), "{report:#?}");
        assert!(
            dependencies.contains("shared_store_replay_cursor"),
            "{report:#?}"
        );
        assert!(
            dependencies.contains("checkpoint_snapshot_floor"),
            "{report:#?}"
        );
        assert!(
            dependencies.contains("raft_snapshot_install_floor"),
            "{report:#?}"
        );
        assert!(
            dependencies.contains("delayed_destroy_grace_period"),
            "{report:#?}"
        );
        assert!(report
            .page_gc_dependency_plan
            .manifest_page_segment_ids
            .contains(&live_segment));
        let reclaim_page = report
            .stages
            .iter()
            .find(|stage| stage.stage == "reclaim_page")
            .expect("reclaim_page stage");
        assert!(reclaim_page.skipped, "{report:#?}");
        assert!(!reclaim_page.applied, "{report:#?}");
        assert!(reclaim_page
            .reason
            .contains("page GC refused because retained dependencies remain"));
        assert_eq!(reclaim_page.page_segments_reclaimed, 0);
        assert!(engine
            .page_store()
            .delayed_destroy_segment_ids()
            .unwrap()
            .contains(&live_segment));
        assert_eq!(
            engine
                .list_slot_dump_manifests(1)
                .first()
                .unwrap()
                .page_segment_ids,
            vec![live_segment]
        );
    }

    // shared-corpus: storage_manager_active_eviction_runtime
    #[test]
    fn storage_manager_active_eviction_supports_weighted_dump_drop_batch_and_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            256,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for idx in 0..4 {
            let key = format!("evict-runtime-{idx}");
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.clone(),
                    value: vec![idx as u8; 96],
                },
            });
            let _ = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet { key },
            });
        }
        let before = engine.storage_cache_inspection_report(1);
        assert!(before.stats.memory_bytes > 0 || before.stats.disk_bytes > 0);

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_expire: false,
            enable_oplog_reclaim: false,
            enable_page_reclaim: false,
            enable_page_compaction: false,
            enable_index_gc: false,
            eviction_memory_pressure_threshold: 1,
            eviction_batch_limit: 1,
            eviction_dump_before_evict: true,
            ..StorageManagerCycleRequest::default()
        });
        let eviction = report.eviction_report.as_ref().expect("eviction report");
        assert!(eviction.pressure_gate_open, "{report:#?}");
        assert_eq!(eviction.batch_limit, 1);
        assert_eq!(eviction.selected_victims.len(), 1);
        assert_eq!(eviction.mode, "evict_cache");
        assert!(!eviction.dump_manifest_ids.is_empty(), "{report:#?}");
        assert!(eviction.cache_entries_removed > 0 || eviction.cache_disk_bytes_removed > 0);
        assert!(
            eviction.pressure_after < eviction.pressure_before,
            "{report:#?}"
        );

        let drop_report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 1,
            enable_expire: false,
            enable_oplog_reclaim: false,
            enable_page_reclaim: false,
            enable_page_compaction: false,
            enable_index_gc: false,
            eviction_memory_pressure_threshold: 1,
            eviction_batch_limit: 1,
            eviction_delete_drop: true,
            ..StorageManagerCycleRequest::default()
        });
        let drop_eviction = drop_report.eviction_report.as_ref().unwrap();
        assert_eq!(drop_eviction.mode, "delete_drop");
        assert!(drop_eviction.dropped_object_count >= 1, "{drop_report:#?}");

        let cooldown = engine.apply_storage_eviction(1, u64::MAX, 1, false, false);
        assert!(!cooldown.pressure_gate_open);
        assert_eq!(cooldown.skipped_reason, "memory_pressure_below_threshold");
    }

    #[test]
    fn storage_cache_warmup_report_filters_slots_and_counts_cache_hits() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let first_key = "warm-slot-a";
        let first_slot = engine.routing_slot_for_key(1, first_key);
        let second_key = (0..100)
            .map(|index| format!("warm-slot-b-{index}"))
            .find(|key| engine.routing_slot_for_key(1, key) != first_slot)
            .expect("test should find a key in another slot");
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: first_key.to_string(),
                value: b"value-a".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: second_key,
                value: b"value-b".to_vec(),
            },
        });
        engine.cache().invalidate_shard(1).unwrap();

        let slot = first_slot;
        let first = engine.storage_cache_warmup_report(1, [slot]);
        assert_eq!(first.selected_slots, vec![slot]);
        assert_eq!(first.considered_page_refs, 1);
        assert_eq!(first.skipped_page_refs, 1);
        assert_eq!(first.page_store_reads, 1);
        assert_eq!(first.already_cached_page_refs, 0);
        assert_eq!(first.failed_page_refs, 0);
        assert!(first.warmed_bytes > 0);

        let second = engine.storage_cache_warmup_report(1, [slot]);
        assert_eq!(second.considered_page_refs, 1);
        assert_eq!(second.skipped_page_refs, 1);
        assert_eq!(second.page_store_reads, 0);
        assert_eq!(second.already_cached_page_refs, 1);
        assert_eq!(second.warmed_page_refs, 1);
    }

    #[test]
    fn storage_cache_inspection_reports_slot_entries_and_invalidates_slot() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        let key = "slot-cache-key";
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"slot-cache-value".to_vec(),
                    },
                })
                .status
                .ok
        );
        engine.cache().invalidate_shard(1).unwrap();
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: key.to_string(),
                    },
                })
                .status
                .ok
        );

        let slot = engine.routing_slot_for_key(1, key);
        let report = engine.storage_cache_inspection_report(1);
        assert!(report.stats.disk_fills >= 1);
        assert!(report
            .entries
            .iter()
            .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
        assert!(report
            .slot_summaries
            .iter()
            .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));

        let invalidated = engine
            .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
                shard_id: 1,
                routing_slot: slot,
            })
            .unwrap();
        assert!(invalidated.memory_entries_removed >= 1);
        let after = engine.storage_cache_inspection_report(1);
        assert!(!after
            .entries
            .iter()
            .any(|entry| entry.selector.starts_with(&format!("slot-{slot}:"))));
    }

    #[test]
    fn tiny_cache_dump_load_restart_refills_from_disk_block_cache() {
        let dir = tempfile::tempdir().unwrap();
        let cache_dir = dir.path().join("cache");
        let page_dir = dir.path().join("pages");
        let index_dir = dir.path().join("indexes");
        let restore_index_dir = dir.path().join("restore-indexes");
        let engine = TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &index_dir);
        engine.load_shard(1);
        let target_value = b"dump-load-target-1234".to_vec();
        for (key, value) in [
            ("target", target_value.clone()),
            ("churn-a", b"cache-churn-a-1234".to_vec()),
            ("churn-b", b"cache-churn-b-1234".to_vec()),
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringSet {
                            key: key.to_string(),
                            value,
                        },
                    })
                    .status
                    .ok
            );
        }
        let target_page_key = {
            let shards = engine.shards.read().expect("shards lock poisoned");
            let address = shards
                .get(&1)
                .unwrap()
                .strings
                .get("target")
                .unwrap()
                .clone();
            CacheKey::page_with_slot(
                1,
                address.page_segment_id,
                address.offset,
                address.length,
                address.routing_slot,
            )
        };

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "target".to_string(),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(target_value.clone())
            }
        );
        for key in ["churn-a", "churn-b"] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::StringGet {
                            key: key.to_string(),
                        },
                    })
                    .status
                    .ok
            );
        }
        assert!(engine.cache().stats().memory_evictions > 0);
        assert!(engine.cache().stats().disk_bytes > 0);
        assert_eq!(engine.cache().get_memory(&target_page_key), None);
        let manifest = engine
            .create_slot_dump_manifest(1, Vec::new())
            .expect("slot dump manifest should persist");
        engine.validate_slot_dump_manifest(&manifest).unwrap();

        let restored =
            TemporalEngine::with_local_dirs(32, &cache_dir, &page_dir, &restore_index_dir);
        restored.load_shard(1);
        restored
            .install_slot_dump_manifest(&manifest)
            .expect("slot dump should install after restart");
        let page_reads_before = restored.page_store().stats().reads;
        let disk_hits_before = restored.cache().stats().disk_hits;
        let response = restored.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "target".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(target_value)
            }
        );
        assert_eq!(
            restored.page_store().stats().reads,
            page_reads_before,
            "restored engine should refill from disk block cache before page store"
        );
        assert!(restored.cache().stats().disk_hits > disk_hits_before);

        let slot = restored.routing_slot_for_key(1, "target");
        let cache_report = restored.storage_cache_inspection_report(1);
        assert!(cache_report
            .slot_summaries
            .iter()
            .any(|summary| summary.routing_slot == slot && summary.entry_count >= 1));
        let invalidated = restored
            .invalidate_storage_cache_slot(StorageCacheInvalidateSlotRequest {
                shard_id: 1,
                routing_slot: slot,
            })
            .unwrap();
        assert!(invalidated.memory_entries_removed >= 1);
        let readiness = restored.storage_production_readiness_report(1);
        assert!(readiness.production_ready, "{readiness:?}");
        assert_eq!(readiness.unreadable_page_ref_count, 0);
        assert_eq!(readiness.corrupt_page_segment_count, 0);
    }

    #[test]
    fn storage_lifecycle_plan_matches_cpp_delayed_and_limited_dirty_slot_dump_policy() {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        for i in 0..128 {
            let key = format!("slot-{i}");
            engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.clone(),
                    value: key.as_bytes().to_vec(),
                },
            });
            let observed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
                shard_id: 1,
                selected_dump_slots: Vec::new(),
                max_dump_slots_per_round: 0,
                min_undumped_oplog_records: 0,
                purge_delayed_destroy: false,
                prune_slot_dump_manifests: false,
                roll_forward_slot_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                page_gc_shared_store_cursors: Vec::new(),
                page_gc_raft_snapshot_refs: Vec::new(),
                page_gc_checkpoint_floor_segment_id: None,
                page_gc_raft_install_floor_segment_id: None,
                page_gc_delayed_destroy_grace_ms: 0,
                invalidate_cache: false,
                warm_cache: false,
            });
            if observed.dirty_slots.len() >= 3 {
                break;
            }
        }

        let delayed = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 99,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(delayed.dump_delayed);
        assert!(delayed.selected_dump_slots.is_empty());
        assert!(delayed
            .reasons
            .contains(&"dirty_slot_dump_delayed".to_string()));

        let limited = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: Vec::new(),
            max_dump_slots_per_round: 2,
            min_undumped_oplog_records: 1,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!limited.dump_delayed);
        assert!(limited.undumped_oplog_records >= 3);
        assert_eq!(limited.selected_dump_slots.len(), 2);
        assert!(limited.dirty_slots.len() >= limited.selected_dump_slots.len());

        let explicit = engine.storage_lifecycle_plan(StorageLifecycleRequest {
            shard_id: 1,
            selected_dump_slots: vec![delayed.dirty_slots[0]],
            max_dump_slots_per_round: 0,
            min_undumped_oplog_records: 99,
            purge_delayed_destroy: false,
            prune_slot_dump_manifests: false,
            roll_forward_slot_dump_installs: false,
            follower_replay_cursors: Vec::new(),
            page_gc_shared_store_cursors: Vec::new(),
            page_gc_raft_snapshot_refs: Vec::new(),
            page_gc_checkpoint_floor_segment_id: None,
            page_gc_raft_install_floor_segment_id: None,
            page_gc_delayed_destroy_grace_ms: 0,
            invalidate_cache: false,
            warm_cache: false,
        });
        assert!(!explicit.dump_delayed);
        assert_eq!(explicit.selected_dump_slots, vec![delayed.dirty_slots[0]]);
    }
}
