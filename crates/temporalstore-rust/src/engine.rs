use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::cache::{CacheKey, CacheStats, MultiLayerCache};
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
use crate::page_store::{
    LocalPageStore, PageAddress, PageStoreError, PageStoreOptions, PageStoreSegmentReport,
    PageStoreStats, PageStoreZoneDescriptor,
};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, FeatureFilter, FeatureFilterOp, FeaturePoint, FeatureWritePolicy, IpsStats,
    RiskFamily, RiskFolType, SequenceFeatureRow, SequenceQuerySpec, ShardId, Status,
    StringSetCondition,
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

#[derive(Debug, Default, Serialize, Deserialize)]
struct ShardState {
    expires_at_ms: HashMap<String, u64>,
    strings: HashMap<String, PageAddress>,
    hashes: HashMap<String, HashMap<String, PageAddress>>,
    sets: HashMap<String, BTreeMap<Vec<u8>, PageAddress>>,
    features: HashMap<String, BTreeMap<u64, PageAddress>>,
    sequences: HashMap<String, BTreeMap<u64, PageAddress>>,
    ips: HashMap<String, BTreeMap<u64, PageAddress>>,
    #[serde(default)]
    ips_meta: HashMap<String, BTreeMap<u64, IpsPointMeta>>,
    #[serde(default)]
    ips_request_ids: HashMap<String, BTreeSet<String>>,
    risk: HashMap<String, BTreeMap<u64, i64>>,
    #[serde(default)]
    risk_changes: HashMap<String, BTreeMap<u64, BTreeSet<Vec<u8>>>>,
    #[serde(default)]
    risk_fol: HashMap<String, RiskFolValue>,
    #[serde(skip)]
    dirty_objects: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RiskFolValue {
    occur_time_ms: u64,
    value: Vec<u8>,
    fol_type: RiskFolType,
}

#[derive(Debug, Default, Clone)]
struct AdmissionState {
    window_epoch_sec: u64,
    read_count: u64,
    write_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum AdmissionScope {
    Shard(ShardId),
    Table(String),
    Tenant(String),
}

struct AdmissionLimit {
    scope: AdmissionScope,
    limit: u64,
    label: &'static str,
}

const FEATURE_ADD_HARD_MAX_SIZE: usize = 100_000;
const HOT_PAGE_SEGMENT_ID: u64 = u64::MAX;
static HOT_PAGE_OFFSET: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IpsPointMeta {
    address: PageAddress,
    action_type: Option<u32>,
    table_id: Option<u64>,
    request_id: Option<String>,
}

struct ExecuteOutcome {
    response: CommandResponse,
    mutated: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardCompactionReport {
    pub shard_id: ShardId,
    pub previous_page_segment_id: u64,
    pub compacted_page_segment_id: u64,
    pub rewritten_page_refs: usize,
    pub stale_page_segment_ids: Vec<u64>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardExpirySweepReport {
    pub shard_id: ShardId,
    pub expired_records_removed: usize,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustStorageObservation {
    pub shard_id: ShardId,
    pub cache: CacheStats,
    pub page_store: PageStoreStats,
    pub observed_memory_hit: bool,
    pub observed_block_cache_hit: bool,
    pub observed_local_file_read: bool,
    pub observed_cache_invalidation: bool,
    pub observed_memory_eviction: bool,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    pub local_page_bytes_written: u64,
    pub local_page_bytes_read: u64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryReport {
    pub shard_id: ShardId,
    pub index_bytes: u64,
    pub oplog_records: usize,
    pub index_log_records: usize,
    pub active_page_segment_ids: Vec<u64>,
    pub live_page_segment_ids: Vec<u64>,
    pub zone_descriptors: Vec<PageStoreZoneDescriptor>,
    #[serde(default)]
    pub page_segment_reports: Vec<PageStoreSegmentReport>,
    #[serde(default)]
    pub page_segment_live_reports: Vec<StorageRecoverySegmentLiveReport>,
    pub total_page_refs: usize,
    pub readable_page_refs: usize,
    #[serde(default)]
    pub unreadable_page_refs: Vec<StorageRecoveryPageError>,
    pub all_live_pages_readable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoveryPageError {
    pub page_segment_id: u64,
    pub offset: u64,
    pub length: u64,
    pub error: String,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageRecoverySegmentLiveReport {
    pub page_segment_id: u64,
    pub physical_bytes: u64,
    pub logical_bytes: u64,
    pub page_count: u64,
    pub live_page_refs: u64,
    pub readable_live_page_refs: u64,
    pub unreadable_live_page_refs: u64,
    pub stale_page_estimate: u64,
    pub live_physical_bytes: u64,
    pub live_logical_bytes: u64,
    pub live_object_count: u64,
    pub live_routing_slot_count: u64,
    pub live_ref_density_basis_points: u64,
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
        out.push_str(
            "# HELP temporalstore_partition_routing_slots Routing slots owned by shard.\n",
        );
        out.push_str("# TYPE temporalstore_partition_routing_slots gauge\n");
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
            push_metric(
                &mut out,
                "temporalstore_partition_routing_slots",
                &[("shard_id", stats.shard_id.to_string())],
                stats.object_manager.routing_slot_count as u64,
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
        let page_segment_reports = self.page_store.segment_reports().unwrap_or_default();
        let shards = self.shards.read().expect("engine lock poisoned");
        let addresses = shards
            .get(&shard_id)
            .map(collect_live_page_addresses)
            .unwrap_or_default();
        let total_page_refs = addresses.len();
        let mut readable_page_refs = 0usize;
        let mut unreadable_page_refs = Vec::new();
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
            oplog_records,
            index_log_records,
            active_page_segment_ids,
            live_page_segment_ids,
            zone_descriptors,
            page_segment_reports,
            page_segment_live_reports,
            total_page_refs,
            readable_page_refs,
            unreadable_page_refs,
            all_live_pages_readable: total_page_refs == readable_page_refs,
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
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let now = now_ms();
        let expired_keys = shard
            .expires_at_ms
            .iter()
            .filter_map(|(key, expires_at)| (*expires_at <= now).then(|| key.clone()))
            .collect::<Vec<_>>();
        let mut expired_records_removed = 0;
        for key in expired_keys {
            if delete_record(shard, &key) {
                invalidate_record_all(&self.cache, shard_id, &key);
                expired_records_removed += 1;
            }
        }
        if expired_records_removed > 0 {
            let index_bytes = serde_json::to_vec_pretty(shard)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            self.persist_index_bytes(shard_id, &index_bytes)
                .map_err(|err| Status::error("expire_sweep_failed", err.to_string()))?;
            let _ = self.index_log_store.append_json(shard_id, &index_bytes);
        }
        Ok(ShardExpirySweepReport {
            shard_id,
            expired_records_removed,
        })
    }

    pub fn sweep_all_expired_records(&self) -> Vec<ShardExpirySweepReport> {
        self.loaded_shard_ids()
            .into_iter()
            .filter_map(|shard_id| self.sweep_expired_records(shard_id).ok())
            .collect()
    }

    pub fn compact_shard_pages(&self, shard_id: ShardId) -> Result<ShardCompactionReport, Status> {
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&shard_id) else {
            return Err(Status::error("shard_not_loaded", "shard is not loaded"));
        };
        let before_segments = collect_live_page_segment_ids(shard);
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
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.sequences.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for series in shard.ips.values_mut() {
            compact_page_addresses(
                &self.page_store,
                &self.cache,
                shard_id,
                series.values_mut(),
                &mut rewritten_page_refs,
            )?;
        }
        for (key, series) in &mut shard.ips_meta {
            for (timestamp, meta) in series {
                let bytes = read_page_bytes(&self.cache, &self.page_store, shard_id, &meta.address)
                    .ok_or_else(|| {
                        Status::error(
                            "page_compaction_failed",
                            format!("missing IPS page for {key}@{timestamp}"),
                        )
                    })?;
                let new_address = self
                    .page_store
                    .append(&bytes)
                    .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
                meta.address = new_address.clone();
                let _ = self.cache.put(
                    CacheKey::page(
                        shard_id,
                        new_address.page_segment_id,
                        new_address.offset,
                        new_address.length,
                    ),
                    bytes,
                );
                rewritten_page_refs += 1;
            }
        }

        let after_segments = collect_live_page_segment_ids(shard);
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
        })
    }

    fn index_path(&self, shard_id: ShardId) -> PathBuf {
        self.index_dir.join(format!("shard-{shard_id}.index.json"))
    }

    fn load_index(&self, shard_id: ShardId) -> Option<ShardState> {
        let bytes = fs::read(self.index_path(shard_id)).ok()?;
        serde_json::from_slice(&bytes).ok()
    }

    fn persist_index_bytes(&self, shard_id: ShardId, bytes: &[u8]) -> Result<(), std::io::Error> {
        fs::create_dir_all(&self.index_dir)?;
        fs::write(self.index_path(shard_id), bytes)
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
                oplog: self.oplog_store.stats(shard_id),
            }
        })
    }
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec_pretty(shard).unwrap_or_default()
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

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-rust-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
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
            for point in points {
                let timestamp = point.timestamp_ms.to_string();
                let object_id = stable_page_object_id(shard_id, "feature", &key, Some(&timestamp));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &point.value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    series.insert(point.timestamp_ms, address);
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
            for point in points {
                let exists = series.contains_key(&point.timestamp_ms);
                let should_write = match policy {
                    FeatureWritePolicy::Upsert => true,
                    FeatureWritePolicy::InsertIfAbsent => !exists,
                    FeatureWritePolicy::ReplaceExisting => exists,
                };
                if should_write {
                    let timestamp = point.timestamp_ms.to_string();
                    let object_id =
                        stable_page_object_id(shard_id, "feature", &key, Some(&timestamp));
                    if let Ok(address) = append_value(
                        cache,
                        page_store,
                        shard_id,
                        &point.value,
                        Some(object_id),
                        Some(routing_slot),
                        async_storage,
                    ) {
                        series.insert(point.timestamp_ms, address);
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
                                read_page_bytes(cache, page_store, shard_id, address).map(|value| {
                                    FeaturePoint {
                                        timestamp_ms: *timestamp_ms,
                                        value,
                                    }
                                })
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
                            read_page_bytes(cache, page_store, shard_id, address).and_then(
                                |value| {
                                    let row = SequenceFeatureRow::decode_cpp_feature_value(
                                        *timestamp_ms,
                                        &value,
                                    )?;
                                    filters
                                        .iter()
                                        .all(|filter| sequence_filter_matches(&row, filter))
                                        .then_some(FeaturePoint {
                                            timestamp_ms: *timestamp_ms,
                                            value,
                                        })
                                },
                            )
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
            for point in points {
                let timestamp = point.timestamp_ms.to_string();
                let object_id = stable_page_object_id(shard_id, "feature", &key, Some(&timestamp));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &point.value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    series.insert(point.timestamp_ms, address);
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
                        .filter_map(|(_, address)| {
                            read_page_bytes(cache, page_store, shard_id, address)
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
            for row in rows {
                if let Ok(bytes) = serde_json::to_vec(&row) {
                    let timestamp = row.timestamp_ms.to_string();
                    let object_id =
                        stable_page_object_id(shard_id, "sequence", &key, Some(&timestamp));
                    if let Ok(address) = append_value(
                        cache,
                        page_store,
                        shard_id,
                        &bytes,
                        Some(object_id),
                        Some(routing_slot),
                        async_storage,
                    ) {
                        series.insert(row.timestamp_ms, address);
                        mutated = true;
                    }
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
                        .filter_map(|(_, address)| {
                            read_sequence_row(cache, page_store, shard_id, address)
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
            let timestamp = timestamp_ms.to_string();
            let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &instance,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
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
            let timestamp = timestamp_ms.to_string();
            let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            if let Ok(address) = append_value(
                cache,
                page_store,
                shard_id,
                &instance,
                Some(object_id),
                Some(routing_slot),
                async_storage,
            ) {
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
            let mut loaded = 0i64;
            let routing_slot = page_routing_slot(&key, start_routing_slot, end_routing_slot);
            for point in points {
                let timestamp = point.timestamp_ms.to_string();
                let object_id = stable_page_object_id(shard_id, "ips", &key, Some(&timestamp));
                if let Ok(address) = append_value(
                    cache,
                    page_store,
                    shard_id,
                    &point.value,
                    Some(object_id),
                    Some(routing_slot),
                    async_storage,
                ) {
                    shard
                        .ips
                        .entry(key.clone())
                        .or_default()
                        .insert(point.timestamp_ms, address.clone());
                    shard.ips_meta.entry(key.clone()).or_default().insert(
                        point.timestamp_ms,
                        IpsPointMeta {
                            address,
                            action_type: None,
                            table_id: None,
                            request_id: None,
                        },
                    );
                    loaded += 1;
                    mutated = true;
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
                            read_page_bytes(cache, page_store, shard_id, address).map(|value| {
                                FeaturePoint {
                                    timestamp_ms: *timestamp_ms,
                                    value,
                                }
                            })
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
                                    read_page_bytes(cache, page_store, shard_id, address).map(
                                        |value| FeaturePoint {
                                            timestamp_ms: *timestamp_ms,
                                            value,
                                        },
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
                .entry(key)
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
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
                    .insert(key, now_ms().saturating_add(ttl_ms));
            }
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
                .entry(key)
                .or_default()
                .entry(timestamp_ms)
                .or_default() += amount;
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
            let series = shard.risk.entry(key).or_default();
            *series.entry(timestamp_ms).or_default() += amount;
            let values = series
                .range(start_ms..=end_ms)
                .map(|(_, value)| *value)
                .collect::<Vec<_>>();
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
    removed |= shard.risk_changes.remove(key).is_some();
    removed |= shard.risk_fol.remove(key).is_some();
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
    for series in shard.ips_meta.values() {
        ids.extend(series.values().map(|meta| meta.address.page_segment_id));
    }
    ids
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
        addresses.extend(series.values().cloned());
    }
    for series in shard.sequences.values() {
        addresses.extend(series.values().cloned());
    }
    for series in shard.ips.values() {
        addresses.extend(series.values().cloned());
    }
    for series in shard.ips_meta.values() {
        addresses.extend(series.values().map(|meta| meta.address.clone()));
    }
    addresses
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
            .append(&bytes)
            .map_err(|err| Status::error("page_compaction_failed", err.to_string()))?;
        *address = new_address.clone();
        let _ = cache.put(
            CacheKey::page(
                shard_id,
                new_address.page_segment_id,
                new_address.offset,
                new_address.length,
            ),
            bytes,
        );
        *rewritten_page_refs += 1;
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
        CacheKey::page(
            shard_id,
            address.page_segment_id,
            address.offset,
            address.length,
        ),
        bytes,
    );
    Ok(address)
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
        || shard.risk_changes.contains_key(key)
        || shard.risk_fol.contains_key(key)
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
    address: &PageAddress,
) -> Option<SequenceFeatureRow> {
    let bytes = read_page_bytes(cache, page_store, shard_id, address)?;
    serde_json::from_slice(&bytes).ok()
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
                .filter_map(|(_, address)| read_sequence_row(cache, page_store, shard_id, address))
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
        "count" | "" => values.len() as i64,
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
                    read_page_bytes(cache, page_store, shard_id, address).map(|value| {
                        FeaturePoint {
                            timestamp_ms: *timestamp_ms,
                            value,
                        }
                    })
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
            read_page_bytes(cache, page_store, shard_id, &meta.address).map(|value| FeaturePoint {
                timestamp_ms: *timestamp_ms,
                value,
            })
        })
        .collect()
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
    let cache_key = CacheKey::page(
        shard_id,
        address.page_segment_id,
        address.offset,
        address.length,
    );
    if let Ok(Some(bytes)) = cache.get(&cache_key) {
        return Some(bytes);
    }
    let bytes = page_store.read(address).ok()?;
    let _ = cache.put(cache_key, bytes.clone());
    Some(bytes)
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
        + shard.risk.len();
    let page_ref_count = shard.strings.len()
        + shard.hashes.values().map(HashMap::len).sum::<usize>()
        + shard.sets.values().map(BTreeMap::len).sum::<usize>()
        + shard.features.values().map(BTreeMap::len).sum::<usize>()
        + shard.sequences.values().map(BTreeMap::len).sum::<usize>()
        + shard.ips.values().map(BTreeMap::len).sum::<usize>();
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
        | Command::IpsStat { .. }
        | Command::IpsFilter { .. }
        | Command::RiskCount { .. }
        | Command::RiskQuery { .. }
        | Command::RiskDetail { .. }
        | Command::RiskFamilyQuery { .. }
        | Command::RiskFolQuery { .. }
        | Command::RiskManager { .. } => Vec::new(),
    }
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
    use crate::page_store::PageStoreZoneState;
    use crate::types::parse_cpp_feature_filters;

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
        assert_eq!(ids, vec![7, 8, 9, 10, 11, 12, 13]);
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
        assert_eq!(engine.live_page_segment_ids(1), vec![1]);

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
        assert_eq!(report.oplog_records, 2);
        assert_eq!(report.index_log_records, 2);
        assert_eq!(report.active_page_segment_ids, vec![0, 1]);
        assert_eq!(report.live_page_segment_ids, vec![0, 1]);
        assert_eq!(report.total_page_refs, 2);
        assert_eq!(report.readable_page_refs, 2);
        assert!(report.all_live_pages_readable);
        assert_eq!(report.zone_descriptors.len(), 2);
        assert_eq!(report.zone_descriptors[0].state, PageStoreZoneState::Sealed);
        assert_eq!(report.zone_descriptors[1].state, PageStoreZoneState::Active);
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
    fn string_round_trip() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
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
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
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
            CacheKey::page(1, address.page_segment_id, address.offset, address.length)
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
            CacheKey::page(1, address.page_segment_id, address.offset, address.length)
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
    fn set_members_round_trip() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "group".to_string(),
                member: b"alice".to_vec(),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetAdd {
                key: "group".to_string(),
                member: b"bob".to_vec(),
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::SetMembers {
                key: "group".to_string(),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::Members {
                members: vec![b"alice".to_vec(), b"bob".to_vec()]
            }
        );
    }

    #[test]
    fn hash_multi_get_set_and_incrby_match_extension_api() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashMultiSet {
                        key: "h".to_string(),
                        entries: vec![
                            ("f1".to_string(), b"v1".to_vec()),
                            ("f2".to_string(), b"7".to_vec()),
                        ],
                    },
                })
                .response,
            CommandResponse::Empty
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashMultiGet {
                        key: "h".to_string(),
                        fields: vec!["f1".to_string(), "missing".to_string(), "f2".to_string()],
                    },
                })
                .response,
            CommandResponse::Values {
                values: vec![Some(b"v1".to_vec()), None, Some(b"7".to_vec())]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::HashIncrBy {
                        key: "h".to_string(),
                        field: "f2".to_string(),
                        increment: 5,
                    },
                })
                .response,
            CommandResponse::Integer { value: 12 }
        );
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
    fn feature_query_respects_count_limit() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureAppend {
                key: "f".to_string(),
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 1,
                        value: b"a".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 2,
                        value: b"b".to_vec(),
                    },
                ],
            },
        });
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::FeatureQuery {
                key: "f".to_string(),
                start_ms: 0,
                end_ms: 10,
                count: Some(1),
            },
        });
        assert_eq!(
            response.response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 1,
                    value: b"a".to_vec()
                }]
            }
        );
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
    fn ips_load_snapshot_stat_and_filter_match_cpp_style_module_shape() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsLoad {
                        key: "ips-load".to_string(),
                        points: vec![
                            FeaturePoint {
                                timestamp_ms: 10,
                                value: b"loaded-10".to_vec(),
                            },
                            FeaturePoint {
                                timestamp_ms: 20,
                                value: b"loaded-20".to_vec(),
                            },
                        ],
                    },
                })
                .response,
            CommandResponse::Integer { value: 2 }
        );
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-load".to_string(),
                timestamp_ms: 30,
                instance: b"opt-30".to_vec(),
                action_type: Some(7),
                table_id: Some(42),
                request_id: Some("req-30".to_string()),
            },
        });
        engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::IpsAddWithOptions {
                key: "ips-load".to_string(),
                timestamp_ms: 40,
                instance: b"opt-40".to_vec(),
                action_type: Some(7),
                table_id: Some(43),
                request_id: Some("req-40".to_string()),
            },
        });

        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsSnapshot {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 35,
                        count: None,
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![
                    FeaturePoint {
                        timestamp_ms: 10,
                        value: b"loaded-10".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 20,
                        value: b"loaded-20".to_vec(),
                    },
                    FeaturePoint {
                        timestamp_ms: 30,
                        value: b"opt-30".to_vec(),
                    },
                ]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsFilter {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 100,
                        count: Some(10),
                        action_type: Some(7),
                        table_id: Some(42),
                    },
                })
                .response,
            CommandResponse::FeaturePoints {
                points: vec![FeaturePoint {
                    timestamp_ms: 30,
                    value: b"opt-30".to_vec(),
                }]
            }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::IpsStat {
                        key: "ips-load".to_string(),
                        start_ms: 0,
                        end_ms: 100,
                    },
                })
                .response,
            CommandResponse::IpsStats {
                stats: IpsStats {
                    total: 4,
                    first_timestamp_ms: Some(10),
                    last_timestamp_ms: Some(40),
                    action_type_counts: vec![(7, 2)],
                    table_id_counts: vec![(42, 1), (43, 1)],
                }
            }
        );
    }

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
    fn risk_cpp_family_set_query_setandget_and_manager_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (family, timestamp_ms, amount) in [
            (RiskFamily::H, 10, 5),
            (RiskFamily::H, 20, 7),
            (RiskFamily::Cpc, 10, 3),
            (RiskFamily::Fol, 10, 11),
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command: Command::RiskSet {
                            family,
                            key: "risk-cpp".to_string(),
                            timestamp_ms,
                            amount,
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
                    command: Command::RiskFamilyQuery {
                        family: RiskFamily::H,
                        key: "risk-cpp".to_string(),
                        start_ms: 0,
                        end_ms: 30,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 12 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskSetAndGet {
                        family: RiskFamily::Cpc,
                        key: "risk-cpp".to_string(),
                        timestamp_ms: 20,
                        amount: 4,
                        start_ms: 0,
                        end_ms: 30,
                        aggregator: "sum".to_string(),
                    },
                })
                .response,
            CommandResponse::Integer { value: 7 }
        );
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::RiskManager {
                        key: "risk-cpp".to_string(),
                    },
                })
                .response,
            CommandResponse::HashEntries {
                entries: vec![
                    ("h_events".to_string(), b"2".to_vec()),
                    ("h_sum".to_string(), b"12".to_vec()),
                    ("cpc_events".to_string(), b"2".to_vec()),
                    ("cpc_sum".to_string(), b"7".to_vec()),
                    ("fol_events".to_string(), b"1".to_vec()),
                    ("fol_sum".to_string(), b"11".to_vec()),
                ],
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

        let metrics = engine.prometheus_metrics();
        assert!(metrics.contains("temporalstore_shard_records{shard_id=\"1\",kind=\"string\"} 1"));
        assert!(metrics.contains("temporalstore_cache_operations_total"));
        assert!(metrics.contains(
            "temporalstore_cache_operations_total{shard_id=\"1\",kind=\"memory_evictions\"}"
        ));
        assert!(metrics.contains("temporalstore_page_store_operations_total"));
        assert!(metrics.contains("temporalstore_oplog_records_total{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_objects{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_page_refs{shard_id=\"1\"} 1"));
        assert!(metrics.contains("temporalstore_object_manager_dirty_objects{shard_id=\"1\"} 1"));
        assert!(
            metrics.contains("temporalstore_partition_routing_slots{shard_id=\"1\"} 4294967295")
        );
    }
}
