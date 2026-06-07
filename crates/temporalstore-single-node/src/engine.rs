use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::cache::{CacheKey, MultiLayerCache};
use crate::control::{
    Config, GetConfigResponse, GetInfoResponse, GetStatsResponse, LoadShardRequest,
    LoadShardResponse, MembershipUpdateRequest, ScanStreamRequest, ScanStreamResponse,
    SetConfigRequest, ShardInfo, ShardStats, StreamKind, StreamReadRequest, StreamReadResponse,
    StreamRecord, UnloadShardRequest, UnloadShardResponse,
};
use crate::index_log::LocalIndexLogStore;
use crate::oplog::LocalOplogStore;
use crate::page_store::{LocalPageStore, PageAddress};
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow, ShardId,
    Status,
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
    risk: HashMap<String, BTreeMap<u64, i64>>,
}

struct ExecuteOutcome {
    response: CommandResponse,
    mutated: bool,
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
        Self::with_cache_page_store_and_index_dir(
            MultiLayerCache::new(memory_capacity_bytes, cache_dir),
            LocalPageStore::new(page_store_dir),
            index_dir,
        )
    }

    pub fn load_shard(&self, shard_id: ShardId) {
        let request = LoadShardRequest {
            shard_id,
            load_version: 0,
            shard_uri: String::new(),
            start_routing_slot: 0,
            end_routing_slot: u32::MAX,
            readonly: false,
            table_name: String::new(),
        };
        let _ = self.load_shard_with(request);
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
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
        self.shards
            .write()
            .expect("engine lock poisoned")
            .remove(&request.shard_id);
        if let Some(info) = self
            .infos
            .write()
            .expect("info lock poisoned")
            .get_mut(&request.shard_id)
        {
            info.loaded = false;
        }
        UnloadShardResponse {
            status: Status::ok(),
        }
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
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
        let feature_max_size = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .map(|config| config.feature_max_size)
            .unwrap_or(5000);
        let outcome = execute_on_shard(
            &self.cache,
            &self.page_store,
            feature_max_size,
            request.shard_id,
            shard,
            command.clone(),
        );
        if outcome.mutated {
            if is_write_command(&command) {
                let _ = self.oplog_store.append(request.shard_id, command);
            }
            let index_bytes = serialize_index(shard);
            let _ = self
                .index_log_store
                .append_json(request.shard_id, &index_bytes);
            let _ = self.persist_index_bytes(request.shard_id, &index_bytes);
        }
        ExecuteResponse {
            status: Status::ok(),
            response: outcome.response,
        }
    }

    pub fn set_config(&self, request: SetConfigRequest) -> Status {
        self.configs
            .write()
            .expect("config lock poisoned")
            .insert(request.shard_id, request.config);
        Status::ok()
    }

    pub fn get_config(&self, shard_id: ShardId) -> GetConfigResponse {
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
            info.replica_node_ids = request.replica_node_ids;
            info.leader_node_id = request.leader_node_id;
            Status::ok()
        } else {
            Status::error("shard_not_found", "shard is not loaded")
        }
    }

    pub fn get_stats(&self, shard_id: ShardId) -> GetStatsResponse {
        let shards = self.shards.read().expect("engine lock poisoned");
        let stats = shards.get(&shard_id).map(|state| ShardStats {
            shard_id,
            string_records: state.strings.len(),
            hash_records: state.hashes.len(),
            set_records: state.sets.len(),
            feature_records: state.features.len(),
            sequence_records: state.sequences.len(),
            ips_records: state.ips.len(),
            risk_records: state.risk.len(),
            cache: self.cache.stats(),
            page_store: self.page_store.stats(),
            oplog: self.oplog_store.stats(shard_id),
        });
        GetStatsResponse {
            status: if stats.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not loaded")
            },
            stats,
        }
    }

    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Page => self
                .page_store
                .read_range(request.page_segment_id, request.offset, request.size)
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
}

fn serialize_index(shard: &ShardState) -> Vec<u8> {
    serde_json::to_vec_pretty(shard).unwrap_or_default()
}

fn unique_temp_path(kind: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "temporalstore-single-node-{kind}-{}-{nanos}-{counter}",
        std::process::id()
    ))
}

fn execute_on_shard(
    cache: &MultiLayerCache,
    page_store: &LocalPageStore,
    feature_max_size: usize,
    shard_id: ShardId,
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
            shard
                .expires_at_ms
                .insert(key.clone(), now_ms().saturating_add(ttl_ms));
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
            if let Ok(address) = page_store.append(&value) {
                shard.strings.insert(key.clone(), address);
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            remove_if_expired(shard, &key);
            if let Ok(address) = page_store.append(&value) {
                shard.strings.insert(key.clone(), address);
                shard
                    .expires_at_ms
                    .insert(key.clone(), now_ms().saturating_add(ttl_ms));
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
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
                        .and_then(|address| page_store.read(address).ok()),
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
            if let Ok(address) = page_store.append(&value) {
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
                        .and_then(|address| page_store.read(address).ok()),
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
                        .and_then(|address| page_store.read(address).ok())
                })
                .collect();
            CommandResponse::Values { values }
        }
        Command::HashMultiSet { key, entries } => {
            remove_if_expired(shard, &key);
            for (field, value) in entries {
                if let Ok(address) = page_store.append(&value) {
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
                .and_then(|address| page_store.read(address).ok())
                .and_then(|bytes| parse_i64(&bytes))
                .unwrap_or_default();
            let value = current.saturating_add(increment);
            if let Ok(address) = page_store.append(value.to_string().as_bytes()) {
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
                            page_store
                                .read(address)
                                .ok()
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
            if let Ok(address) = page_store.append(&member) {
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
                            .filter_map(|address| page_store.read(address).ok())
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
            for point in points {
                if let Ok(address) = page_store.append(&point.value) {
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
                                page_store.read(address).ok().map(|value| FeaturePoint {
                                    timestamp_ms: *timestamp_ms,
                                    value,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                CommandResponse::FeaturePoints { points }
            },
        ),
        Command::FeatureReplace {
            key,
            start_ms,
            end_ms,
            points,
        } => {
            remove_if_expired(shard, &key);
            let series = shard.features.entry(key.clone()).or_default();
            let replaced = series
                .range(start_ms..=end_ms)
                .map(|(timestamp_ms, _)| *timestamp_ms)
                .collect::<Vec<_>>();
            for timestamp_ms in replaced {
                series.remove(&timestamp_ms);
                mutated = true;
            }
            for point in points {
                if let Ok(address) = page_store.append(&point.value) {
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
                        .filter_map(|(_, address)| page_store.read(address).ok())
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
            for row in rows {
                if let Ok(bytes) = serde_json::to_vec(&row) {
                    if let Ok(address) = page_store.append(&bytes) {
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
                        .filter_map(|(_, address)| read_sequence_row(page_store, address))
                        .filter(|row| {
                            filters
                                .iter()
                                .all(|filter| sequence_filter_matches(row, filter))
                        })
                        .take(count)
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::SequenceRows { rows }
        }
        Command::IpsAdd {
            key,
            timestamp_ms,
            instance,
        } => {
            remove_if_expired(shard, &key);
            if let Ok(address) = page_store.append(&instance) {
                shard
                    .ips
                    .entry(key)
                    .or_default()
                    .insert(timestamp_ms, address);
                mutated = true;
            }
            CommandResponse::Empty
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
                            page_store.read(address).ok().map(|value| FeaturePoint {
                                timestamp_ms: *timestamp_ms,
                                value,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            CommandResponse::FeaturePoints { points }
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
    shard
        .expires_at_ms
        .get(key)
        .map(|expires_at| expires_at.saturating_sub(now_ms()) as i64)
        .unwrap_or(-1)
}

fn remove_if_expired(shard: &mut ShardState, key: &str) -> bool {
    if shard
        .expires_at_ms
        .get(key)
        .map(|expires_at| *expires_at <= now_ms())
        .unwrap_or(false)
    {
        return delete_record(shard, key);
    }
    false
}

fn delete_record(shard: &mut ShardState, key: &str) -> bool {
    let mut removed = false;
    removed |= shard.expires_at_ms.remove(key).is_some();
    removed |= shard.strings.remove(key).is_some();
    removed |= shard.hashes.remove(key).is_some();
    removed |= shard.sets.remove(key).is_some();
    removed |= shard.features.remove(key).is_some();
    removed |= shard.sequences.remove(key).is_some();
    removed |= shard.ips.remove(key).is_some();
    removed |= shard.risk.remove(key).is_some();
    removed
}

fn record_exists(shard: &ShardState, key: &str) -> bool {
    shard.strings.contains_key(key)
        || shard.hashes.contains_key(key)
        || shard.sets.contains_key(key)
        || shard.features.contains_key(key)
        || shard.sequences.contains_key(key)
        || shard.ips.contains_key(key)
        || shard.risk.contains_key(key)
}

fn invalidate_record_all(cache: &MultiLayerCache, shard_id: ShardId, key: &str) {
    let _ = cache.invalidate(&CacheKey::string(shard_id, key));
    let _ = cache.invalidate_record(shard_id, "hash", key);
    let _ = cache.invalidate_record(shard_id, "set", key);
    let _ = cache.invalidate_record(shard_id, "feature", key);
}

fn read_sequence_row(
    page_store: &LocalPageStore,
    address: &PageAddress,
) -> Option<SequenceFeatureRow> {
    let bytes = page_store.read(address).ok()?;
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
        FeatureFilterOp::LessThan => lhs < filter.value,
    }
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

fn parse_i64(bytes: &Vec<u8>) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

fn is_write_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonDelete { .. }
            | Command::CommonExpire { .. }
            | Command::StringSet { .. }
            | Command::StringSetEx { .. }
            | Command::StringDelete { .. }
            | Command::HashSet { .. }
            | Command::HashMultiSet { .. }
            | Command::HashIncrBy { .. }
            | Command::HashDelete { .. }
            | Command::SetAdd { .. }
            | Command::SetRemove { .. }
            | Command::FeatureAppend { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::RiskIncrement { .. }
    )
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
        let _ = cache.put(key, bytes);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.memory_hits, 1);
        assert!(stats.puts >= 1);
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
        assert_eq!(cache.stats().puts, 1);
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
        assert_eq!(cache.stats().memory_hits, 1);
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

        cache.invalidate(&CacheKey::string(1, "k")).unwrap();
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
        assert_eq!(cache.stats().puts, 2);
        assert!(cache.stats().memory_bytes > 0);
        assert!(cache.stats().disk_bytes > 0);
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
                    op: FeatureFilterOp::GreaterThan,
                    value: 100 + (seed * 29) % 500,
                },
                FeatureFilter {
                    field: "author_id".to_string(),
                    op: FeatureFilterOp::LessThan,
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
                .filter(|row| filters.iter().all(|filter| sequence_filter_matches(row, filter)))
                .take(count)
                .cloned()
                .collect::<Vec<_>>();

            assert_eq!(rows, expected, "seed {seed}");
            assert!(rows.windows(2).all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
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
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 7,
                    load_version: 42,
                    shard_uri: "file:///tmp/shard-7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 20,
                    readonly: false,
                    table_name: "table".to_string(),
                })
                .status
                .ok
        );
        assert!(
            engine
                .set_config(SetConfigRequest {
                    shard_id: 7,
                    config: Config {
                        version: 2,
                        feature_max_size: 123,
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
                    replica_node_ids: vec![1, 2, 3],
                    leader_node_id: Some(1),
                })
                .ok
        );
        let info = engine.get_info(7).info.unwrap();
        assert_eq!(info.load_version, 42);
        assert_eq!(info.replica_node_ids, vec![1, 2, 3]);

        engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        let stats = engine.get_stats(7).stats.unwrap();
        assert_eq!(stats.string_records, 1);
        assert_eq!(stats.page_store.writes, 1);

        assert!(
            engine
                .unload_shard_with(UnloadShardRequest { shard_id: 7 })
                .status
                .ok
        );
        assert!(!engine.get_info(7).info.unwrap().loaded);
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
        assert!(engine
            .load_shard_with(LoadShardRequest {
                shard_id: 1,
                load_version: 1,
                shard_uri: "file:///tmp/readonly".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 99,
                readonly: true,
                table_name: "table".to_string(),
            })
            .status
            .ok);

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
}
