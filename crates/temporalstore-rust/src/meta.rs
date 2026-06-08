use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::partition_id::{validate_partition_set_count, PartitionId, MAX_TABLE_ID};
use crate::types::{ShardId, Status};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MetaEntityState {
    Normal,
    Frozen,
    Dropped,
}

impl Default for MetaEntityState {
    fn default() -> Self {
        Self::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLocation {
    pub shard_id: ShardId,
    pub server_addr: String,
    #[serde(default)]
    pub latest_snapshot: Option<ShardSnapshotRef>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardRequest {
    pub shard_id: ShardId,
    pub server_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardSnapshotRef {
    pub uri: String,
    pub checksum: String,
    pub byte_size: u64,
    pub last_log_index: u64,
    pub created_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishShardSnapshotRequest {
    pub shard_id: ShardId,
    pub snapshot: ShardSnapshotRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetShardResponse {
    pub status: Status,
    pub location: Option<ShardLocation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AckResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterServerRequest {
    pub server_addr: String,
    #[serde(default)]
    pub node_id: u64,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatRequest {
    pub server_addr: String,
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub binary_version: String,
    #[serde(default)]
    pub shard_loads: Vec<ShardLoad>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatResponse {
    pub status: Status,
    pub forbid_auto_register: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    pub key_count: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMetaInfo {
    pub server_addr: String,
    pub node_id: u64,
    pub location: String,
    pub state: MetaEntityState,
    pub last_heartbeat_ms: u64,
    pub boot_time_ms: u64,
    pub binary_version: String,
    pub shard_loads: Vec<ShardLoad>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleServerReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StaleResourceReport {
    pub status: Status,
    pub frozen_servers: Vec<String>,
    pub frozen_proxies: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FreezeStaleServersRequest {
    pub stale_after_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RegisterProxyRequest {
    pub proxy_addr: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub location: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatRequest {
    pub proxy_addr: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub config_version: u64,
    #[serde(default)]
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyHeartbeatResponse {
    pub status: Status,
    pub config_changed: bool,
    pub namespace: String,
    pub config_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetaInfo {
    pub proxy_addr: String,
    pub namespace: String,
    pub location: String,
    pub state: MetaEntityState,
    pub config_version: u64,
    pub last_heartbeat_ms: u64,
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddNamespaceRequest {
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceMetaInfo {
    pub namespace: String,
    pub table_count: usize,
    pub state: MetaEntityState,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddTableRequest {
    pub namespace: String,
    pub table_name: String,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    #[serde(default = "default_replica_count")]
    pub replica_count: u64,
    #[serde(default)]
    pub use_cpp_partition_ids: bool,
    #[serde(default)]
    pub partition_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTableTopologyRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub old_topology_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableMetaInfo {
    pub table_id: u64,
    pub namespace: String,
    pub table_name: String,
    pub state: MetaEntityState,
    pub topology_version: u64,
    pub first_shard_id: ShardId,
    pub shard_count: u64,
    pub replica_count: u64,
    #[serde(default)]
    pub use_cpp_partition_ids: bool,
    #[serde(default)]
    pub partition_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TablePartition {
    pub shard_id: ShardId,
    pub start_slot: u64,
    pub end_slot: u64,
    pub primary: Option<String>,
    pub replicas: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableTopologyResponse {
    pub status: Status,
    pub table: Option<TableMetaInfo>,
    pub partitions: Vec<TablePartition>,
    pub unchanged: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListNamespacesResponse {
    pub status: Status,
    pub namespaces: Vec<NamespaceMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListTablesResponse {
    pub status: Status,
    pub tables: Vec<TableMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListServersResponse {
    pub status: Status,
    pub servers: Vec<ServerMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListProxiesResponse {
    pub status: Status,
    pub proxies: Vec<ProxyMetaInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateChangeRequest {
    pub endpoint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadFinishRequest {
    pub server_addr: String,
    pub shard_id: ShardId,
    pub load_version: u64,
    pub status: Status,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaStats {
    pub register_shard_total: u64,
    pub get_shard_total: u64,
    pub server_register_total: u64,
    pub server_heartbeat_total: u64,
    pub proxy_register_total: u64,
    pub proxy_heartbeat_total: u64,
    pub namespace_create_total: u64,
    pub table_create_total: u64,
    pub topology_query_total: u64,
    pub load_finish_total: u64,
    pub topology_version: u64,
    pub server_count: usize,
    pub proxy_count: usize,
    pub namespace_count: usize,
    pub table_count: usize,
    pub shard_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaInfo {
    pub status: Status,
    pub stats: MetaStats,
    pub boot_time_ms: u64,
    pub durable_mutation_log: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum MetaMutation {
    RegisterShard(RegisterShardRequest),
    PublishShardSnapshot(PublishShardSnapshotRequest),
    RegisterServer(RegisterServerRequest),
    RegisterProxy(RegisterProxyRequest),
    AddNamespace(AddNamespaceRequest),
    AddTable(AddTableRequest),
    FinishLoad(LoadFinishRequest),
    FreezeServer(StateChangeRequest),
    DropServer(StateChangeRequest),
    FreezeProxy(StateChangeRequest),
    DropProxy(StateChangeRequest),
}

#[derive(Debug, Clone)]
pub struct LocalMetaMutationLog {
    path: PathBuf,
    write_lock: Arc<Mutex<()>>,
}

impl LocalMetaMutationLog {
    pub fn new(path: impl Into<PathBuf>) -> io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self {
            path,
            write_lock: Arc::default(),
        })
    }

    pub fn append(&self, mutation: &MetaMutation) -> io::Result<()> {
        let _guard = self
            .write_lock
            .lock()
            .expect("meta mutation log lock poisoned");
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        serde_json::to_writer(&mut file, mutation).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
        Ok(())
    }

    pub fn load(&self) -> io::Result<Vec<MetaMutation>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let mut mutations = Vec::new();
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let mutation = serde_json::from_str::<MetaMutation>(&line).map_err(io::Error::other)?;
            mutations.push(mutation);
        }
        Ok(mutations)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Default)]
struct MetaCounters {
    register_shard_total: u64,
    get_shard_total: u64,
    server_register_total: u64,
    server_heartbeat_total: u64,
    proxy_register_total: u64,
    proxy_heartbeat_total: u64,
    namespace_create_total: u64,
    table_create_total: u64,
    topology_query_total: u64,
    load_finish_total: u64,
}

#[derive(Debug, Clone)]
struct TableRecord {
    info: TableMetaInfo,
}

#[derive(Debug, Default)]
struct MetaState {
    shards: HashMap<ShardId, ShardLocation>,
    servers: BTreeMap<String, ServerMetaInfo>,
    proxies: BTreeMap<String, ProxyMetaInfo>,
    namespaces: BTreeMap<String, MetaEntityState>,
    tables: BTreeMap<String, TableRecord>,
    counters: MetaCounters,
    next_table_id: u64,
    topology_version: u64,
}

#[derive(Debug, Clone)]
pub struct SingleNodeMeta {
    inner: Arc<RwLock<MetaState>>,
    boot_time_ms: u64,
    mutation_log: Option<LocalMetaMutationLog>,
}

impl Default for SingleNodeMeta {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MetaState {
                next_table_id: 1,
                ..MetaState::default()
            })),
            boot_time_ms: now_ms(),
            mutation_log: None,
        }
    }
}

impl SingleNodeMeta {
    pub fn with_mutation_log(path: impl Into<PathBuf>) -> io::Result<Self> {
        let log = LocalMetaMutationLog::new(path)?;
        let meta = Self {
            mutation_log: Some(log.clone()),
            ..Self::default()
        };
        for mutation in log.load()? {
            meta.apply_mutation(mutation);
        }
        Ok(meta)
    }

    pub fn register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        self.record_mutation(MetaMutation::RegisterShard(request.clone()));
        self.apply_register(request)
    }

    fn apply_register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.register_shard_total += 1;
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        state.shards.insert(
            request.shard_id,
            ShardLocation {
                shard_id: request.shard_id,
                server_addr: request.server_addr.clone(),
                latest_snapshot,
            },
        );
        ensure_server(&mut state, &request.server_addr);
        state.topology_version += 1;
        RegisterShardResponse {
            status: Status::ok(),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.get_shard_total += 1;
        let location = state.shards.get(&shard_id).cloned();
        GetShardResponse {
            status: if location.is_some() {
                Status::ok()
            } else {
                Status::error("shard_not_found", "shard is not registered")
            },
            location,
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        self.record_mutation(MetaMutation::PublishShardSnapshot(request.clone()));
        self.apply_publish_shard_snapshot(request)
    }

    fn apply_publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(location) = state.shards.get_mut(&request.shard_id) else {
            return AckResponse {
                status: Status::error("shard_not_found", "shard is not registered"),
            };
        };
        if location.latest_snapshot.as_ref().map_or(false, |existing| {
            existing.last_log_index > request.snapshot.last_log_index
        }) {
            return AckResponse {
                status: Status::error(
                    "stale_snapshot",
                    "snapshot is older than the latest metaserver record",
                ),
            };
        }
        location.latest_snapshot = Some(request.snapshot);
        state.topology_version += 1;
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        self.record_mutation(MetaMutation::RegisterServer(request.clone()));
        self.apply_register_server(request)
    }

    fn apply_register_server(&self, request: RegisterServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.server_register_total += 1;
        let now = now_ms();
        state.servers.insert(
            request.server_addr.clone(),
            ServerMetaInfo {
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                state: MetaEntityState::Normal,
                last_heartbeat_ms: now,
                boot_time_ms: 0,
                binary_version: request.binary_version,
                shard_loads: Vec::new(),
            },
        );
        state.topology_version += 1;
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.server_heartbeat_total += 1;
        let Some(server) = state.servers.get_mut(&request.server_addr) else {
            return ServerHeartbeatResponse {
                status: Status::error("not_found", "server not found"),
                forbid_auto_register: false,
            };
        };
        if server.state == MetaEntityState::Frozen {
            return ServerHeartbeatResponse {
                status: Status::error("resource_frozen", "server frozen"),
                forbid_auto_register: true,
            };
        }
        server.last_heartbeat_ms = now_ms();
        server.boot_time_ms = request.boot_time_ms;
        if !request.binary_version.is_empty() {
            server.binary_version = request.binary_version;
        }
        server.shard_loads = request.shard_loads;
        ServerHeartbeatResponse {
            status: Status::ok(),
            forbid_auto_register: false,
        }
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        self.record_mutation(MetaMutation::RegisterProxy(request.clone()));
        self.apply_register_proxy(request)
    }

    fn apply_register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.proxy_register_total += 1;
        state.proxies.insert(
            request.proxy_addr.clone(),
            ProxyMetaInfo {
                proxy_addr: request.proxy_addr,
                namespace: request.namespace,
                location: request.location,
                state: MetaEntityState::Normal,
                config_version: request.config_version,
                last_heartbeat_ms: now_ms(),
                binary_version: request.binary_version,
            },
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.proxy_heartbeat_total += 1;
        let Some(proxy) = state.proxies.get_mut(&request.proxy_addr) else {
            return ProxyHeartbeatResponse {
                status: Status::error("not_found", "proxy not found"),
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
            };
        };
        if proxy.state == MetaEntityState::Frozen {
            return ProxyHeartbeatResponse {
                status: Status::error("resource_frozen", "proxy frozen"),
                config_changed: false,
                namespace: proxy.namespace.clone(),
                config_version: proxy.config_version,
            };
        }
        proxy.last_heartbeat_ms = now_ms();
        if !request.binary_version.is_empty() {
            proxy.binary_version = request.binary_version;
        }
        let config_changed =
            proxy.namespace != request.namespace || proxy.config_version > request.config_version;
        ProxyHeartbeatResponse {
            status: Status::ok(),
            config_changed,
            namespace: proxy.namespace.clone(),
            config_version: proxy.config_version,
        }
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        self.record_mutation(MetaMutation::AddNamespace(request.clone()));
        self.apply_add_namespace(request)
    }

    fn apply_add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.namespace_create_total += 1;
        state
            .namespaces
            .entry(request.namespace)
            .or_insert(MetaEntityState::Normal);
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::AddTable(request.clone()));
        self.apply_add_table(request)
    }

    fn apply_add_table(&self, request: AddTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        if request.shard_count == 0 {
            return AckResponse {
                status: Status::error("bad_request", "shard_count must be > 0"),
            };
        }
        if request.use_cpp_partition_ids {
            if request.shard_count > u32::MAX as u64 {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "shard_count exceeds C++ partition-set range",
                    ),
                };
            }
            if let Err(err) = validate_partition_set_count(request.shard_count as u32) {
                return AckResponse {
                    status: Status::error("bad_request", err.to_string()),
                };
            }
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.table_create_total += 1;
        state
            .namespaces
            .entry(request.namespace.clone())
            .or_insert(MetaEntityState::Normal);
        let key = table_key(&request.namespace, &request.table_name);
        if state.tables.contains_key(&key) {
            return AckResponse {
                status: Status::error("already_exists", "table already exists"),
            };
        }
        let table_id = state.next_table_id;
        if request.use_cpp_partition_ids && table_id > MAX_TABLE_ID as u64 {
            return AckResponse {
                status: Status::error(
                    "bad_request",
                    "table_id exceeds C++ partition id table range",
                ),
            };
        }
        state.next_table_id += 1;
        state.topology_version += 1;
        let first_shard_id = if request.use_cpp_partition_ids {
            match PartitionId::new(table_id, 0, 0, request.partition_version as u64) {
                Ok(partition_id) => partition_id.id(),
                Err(err) => {
                    return AckResponse {
                        status: Status::error("bad_request", err.to_string()),
                    };
                }
            }
        } else {
            request.first_shard_id
        };
        let info = TableMetaInfo {
            table_id,
            namespace: request.namespace,
            table_name: request.table_name,
            state: MetaEntityState::Normal,
            topology_version: state.topology_version,
            first_shard_id,
            shard_count: request.shard_count,
            replica_count: request.replica_count.max(1),
            use_cpp_partition_ids: request.use_cpp_partition_ids,
            partition_version: request.partition_version,
        };
        state.tables.insert(key, TableRecord { info });
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn get_table_topology(&self, request: GetTableTopologyRequest) -> TableTopologyResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.topology_query_total += 1;
        let Some(table) = state
            .tables
            .get(&table_key(&request.namespace, &request.table_name))
        else {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table not found"),
                table: None,
                partitions: Vec::new(),
                unchanged: false,
            };
        };
        if request.old_topology_version >= table.info.topology_version {
            return TableTopologyResponse {
                status: Status::ok(),
                table: Some(table.info.clone()),
                partitions: Vec::new(),
                unchanged: true,
            };
        }
        let partitions = build_partitions(&state, &table.info);
        TableTopologyResponse {
            status: Status::ok(),
            table: Some(table.info.clone()),
            partitions,
            unchanged: false,
        }
    }

    pub fn list_namespaces(&self) -> ListNamespacesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let namespaces = state
            .namespaces
            .iter()
            .map(|(namespace, state_value)| NamespaceMetaInfo {
                namespace: namespace.clone(),
                table_count: state
                    .tables
                    .values()
                    .filter(|table| table.info.namespace == *namespace)
                    .count(),
                state: *state_value,
            })
            .collect();
        ListNamespacesResponse {
            status: Status::ok(),
            namespaces,
        }
    }

    pub fn list_tables(&self) -> ListTablesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListTablesResponse {
            status: Status::ok(),
            tables: state
                .tables
                .values()
                .map(|table| table.info.clone())
                .collect(),
        }
    }

    pub fn list_servers(&self) -> ListServersResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListServersResponse {
            status: Status::ok(),
            servers: state.servers.values().cloned().collect(),
        }
    }

    pub fn freeze_stale_servers(&self, stale_after_ms: u64) -> StaleServerReport {
        let report = self.freeze_stale_resources(stale_after_ms);
        StaleServerReport {
            status: report.status,
            frozen_servers: report.frozen_servers,
        }
    }

    pub fn freeze_stale_resources(&self, stale_after_ms: u64) -> StaleResourceReport {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let now = now_ms();
        let mut frozen_servers = Vec::new();
        let mut frozen_proxies = Vec::new();
        for server in state.servers.values_mut() {
            if server.state == MetaEntityState::Normal
                && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
            {
                server.state = MetaEntityState::Frozen;
                frozen_servers.push(server.server_addr.clone());
            }
        }
        for proxy in state.proxies.values_mut() {
            if proxy.state == MetaEntityState::Normal
                && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
            {
                proxy.state = MetaEntityState::Frozen;
                frozen_proxies.push(proxy.proxy_addr.clone());
            }
        }
        if !frozen_servers.is_empty() || !frozen_proxies.is_empty() {
            state.topology_version += 1;
        }
        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
            frozen_proxies,
        }
    }

    pub fn start_failure_detector_loop(
        &self,
        stale_after_ms: u64,
        interval_ms: u64,
    ) -> thread::JoinHandle<()> {
        let meta = self.clone();
        let interval = Duration::from_millis(interval_ms.max(1));
        thread::spawn(move || loop {
            let _ = meta.freeze_stale_resources(stale_after_ms);
            thread::sleep(interval);
        })
    }

    pub fn list_proxies(&self) -> ListProxiesResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        ListProxiesResponse {
            status: Status::ok(),
            proxies: state.proxies.values().cloned().collect(),
        }
    }

    pub fn freeze_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(&request.endpoint, MetaEntityState::Frozen)
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(&request.endpoint, MetaEntityState::Dropped)
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(&request.endpoint, MetaEntityState::Frozen)
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(&request.endpoint, MetaEntityState::Dropped)
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        self.record_mutation(MetaMutation::FinishLoad(request.clone()));
        self.apply_finish_load(request)
    }

    fn apply_finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.load_finish_total += 1;
        if !request.status.ok {
            return AckResponse {
                status: request.status,
            };
        }
        ensure_server(&mut state, &request.server_addr);
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        state.shards.insert(
            request.shard_id,
            ShardLocation {
                shard_id: request.shard_id,
                server_addr: request.server_addr,
                latest_snapshot,
            },
        );
        state.topology_version += 1;
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn info(&self) -> MetaInfo {
        MetaInfo {
            status: Status::ok(),
            stats: self.stats(),
            boot_time_ms: self.boot_time_ms,
            durable_mutation_log: self.mutation_log.is_some(),
        }
    }

    pub fn stats(&self) -> MetaStats {
        let state = self.inner.read().expect("meta lock poisoned");
        MetaStats {
            register_shard_total: state.counters.register_shard_total,
            get_shard_total: state.counters.get_shard_total,
            server_register_total: state.counters.server_register_total,
            server_heartbeat_total: state.counters.server_heartbeat_total,
            proxy_register_total: state.counters.proxy_register_total,
            proxy_heartbeat_total: state.counters.proxy_heartbeat_total,
            namespace_create_total: state.counters.namespace_create_total,
            table_create_total: state.counters.table_create_total,
            topology_query_total: state.counters.topology_query_total,
            load_finish_total: state.counters.load_finish_total,
            topology_version: state.topology_version,
            server_count: state.servers.len(),
            proxy_count: state.proxies.len(),
            namespace_count: state.namespaces.len(),
            table_count: state.tables.len(),
            shard_count: state.shards.len(),
        }
    }

    fn set_server_state(&self, endpoint: &str, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .servers
            .contains_key(endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        }
        let request = StateChangeRequest {
            endpoint: endpoint.to_string(),
        };
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeServer(request));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropServer(request));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_server_state(endpoint, next)
    }

    fn apply_set_server_state(&self, endpoint: &str, next: MetaEntityState) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get_mut(endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        };
        server.state = next;
        state.topology_version += 1;
        AckResponse {
            status: Status::ok(),
        }
    }

    fn set_proxy_state(&self, endpoint: &str, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .proxies
            .contains_key(endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        }
        let request = StateChangeRequest {
            endpoint: endpoint.to_string(),
        };
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeProxy(request));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropProxy(request));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_proxy_state(endpoint, next)
    }

    fn apply_set_proxy_state(&self, endpoint: &str, next: MetaEntityState) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(proxy) = state.proxies.get_mut(endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        };
        proxy.state = next;
        AckResponse {
            status: Status::ok(),
        }
    }

    fn record_mutation(&self, mutation: MetaMutation) {
        if let Some(log) = &self.mutation_log {
            log.append(&mutation)
                .expect("failed to append metaserver mutation log");
        }
    }

    pub(crate) fn apply_mutation(&self, mutation: MetaMutation) -> Status {
        match mutation {
            MetaMutation::RegisterShard(request) => self.apply_register(request).status,
            MetaMutation::PublishShardSnapshot(request) => {
                self.apply_publish_shard_snapshot(request).status
            }
            MetaMutation::RegisterServer(request) => self.apply_register_server(request).status,
            MetaMutation::RegisterProxy(request) => self.apply_register_proxy(request).status,
            MetaMutation::AddNamespace(request) => self.apply_add_namespace(request).status,
            MetaMutation::AddTable(request) => self.apply_add_table(request).status,
            MetaMutation::FinishLoad(request) => self.apply_finish_load(request).status,
            MetaMutation::FreezeServer(request) => {
                self.apply_set_server_state(&request.endpoint, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::DropServer(request) => {
                self.apply_set_server_state(&request.endpoint, MetaEntityState::Dropped)
                    .status
            }
            MetaMutation::FreezeProxy(request) => {
                self.apply_set_proxy_state(&request.endpoint, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::DropProxy(request) => {
                self.apply_set_proxy_state(&request.endpoint, MetaEntityState::Dropped)
                    .status
            }
        }
    }
}

fn build_partitions(state: &MetaState, table: &TableMetaInfo) -> Vec<TablePartition> {
    #[derive(Debug)]
    struct PlacementCandidate {
        server_addr: String,
        location: String,
        key_count: u64,
        memory_bytes: u64,
    }

    let mut normal_servers = state
        .servers
        .values()
        .filter(|server| server.state == MetaEntityState::Normal)
        .map(|server| {
            let key_count = server
                .shard_loads
                .iter()
                .map(|load| load.key_count)
                .sum::<u64>();
            let memory_bytes = server
                .shard_loads
                .iter()
                .map(|load| load.memory_bytes)
                .sum::<u64>();
            PlacementCandidate {
                server_addr: server.server_addr.clone(),
                location: server.location.clone(),
                key_count,
                memory_bytes,
            }
        })
        .collect::<Vec<_>>();
    normal_servers.sort_by(|left, right| {
        (left.key_count, left.memory_bytes, &left.server_addr).cmp(&(
            right.key_count,
            right.memory_bytes,
            &right.server_addr,
        ))
    });
    let slot_count = 1_u64 << 30;
    let mut partitions = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let start_slot = slot_count * offset / table.shard_count;
        let end_slot = (slot_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_locations = BTreeSet::new();
        if let Some(location) = state.shards.get(&shard_id) {
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &location.server_addr,
            );
        }
        for candidate in &normal_servers {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            if seen_replicas.contains(&candidate.server_addr) {
                continue;
            }
            if !candidate.location.is_empty() && used_locations.contains(&candidate.location) {
                continue;
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &candidate.server_addr,
            );
        }
        for candidate in &normal_servers {
            if replicas.len() >= table.replica_count as usize {
                break;
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &candidate.server_addr,
            );
        }
        let primary = state
            .shards
            .get(&shard_id)
            .map(|location| location.server_addr.clone())
            .or_else(|| replicas.first().cloned());
        partitions.push(TablePartition {
            shard_id,
            start_slot,
            end_slot,
            primary,
            replicas,
        });
    }
    partitions
}

fn table_shard_id(
    table: &TableMetaInfo,
    offset: u64,
) -> Result<ShardId, crate::partition_id::PartitionIdError> {
    if !table.use_cpp_partition_ids {
        return Ok(table.first_shard_id + offset);
    }
    PartitionId::new(table.table_id, offset, 0, table.partition_version as u64).map(PartitionId::id)
}

fn push_replica(
    state: &MetaState,
    replicas: &mut Vec<String>,
    seen_replicas: &mut BTreeSet<String>,
    used_locations: &mut BTreeSet<String>,
    server_addr: &str,
) {
    if !seen_replicas.insert(server_addr.to_string()) {
        return;
    }
    if let Some(server) = state.servers.get(server_addr) {
        if !server.location.is_empty() {
            used_locations.insert(server.location.clone());
        }
    }
    replicas.push(server_addr.to_string());
}

fn ensure_server(state: &mut MetaState, server_addr: &str) {
    state
        .servers
        .entry(server_addr.to_string())
        .or_insert_with(|| ServerMetaInfo {
            server_addr: server_addr.to_string(),
            node_id: 0,
            location: String::new(),
            state: MetaEntityState::Normal,
            last_heartbeat_ms: now_ms(),
            boot_time_ms: 0,
            binary_version: String::new(),
            shard_loads: Vec::new(),
        });
}

fn table_key(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn default_replica_count() -> u64 {
    1
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metaserver_tracks_servers_heartbeats_and_shard_routes() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                server_addr: "127.0.0.1:18001".to_string(),
                node_id: 1,
                location: "zone-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 7,
                server_addr: "127.0.0.1:18001".to_string(),
            })
            .status
            .ok
        );
        assert_eq!(meta.get(7).location.unwrap().server_addr, "127.0.0.1:18001");
        assert!(
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: "127.0.0.1:18001".to_string(),
                boot_time_ms: 11,
                binary_version: "v1".to_string(),
                shard_loads: vec![ShardLoad {
                    shard_id: 7,
                    key_count: 10,
                    memory_bytes: 100,
                }],
            })
            .status
            .ok
        );
        let server = meta.list_servers().servers.remove(0);
        assert_eq!(server.shard_loads[0].key_count, 10);
        assert_eq!(meta.stats().server_heartbeat_total, 1);
    }

    #[test]
    fn metaserver_freezes_stale_servers_from_heartbeat_age() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "stale".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let report = meta.freeze_stale_servers(0);
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["stale".to_string()]);
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_failure_detector_loop_freezes_stale_servers_and_proxies() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "stale-server".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "stale-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "z".to_string(),
            config_version: 1,
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));
        let _detector = meta.start_failure_detector_loop(0, 10);

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if meta.list_servers().servers[0].state == MetaEntityState::Frozen
                && meta.list_proxies().proxies[0].state == MetaEntityState::Frozen
            {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("failure detector loop did not freeze stale resources");
    }

    #[test]
    fn metaserver_serves_table_topology_and_version_not_modified() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z1".to_string(),
            binary_version: String::new(),
        });
        meta.register_server(RegisterServerRequest {
            server_addr: "s2".to_string(),
            node_id: 2,
            location: "z2".to_string(),
            binary_version: String::new(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 10,
            server_addr: "s1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 2,
                replica_count: 2,
                use_cpp_partition_ids: false,
                partition_version: 0,
            })
            .status
            .ok
        );

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topo.status.ok);
        assert_eq!(topo.partitions.len(), 2);
        assert_eq!(topo.partitions[0].primary.as_deref(), Some("s1"));
        assert_eq!(topo.partitions[0].replicas.len(), 2);

        let unchanged = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: topo.table.unwrap().topology_version,
        });
        assert!(unchanged.status.ok);
        assert!(unchanged.unchanged);
        assert!(unchanged.partitions.is_empty());
    }

    #[test]
    fn metaserver_can_generate_cpp_compatible_partition_ids_for_table_topology() {
        let meta = SingleNodeMeta::default();
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "cpp_ids".to_string(),
            first_shard_id: 999,
            shard_count: 3,
            replica_count: 1,
            use_cpp_partition_ids: true,
            partition_version: 17,
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "cpp_ids".to_string(),
            old_topology_version: 0,
        });

        assert!(topo.status.ok);
        let table = topo.table.unwrap();
        assert_eq!(table.table_id, 1);
        assert!(table.use_cpp_partition_ids);
        assert_eq!(table.partition_version, 17);
        assert_eq!(
            table.first_shard_id,
            PartitionId::new(1, 0, 0, 17).unwrap().id()
        );
        let shard_ids = topo
            .partitions
            .iter()
            .map(|partition| partition.shard_id)
            .collect::<Vec<_>>();
        assert_eq!(
            shard_ids,
            vec![
                PartitionId::new(1, 0, 0, 17).unwrap().id(),
                PartitionId::new(1, 1, 0, 17).unwrap().id(),
                PartitionId::new(1, 2, 0, 17).unwrap().id(),
            ]
        );
        for (offset, shard_id) in shard_ids.into_iter().enumerate() {
            let decoded = PartitionId::from_raw(shard_id);
            assert_eq!(decoded.table_id(), 1);
            assert_eq!(decoded.partition_set_index(), offset as u32);
            assert_eq!(decoded.partition_index(), 0);
            assert_eq!(decoded.partition_version(), 17);
        }
    }

    #[test]
    fn metaserver_topology_prefers_lower_load_replicas() {
        let meta = SingleNodeMeta::default();
        for (server_addr, key_count, memory_bytes) in [
            ("hot", 10_000, 10_000),
            ("cool", 10, 10),
            ("warm", 100, 100),
        ] {
            meta.register_server(RegisterServerRequest {
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: "z".to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 100,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["cool".to_string(), "warm".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_prefers_location_diversity_before_same_zone_load() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("zone-a-cool", "zone-a", 10, 10),
            ("zone-a-warm", "zone-a", 20, 20),
            ("zone-b-hot", "zone-b", 10_000, 10_000),
        ] {
            meta.register_server(RegisterServerRequest {
                server_addr: server_addr.to_string(),
                node_id: 0,
                location: location.to_string(),
                binary_version: String::new(),
            });
            meta.server_heartbeat(ServerHeartbeatRequest {
                server_addr: server_addr.to_string(),
                boot_time_ms: 1,
                binary_version: String::new(),
                shard_loads: vec![ShardLoad {
                    shard_id: 1,
                    key_count,
                    memory_bytes,
                }],
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 200,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["zone-a-cool".to_string(), "zone-b-hot".to_string()]
        );
        assert_eq!(topo.partitions[0].primary.as_deref(), Some("zone-a-cool"));
    }

    #[test]
    fn metaserver_tracks_proxy_heartbeat_config_changes() {
        let meta = SingleNodeMeta::default();
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            location: "z1".to_string(),
            config_version: 3,
            binary_version: "v".to_string(),
        });
        let response = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            config_version: 2,
            binary_version: "v2".to_string(),
        });
        assert!(response.status.ok);
        assert!(response.config_changed);
        assert_eq!(response.config_version, 3);
        assert_eq!(meta.list_proxies().proxies[0].binary_version, "v2");
    }

    #[test]
    fn metaserver_finish_load_updates_shard_route() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        let ack = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
        });
        assert!(ack.status.ok);
        assert_eq!(meta.get(42).location.unwrap().server_addr, "s1");
        assert_eq!(meta.stats().load_finish_total, 1);
    }

    #[test]
    fn metaserver_mutation_log_recovers_routes_tables_and_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(meta.info().durable_mutation_log);
        meta.register_server(RegisterServerRequest {
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 10,
            server_addr: "server-a".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 7,
            binary_version: "v1".to_string(),
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                endpoint: "proxy-a".to_string(),
            })
            .status
            .ok
        );

        let mutations = LocalMetaMutationLog::new(&log_path)
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(mutations.len(), 6);

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(10).location.unwrap(),
            ShardLocation {
                shard_id: 10,
                server_addr: "server-a".to_string(),
                latest_snapshot: None,
            }
        );
        assert_eq!(recovered.list_tables().tables.len(), 1);
        assert_eq!(
            recovered
                .get_table_topology(GetTableTopologyRequest {
                    namespace: "ns".to_string(),
                    table_name: "tbl".to_string(),
                    old_topology_version: 0,
                })
                .partitions[0]
                .primary
                .as_deref(),
            Some("server-a")
        );
        assert_eq!(
            recovered.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_records_latest_shard_snapshot_and_rejects_stale_ref() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("snapshot-meta.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register(RegisterShardRequest {
            shard_id: 44,
            server_addr: "server-a".to_string(),
        });
        let first = ShardSnapshotRef {
            uri: "s3://cluster-a/shards/44/snapshots/1-10-a/manifest.json".to_string(),
            checksum: "sha256:first".to_string(),
            byte_size: 1024,
            last_log_index: 10,
            created_at_ms: 100,
        };
        assert!(
            meta.publish_shard_snapshot(PublishShardSnapshotRequest {
                shard_id: 44,
                snapshot: first.clone(),
            })
            .status
            .ok
        );
        let stale = ShardSnapshotRef {
            uri: "s3://cluster-a/shards/44/snapshots/1-9-stale/manifest.json".to_string(),
            checksum: "sha256:stale".to_string(),
            byte_size: 512,
            last_log_index: 9,
            created_at_ms: 90,
        };
        assert_eq!(
            meta.publish_shard_snapshot(PublishShardSnapshotRequest {
                shard_id: 44,
                snapshot: stale,
            })
            .status
            .code,
            "stale_snapshot"
        );

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(44).location.unwrap().latest_snapshot,
            Some(first)
        );
    }

    #[test]
    fn metaserver_mutation_log_ignores_failed_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();

        let missing_server = meta.freeze_server(StateChangeRequest {
            endpoint: "missing-server".to_string(),
        });
        let missing_proxy = meta.drop_proxy(StateChangeRequest {
            endpoint: "missing-proxy".to_string(),
        });

        assert!(!missing_server.status.ok);
        assert!(!missing_proxy.status.ok);
        assert!(LocalMetaMutationLog::new(&log_path)
            .unwrap()
            .load()
            .unwrap()
            .is_empty());
    }
}
