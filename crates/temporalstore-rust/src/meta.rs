use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize};

use crate::control::{PartitionInfoStats, ShardCanonicalStorageStats};
use crate::partition_id::{validate_partition_set_count, PartitionId, MAX_TABLE_ID};
use crate::types::{ShardId, Status};
use rustmtcache::CacheStats;

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

impl MetaEntityState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Frozen => "frozen",
            Self::Dropped => "dropped",
        }
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
pub struct UpdateServerRequest {
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
    #[serde(default)]
    pub partition_loads: Vec<PartitionLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerHeartbeatResponse {
    pub status: Status,
    pub forbid_auto_register: bool,
    #[serde(default)]
    pub topology_version: u64,
    #[serde(default)]
    pub server_state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardLoad {
    pub shard_id: ShardId,
    pub key_count: u64,
    pub memory_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionLoad {
    pub shard_id: ShardId,
    pub partition_info: PartitionInfoStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerRuntimeLoad {
    pub queue_depth: usize,
    pub background_queue_depth: usize,
    pub queued_shard_count: usize,
    pub running_shard_count: usize,
    pub dirty_object_count: usize,
    pub dirty_shard_count: usize,
    pub rejected_total: u64,
    pub rejected_background_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
    pub dump_runs: u64,
    pub compaction_runs: u64,
    pub gc_runs: u64,
    pub storage_lifecycle_runs: u64,
    #[serde(default)]
    pub last_meta_topology_version: u64,
    #[serde(default)]
    pub meta_heartbeat_consecutive_failures: u64,
    #[serde(default)]
    pub meta_forbid_auto_register: bool,
    #[serde(default)]
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerShardServingState {
    pub shard_id: ShardId,
    pub serving_state: String,
    pub worker_index: usize,
    pub worker_threads: usize,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_slot: u32,
    pub end_routing_slot: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub cache_memory_bytes: u64,
    #[serde(default)]
    pub cache: CacheStats,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    #[serde(alias = "page_store_bytes_written")]
    pub block_store_bytes_written: u64,
    pub oplog_sequence: u64,
    pub dirty_object_count: u64,
    pub dirty_slot_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerMetaInfo {
    pub server_addr: String,
    pub node_id: u64,
    pub location: String,
    pub state: MetaEntityState,
    pub last_heartbeat_ms: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    pub boot_time_ms: u64,
    pub binary_version: String,
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    pub partition_loads: Vec<PartitionLoad>,
    #[serde(default)]
    pub runtime_load: ServerRuntimeLoad,
    #[serde(default)]
    pub shard_states: Vec<ServerShardServingState>,
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
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModePolicy {
    #[serde(default)]
    pub server_freeze_cooldown_ms: u64,
    #[serde(default)]
    pub proxy_freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SafeModeReport {
    pub status: Status,
    pub blocked_servers: Vec<String>,
    pub blocked_proxies: Vec<String>,
    pub server_count: usize,
    pub proxy_count: usize,
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
    #[serde(default)]
    pub serving_mode: String,
    #[serde(default)]
    pub drop_percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyMetaInfo {
    pub proxy_addr: String,
    pub namespace: String,
    pub location: String,
    pub state: MetaEntityState,
    pub config_version: u64,
    pub last_heartbeat_ms: u64,
    #[serde(default)]
    pub frozen_since_ms: u64,
    #[serde(default)]
    pub freeze_cooldown_until_ms: u64,
    pub binary_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProxyGroupMetaInfo {
    #[serde(rename = "namespace_name", alias = "namespace")]
    pub namespace: String,
    #[serde(default)]
    pub placement: serde_json::Value,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default)]
    pub proxies: Vec<ProxyMetaInfo>,
    #[serde(default)]
    pub instance_num: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PutProxyGroupRequest {
    pub info: ProxyGroupMetaInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DropProxyGroupRequest {
    #[serde(rename = "namespace_name", alias = "namespace")]
    pub namespace: String,
    #[serde(default)]
    pub placement: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListProxyGroupRequest {
    #[serde(default)]
    #[serde(rename = "namespace_name", alias = "namespace")]
    pub namespace: String,
    #[serde(default)]
    pub placement: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ListProxyGroupResponse {
    pub status: Status,
    pub groups: Vec<ProxyGroupMetaInfo>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManagementInfo {
    #[serde(default)]
    pub readonly: bool,
    #[serde(default)]
    pub reserved_namespace_name_list: Vec<String>,
    #[serde(default)]
    pub reserved_table_name_list: Vec<String>,
    #[serde(default)]
    pub reserved_consul_name_list: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateManageInfoRequest {
    pub info: ManagementInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AddNamespaceRequest {
    #[serde(alias = "name", alias = "namespace_name")]
    pub namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NamespaceMetaInfo {
    pub namespace: String,
    pub table_count: usize,
    pub state: MetaEntityState,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
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
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

impl<'de> Deserialize<'de> for AddTableRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawAddTableRequest {
            #[serde(default, alias = "namespace_name")]
            namespace: String,
            #[serde(default, alias = "name")]
            table_name: String,
            #[serde(default)]
            first_shard_id: Option<ShardId>,
            #[serde(default, alias = "partition_set_num")]
            shard_count: Option<u64>,
            #[serde(default = "default_replica_count")]
            replica_count: u64,
            #[serde(default)]
            use_cpp_partition_ids: Option<bool>,
            #[serde(default)]
            partition_version: u32,
            #[serde(default)]
            serving_options: TableServingOptions,
            #[serde(default)]
            partition_units: Vec<serde_json::Value>,
        }

        let raw = RawAddTableRequest::deserialize(deserializer)?;
        let cpp_shape = raw.use_cpp_partition_ids.unwrap_or(false)
            || !raw.partition_units.is_empty()
            || raw.first_shard_id.is_none();
        Ok(Self {
            namespace: raw.namespace,
            table_name: raw.table_name,
            first_shard_id: raw.first_shard_id.unwrap_or_default(),
            shard_count: raw.shard_count.unwrap_or(0),
            replica_count: raw.replica_count,
            use_cpp_partition_ids: raw.use_cpp_partition_ids.unwrap_or(cpp_shape),
            partition_version: raw.partition_version,
            serving_options: raw.serving_options,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteTableRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    #[serde(alias = "name")]
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionStateChangeRequest {
    #[serde(rename = "partition_id", alias = "shard_id")]
    pub partition_id: ShardId,
    #[serde(default)]
    pub force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    #[serde(alias = "name")]
    pub table_name: String,
    #[serde(default, alias = "partition_set_num")]
    pub shard_count: Option<u64>,
    #[serde(default)]
    pub replica_count: Option<u64>,
    #[serde(default)]
    pub first_shard_id: Option<ShardId>,
    #[serde(default)]
    pub use_cpp_partition_ids: Option<bool>,
    #[serde(default)]
    pub partition_version: Option<u32>,
    #[serde(default)]
    pub serving_options: Option<TableServingOptionsPatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptions {
    #[serde(default = "default_pin_primary")]
    pub pin_primary: bool,
    #[serde(default = "default_replica_read_policy")]
    pub replica_read_policy: String,
    #[serde(default)]
    pub preferred_location: String,
    #[serde(default)]
    pub drop_percent: u8,
    #[serde(default = "default_max_read_retries")]
    pub max_read_retries: u32,
    #[serde(default)]
    pub max_write_retries: u32,
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,
    #[serde(default = "default_continuous_failed_time_ms")]
    pub continuous_failed_time_ms: u64,
    #[serde(default = "default_table_io_timeout_ms")]
    pub io_timeout_ms: u64,
    #[serde(default = "default_table_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
}

impl Default for TableServingOptions {
    fn default() -> Self {
        Self {
            pin_primary: default_pin_primary(),
            replica_read_policy: default_replica_read_policy(),
            preferred_location: String::new(),
            drop_percent: 0,
            max_read_retries: default_max_read_retries(),
            max_write_retries: 0,
            retry_backoff_ms: default_retry_backoff_ms(),
            continuous_failed_time_ms: default_continuous_failed_time_ms(),
            io_timeout_ms: default_table_io_timeout_ms(),
            connect_timeout_ms: default_table_connect_timeout_ms(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableServingOptionsPatch {
    #[serde(default)]
    pub pin_primary: Option<bool>,
    #[serde(default)]
    pub replica_read_policy: Option<String>,
    #[serde(default)]
    pub preferred_location: Option<String>,
    #[serde(default)]
    pub drop_percent: Option<u8>,
    #[serde(default)]
    pub max_read_retries: Option<u32>,
    #[serde(default)]
    pub max_write_retries: Option<u32>,
    #[serde(default)]
    pub retry_backoff_ms: Option<u64>,
    #[serde(default)]
    pub continuous_failed_time_ms: Option<u64>,
    #[serde(default)]
    pub io_timeout_ms: Option<u64>,
    #[serde(default)]
    pub connect_timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetTableTopologyRequest {
    #[serde(alias = "namespace_name")]
    pub namespace: String,
    #[serde(alias = "name")]
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
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TablePartition {
    pub shard_id: ShardId,
    #[serde(default)]
    pub state: MetaEntityState,
    pub start_slot: u64,
    pub end_slot: u64,
    pub primary: Option<String>,
    pub replicas: Vec<String>,
    #[serde(default)]
    pub primary_endpoint: Option<ServerEndpoint>,
    #[serde(default)]
    pub replica_endpoints: Vec<ServerEndpoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerEndpoint {
    pub server_addr: String,
    #[serde(default)]
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableTopologyResponse {
    pub status: Status,
    pub table: Option<TableMetaInfo>,
    pub partitions: Vec<TablePartition>,
    pub unchanged: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionRequest {
    #[serde(default)]
    pub old_topology_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyVersionReport {
    pub status: Status,
    pub current_topology_version: u64,
    pub old_topology_version: u64,
    pub unchanged: bool,
    pub server_count: usize,
    pub proxy_count: usize,
    pub table_count: usize,
    pub shard_route_count: usize,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub dropped_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_proxies: usize,
    pub normal_tables: usize,
    pub frozen_tables: usize,
    pub dropped_tables: usize,
    pub changed_tables: Vec<TableMetaInfo>,
    #[serde(default)]
    pub events: Vec<TopologyChangeEvent>,
    #[serde(default)]
    pub event_history_truncated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TopologyChangeEvent {
    pub topology_version: u64,
    pub timestamp_ms: u64,
    pub kind: String,
    pub resource: String,
    pub detail: String,
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
    #[serde(default)]
    pub freeze_cooldown_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadFinishRequest {
    pub server_addr: String,
    pub shard_id: ShardId,
    pub load_version: u64,
    pub status: Status,
    #[serde(default)]
    pub scheduler_task_id: Option<u64>,
    #[serde(default)]
    pub scheduler_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaControlPlaneParityReport {
    pub status: Status,
    pub table_topology_ready: bool,
    pub transitional_state_model_ready: bool,
    pub topology_history_ready: bool,
    pub scheduler_owned_finish_load_ready: bool,
    pub scheduler_generation_check_ready: bool,
    pub durable_replay_ready: bool,
    pub real_data_node_coordination_ready: bool,
    pub scheduler_finish_generation_count: usize,
    pub topology_event_count: usize,
    pub topology_version: u64,
    pub blockers: Vec<String>,
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
    #[serde(default)]
    pub management_info: ManagementInfo,
    #[serde(default)]
    pub manage_info: ManagementInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaPreflightReport {
    pub status: Status,
    pub stats: MetaStats,
    pub normal_servers: usize,
    pub frozen_servers: usize,
    pub normal_proxies: usize,
    pub frozen_proxies: usize,
    pub dropped_tables: usize,
    pub shard_routes: usize,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "request", rename_all = "snake_case")]
pub enum MetaMutation {
    RegisterShard(RegisterShardRequest),
    PublishShardSnapshot(PublishShardSnapshotRequest),
    RegisterServer(RegisterServerRequest),
    UpdateServer(UpdateServerRequest),
    RegisterProxy(RegisterProxyRequest),
    PutProxyGroup(PutProxyGroupRequest),
    DropProxyGroup(DropProxyGroupRequest),
    UpdateManageInfo(UpdateManageInfoRequest),
    MuteMetaChange,
    ResumeMetaChange,
    AddNamespace(AddNamespaceRequest),
    AddTable(AddTableRequest),
    DeleteTable(DeleteTableRequest),
    UpdateTable(UpdateTableRequest),
    FreezeTable(DeleteTableRequest),
    UnfreezeTable(DeleteTableRequest),
    FreezePartition(PartitionStateChangeRequest),
    DropPartition(PartitionStateChangeRequest),
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

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TableRecord {
    info: TableMetaInfo,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct MetaState {
    shards: HashMap<ShardId, ShardLocation>,
    servers: BTreeMap<String, ServerMetaInfo>,
    proxies: BTreeMap<String, ProxyMetaInfo>,
    proxy_groups: BTreeMap<String, ProxyGroupMetaInfo>,
    management_info: ManagementInfo,
    namespaces: BTreeMap<String, MetaEntityState>,
    partition_states: BTreeMap<ShardId, MetaEntityState>,
    tables: BTreeMap<String, TableRecord>,
    counters: MetaCounters,
    next_table_id: u64,
    topology_version: u64,
    topology_events: VecDeque<TopologyChangeEvent>,
    scheduler_finish_generations: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshot {
    pub format_version: u32,
    pub created_at_ms: u64,
    pub shards: HashMap<ShardId, ShardLocation>,
    pub servers: BTreeMap<String, ServerMetaInfo>,
    pub proxies: BTreeMap<String, ProxyMetaInfo>,
    #[serde(default)]
    pub proxy_groups: BTreeMap<String, ProxyGroupMetaInfo>,
    #[serde(default)]
    pub management_info: ManagementInfo,
    pub namespaces: BTreeMap<String, MetaEntityState>,
    #[serde(default)]
    pub partition_states: BTreeMap<ShardId, MetaEntityState>,
    pub tables: Vec<TableMetaInfo>,
    pub stats: MetaStats,
    pub next_table_id: u64,
    pub topology_version: u64,
    #[serde(default)]
    pub scheduler_finish_generations: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotResponse {
    pub status: Status,
    pub snapshot: Option<MetaSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshotFileResponse {
    pub status: Status,
    pub path: String,
    pub snapshot: Option<MetaSnapshot>,
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

    pub fn export_snapshot(&self) -> MetaSnapshot {
        let state = self.inner.read().expect("meta lock poisoned");
        MetaSnapshot::from_state(&state)
    }

    pub(crate) fn state_from_snapshot(snapshot: MetaSnapshot) -> Result<MetaState, Status> {
        if snapshot.format_version != 1 {
            return Err(Status::error(
                "bad_snapshot",
                "unsupported metaserver snapshot version",
            ));
        }
        let mut tables = BTreeMap::new();
        let mut next_table_id = snapshot.next_table_id.max(1);
        for table in snapshot.tables {
            if table.namespace.is_empty() || table.table_name.is_empty() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains invalid table name",
                ));
            }
            next_table_id = next_table_id.max(table.table_id.saturating_add(1));
            let key = table_key(&table.namespace, &table.table_name);
            if tables.insert(key, TableRecord { info: table }).is_some() {
                return Err(Status::error(
                    "bad_snapshot",
                    "snapshot contains duplicate table",
                ));
            }
        }
        Ok(MetaState {
            shards: snapshot.shards,
            servers: snapshot.servers,
            proxies: snapshot.proxies,
            proxy_groups: snapshot.proxy_groups,
            management_info: snapshot.management_info,
            namespaces: snapshot.namespaces,
            partition_states: snapshot.partition_states,
            tables,
            counters: counters_from_stats(&snapshot.stats),
            next_table_id,
            topology_version: snapshot.topology_version,
            topology_events: VecDeque::new(),
            scheduler_finish_generations: snapshot.scheduler_finish_generations,
        })
    }

    pub fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        let state = match Self::state_from_snapshot(snapshot) {
            Ok(state) => state,
            Err(status) => return AckResponse { status },
        };
        *self.inner.write().expect("meta lock poisoned") = state;
        AckResponse {
            status: Status::ok(),
        }
    }
}

impl MetaSnapshot {
    pub(crate) fn from_state(state: &MetaState) -> Self {
        MetaSnapshot {
            format_version: 1,
            created_at_ms: now_ms(),
            shards: state.shards.clone(),
            servers: state.servers.clone(),
            proxies: state.proxies.clone(),
            proxy_groups: state.proxy_groups.clone(),
            management_info: state.management_info.clone(),
            namespaces: state.namespaces.clone(),
            partition_states: state.partition_states.clone(),
            tables: state
                .tables
                .values()
                .map(|table| table.info.clone())
                .collect(),
            stats: stats_from_state(&state),
            next_table_id: state.next_table_id,
            topology_version: state.topology_version,
            scheduler_finish_generations: state.scheduler_finish_generations.clone(),
        }
    }
}

impl SingleNodeMeta {
    pub fn save_snapshot(&self, path: impl AsRef<Path>) -> io::Result<MetaSnapshot> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let snapshot = self.export_snapshot();
        let tmp_path = path.with_extension(format!(
            "{}.tmp",
            path.extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("json")
        ));
        {
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&tmp_path)?;
            serde_json::to_writer_pretty(&mut file, &snapshot).map_err(io::Error::other)?;
            file.write_all(b"\n")?;
            file.sync_data()?;
        }
        fs::rename(&tmp_path, path)?;
        Ok(snapshot)
    }

    pub fn load_snapshot_from_file(path: impl AsRef<Path>) -> io::Result<MetaSnapshot> {
        let file = OpenOptions::new().read(true).open(path)?;
        serde_json::from_reader(file).map_err(io::Error::other)
    }

    pub fn install_snapshot_from_file(&self, path: impl AsRef<Path>) -> io::Result<AckResponse> {
        let snapshot = Self::load_snapshot_from_file(path)?;
        Ok(self.install_snapshot(snapshot))
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
        record_topology_event(
            &mut state,
            "register_shard",
            format!("shard:{}", request.shard_id),
            format!("server_addr={}", request.server_addr),
        );
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
        record_topology_event(
            &mut state,
            "publish_shard_snapshot",
            format!("shard:{}", request.shard_id),
            "latest_snapshot_updated",
        );
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
        if let Some(existing) = state.servers.get(&request.server_addr) {
            let now = now_ms();
            if existing.state == MetaEntityState::Frozen && existing.freeze_cooldown_until_ms > now
            {
                return AckResponse {
                    status: Status::error("resource_frozen", "server is in freeze cooldown"),
                };
            }
        }
        let now = now_ms();
        let server_addr = request.server_addr.clone();
        state.servers.insert(
            server_addr.clone(),
            ServerMetaInfo {
                server_addr: request.server_addr,
                node_id: request.node_id,
                location: request.location,
                state: MetaEntityState::Normal,
                last_heartbeat_ms: now,
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
                boot_time_ms: 0,
                binary_version: request.binary_version,
                shard_loads: Vec::new(),
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            },
        );
        record_topology_event(
            &mut state,
            "register_server",
            format!("server:{server_addr}"),
            "state=normal",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn update_server(&self, request: UpdateServerRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UpdateServer(request.clone()));
        self.apply_update_server(request)
    }

    fn apply_update_server(&self, request: UpdateServerRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get_mut(&request.server_addr) else {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        };
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error("failed_precondition", "server state is not normal"),
            };
        }
        if request.node_id > 0 {
            server.node_id = request.node_id;
        }
        if !request.location.is_empty() {
            server.location = request.location;
        }
        if !request.binary_version.is_empty() {
            server.binary_version = request.binary_version;
        }
        record_topology_event(
            &mut state,
            "update_server",
            format!("server:{}", request.server_addr),
            "metadata_updated",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.server_heartbeat_total += 1;
        let topology_version = state.topology_version;
        let Some(server) = state.servers.get_mut(&request.server_addr) else {
            return ServerHeartbeatResponse {
                status: Status::error("not_found", "server not found"),
                forbid_auto_register: false,
                topology_version,
                server_state: "unknown".to_string(),
            };
        };
        if server.state == MetaEntityState::Frozen {
            return ServerHeartbeatResponse {
                status: Status::error("resource_frozen", "server frozen"),
                forbid_auto_register: true,
                topology_version,
                server_state: MetaEntityState::Frozen.as_str().to_string(),
            };
        }
        server.last_heartbeat_ms = now_ms();
        server.boot_time_ms = request.boot_time_ms;
        if !request.binary_version.is_empty() {
            server.binary_version = request.binary_version;
        }
        server.shard_loads = request.shard_loads;
        server.partition_loads = request.partition_loads;
        server.runtime_load = request.runtime_load;
        server.shard_states = request.shard_states;
        ServerHeartbeatResponse {
            status: Status::ok(),
            forbid_auto_register: false,
            topology_version,
            server_state: server.state.as_str().to_string(),
        }
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        self.record_mutation(MetaMutation::RegisterProxy(request.clone()));
        self.apply_register_proxy(request)
    }

    fn apply_register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.counters.proxy_register_total += 1;
        if let Some(existing) = state.proxies.get(&request.proxy_addr) {
            let now = now_ms();
            if existing.state == MetaEntityState::Frozen && existing.freeze_cooldown_until_ms > now
            {
                return AckResponse {
                    status: Status::error("resource_frozen", "proxy is in freeze cooldown"),
                };
            }
        }
        state.proxies.insert(
            request.proxy_addr.clone(),
            ProxyMetaInfo {
                proxy_addr: request.proxy_addr,
                namespace: request.namespace,
                location: request.location,
                state: MetaEntityState::Normal,
                config_version: request.config_version,
                last_heartbeat_ms: now_ms(),
                frozen_since_ms: 0,
                freeze_cooldown_until_ms: 0,
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
                serving_mode: String::new(),
                drop_percent: 0,
            };
        };
        if proxy.state == MetaEntityState::Frozen {
            return ProxyHeartbeatResponse {
                status: Status::error("resource_frozen", "proxy frozen"),
                config_changed: true,
                namespace: proxy.namespace.clone(),
                config_version: proxy.config_version,
                serving_mode: proxy_serving_mode_for_state(proxy.state).to_string(),
                drop_percent: 0,
            };
        }
        proxy.last_heartbeat_ms = now_ms();
        if !request.binary_version.is_empty() {
            proxy.binary_version = request.binary_version;
        }
        let serving_mode = proxy_serving_mode_for_state(proxy.state).to_string();
        let config_changed = proxy.namespace != request.namespace
            || proxy.config_version > request.config_version
            || proxy.state != MetaEntityState::Normal;
        ProxyHeartbeatResponse {
            status: Status::ok(),
            config_changed,
            namespace: proxy.namespace.clone(),
            config_version: proxy.config_version,
            serving_mode,
            drop_percent: 0,
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
        if let Err(err) = validate_serving_options(&request.serving_options) {
            return AckResponse {
                status: Status::error("bad_request", err),
            };
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
        let namespace = request.namespace.clone();
        let table_name = request.table_name.clone();
        let topology_version = record_topology_event(
            &mut state,
            "add_table",
            format!("table:{namespace}/{table_name}"),
            format!(
                "shards={},replicas={}",
                request.shard_count,
                request.replica_count.max(1)
            ),
        );
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
            topology_version,
            first_shard_id,
            shard_count: request.shard_count,
            replica_count: request.replica_count.max(1),
            use_cpp_partition_ids: request.use_cpp_partition_ids,
            partition_version: request.partition_version,
            serving_options: request.serving_options,
        };
        state.tables.insert(key, TableRecord { info });
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::DeleteTable(request.clone()));
        self.apply_delete_table(request)
    }

    fn apply_delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(current_state) = state.tables.get(&key).map(|table| table.info.state) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if current_state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table already dropped"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "delete_table",
            format!("table:{}/{}", request.namespace, request.table_name),
            "state=dropped",
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after state check");
        table.info.state = MetaEntityState::Dropped;
        table.info.topology_version = topology_version;
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn freeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::FreezeTable(request.clone()));
        self.apply_set_table_state(request, MetaEntityState::Frozen)
    }

    pub fn unfreeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UnfreezeTable(request.clone()));
        self.apply_set_table_state(request, MetaEntityState::Normal)
    }

    pub fn freeze_partition(&self, request: PartitionStateChangeRequest) -> AckResponse {
        self.record_mutation(MetaMutation::FreezePartition(request.clone()));
        self.apply_set_partition_state(request, MetaEntityState::Frozen)
    }

    pub fn drop_partition(&self, request: PartitionStateChangeRequest) -> AckResponse {
        self.record_mutation(MetaMutation::DropPartition(request.clone()));
        self.apply_set_partition_state(request, MetaEntityState::Dropped)
    }

    pub fn update_table(&self, request: UpdateTableRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UpdateTable(request.clone()));
        self.apply_update_table(request)
    }

    fn apply_update_table(&self, request: UpdateTableRequest) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        if request.shard_count.is_none()
            && request.replica_count.is_none()
            && request.first_shard_id.is_none()
            && request.use_cpp_partition_ids.is_none()
            && request.partition_version.is_none()
            && request.serving_options.is_none()
        {
            return AckResponse {
                status: Status::error("bad_request", "at least one table option is required"),
            };
        }

        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(existing) = state.tables.get(&key).map(|table| table.info.clone()) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if existing.state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table is dropped"),
            };
        }
        if existing.state == MetaEntityState::Frozen {
            return AckResponse {
                status: Status::error("resource_frozen", "table is frozen"),
            };
        }
        if matches!(request.shard_count, Some(0)) {
            return AckResponse {
                status: Status::error("bad_request", "shard_count must be > 0"),
            };
        }
        if let Some(shard_count) = request.shard_count {
            if shard_count < existing.shard_count {
                return AckResponse {
                    status: Status::error("bad_request", "shard_count cannot shrink"),
                };
            }
            if existing.use_cpp_partition_ids {
                if shard_count > u32::MAX as u64 {
                    return AckResponse {
                        status: Status::error(
                            "bad_request",
                            "shard_count exceeds C++ partition-set range",
                        ),
                    };
                }
                if let Err(err) = validate_partition_set_count(shard_count as u32) {
                    return AckResponse {
                        status: Status::error("bad_request", err.to_string()),
                    };
                }
            }
        }
        if let Some(use_cpp_partition_ids) = request.use_cpp_partition_ids {
            if use_cpp_partition_ids != existing.use_cpp_partition_ids {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "use_cpp_partition_ids cannot change after table creation",
                    ),
                };
            }
        }
        if let Some(partition_version) = request.partition_version {
            if partition_version != existing.partition_version {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "partition_version cannot change after table creation",
                    ),
                };
            }
        }
        if let Some(first_shard_id) = request.first_shard_id {
            if existing.use_cpp_partition_ids {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "first_shard_id is derived for C++ partition-id tables",
                    ),
                };
            }
            if first_shard_id != existing.first_shard_id {
                return AckResponse {
                    status: Status::error(
                        "bad_request",
                        "first_shard_id cannot change after table creation",
                    ),
                };
            }
        }

        let new_shard_count = request.shard_count.unwrap_or(existing.shard_count);
        let new_replica_count = request
            .replica_count
            .map(|replica_count| replica_count.max(1))
            .unwrap_or(existing.replica_count);
        let new_serving_options = request
            .serving_options
            .as_ref()
            .map(|patch| apply_serving_options_patch(existing.serving_options.clone(), patch))
            .unwrap_or_else(|| existing.serving_options.clone());
        if let Err(err) = validate_serving_options(&new_serving_options) {
            return AckResponse {
                status: Status::error("bad_request", err),
            };
        }
        let changed = new_shard_count != existing.shard_count
            || new_replica_count != existing.replica_count
            || new_serving_options != existing.serving_options;
        if !changed {
            return AckResponse {
                status: Status::error("not_modified", "table options are unchanged"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "update_table",
            format!("table:{}/{}", request.namespace, request.table_name),
            format!("shards={new_shard_count},replicas={new_replica_count}"),
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after update validation");
        table.info.shard_count = new_shard_count;
        table.info.replica_count = new_replica_count;
        table.info.serving_options = new_serving_options;
        table.info.topology_version = topology_version;
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
        if table.info.state == MetaEntityState::Dropped {
            return TableTopologyResponse {
                status: Status::error("table_not_found", "table is dropped"),
                table: Some(table.info.clone()),
                partitions: Vec::new(),
                unchanged: false,
            };
        }
        if table.info.state == MetaEntityState::Frozen {
            return TableTopologyResponse {
                status: Status::error("resource_frozen", "table is frozen"),
                table: Some(table.info.clone()),
                partitions: Vec::new(),
                unchanged: false,
            };
        }
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
                    .filter(|table| {
                        table.info.namespace == *namespace
                            && table.info.state != MetaEntityState::Dropped
                    })
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

    pub fn put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        self.record_mutation(MetaMutation::PutProxyGroup(request.clone()));
        self.apply_put_proxy_group(request)
    }

    fn apply_put_proxy_group(&self, request: PutProxyGroupRequest) -> AckResponse {
        if request.info.namespace.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace is required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        match state.namespaces.get(&request.info.namespace) {
            Some(MetaEntityState::Normal) => {}
            Some(MetaEntityState::Frozen) => {
                return AckResponse {
                    status: Status::error("resource_frozen", "namespace is frozen"),
                };
            }
            Some(MetaEntityState::Dropped) => {
                return AckResponse {
                    status: Status::error("namespace_not_found", "namespace is dropped"),
                };
            }
            None => {
                return AckResponse {
                    status: Status::error("namespace_not_found", "namespace not found"),
                };
            }
        }
        let key = proxy_group_key(&request.info.namespace, &request.info.placement);
        state.proxy_groups.insert(key, request.info.clone());
        record_topology_event(
            &mut state,
            "proxy_group_put",
            format!("namespace:{}", request.info.namespace),
            "proxy_group_updated",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn drop_proxy_group(&self, request: DropProxyGroupRequest) -> AckResponse {
        self.record_mutation(MetaMutation::DropProxyGroup(request.clone()));
        self.apply_drop_proxy_group(request)
    }

    fn apply_drop_proxy_group(&self, request: DropProxyGroupRequest) -> AckResponse {
        if request.namespace.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace is required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = proxy_group_key(&request.namespace, &request.placement);
        if state.proxy_groups.remove(&key).is_none() {
            return AckResponse {
                status: Status::error("not_found", "proxy group not found"),
            };
        }
        record_topology_event(
            &mut state,
            "proxy_group_drop",
            format!("namespace:{}", request.namespace),
            "proxy_group_dropped",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn list_proxy_groups(&self, request: ListProxyGroupRequest) -> ListProxyGroupResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        if !request.namespace.is_empty() {
            match state.namespaces.get(&request.namespace) {
                Some(MetaEntityState::Normal) | Some(MetaEntityState::Frozen) => {}
                Some(MetaEntityState::Dropped) | None => {
                    return ListProxyGroupResponse {
                        status: Status::error("namespace_not_found", "namespace not found"),
                        groups: Vec::new(),
                    };
                }
            }
        }
        let groups = state
            .proxy_groups
            .values()
            .filter(|group| request.namespace.is_empty() || group.namespace == request.namespace)
            .filter(|group| {
                request
                    .placement
                    .as_ref()
                    .map(|placement| group.placement == *placement)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        ListProxyGroupResponse {
            status: Status::ok(),
            groups,
        }
    }

    pub fn update_manage_info(&self, request: UpdateManageInfoRequest) -> AckResponse {
        self.record_mutation(MetaMutation::UpdateManageInfo(request.clone()));
        self.apply_update_manage_info(request)
    }

    fn apply_update_manage_info(&self, request: UpdateManageInfoRequest) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.management_info = request.info;
        record_topology_event(
            &mut state,
            "manage_info_update",
            "management".to_string(),
            "management_info_updated",
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn mute_meta_change(&self) -> AckResponse {
        self.record_mutation(MetaMutation::MuteMetaChange);
        self.apply_set_meta_change_readonly(true)
    }

    pub fn resume_meta_change(&self) -> AckResponse {
        self.record_mutation(MetaMutation::ResumeMetaChange);
        self.apply_set_meta_change_readonly(false)
    }

    fn apply_set_meta_change_readonly(&self, readonly: bool) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        state.management_info.readonly = readonly;
        record_topology_event(
            &mut state,
            if readonly {
                "meta_change_muted"
            } else {
                "meta_change_resumed"
            },
            "management".to_string(),
            format!("readonly={readonly}"),
        );
        AckResponse {
            status: Status::ok(),
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
        self.freeze_stale_resources_with_policy(
            stale_after_ms,
            SafeModePolicy {
                server_freeze_cooldown_ms: 0,
                proxy_freeze_cooldown_ms: 0,
            },
        )
    }

    pub fn freeze_stale_resources_with_policy(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = now_ms();
        let (stale_servers, stale_proxies) = {
            let state = self.inner.read().expect("meta lock poisoned");
            let stale_servers = state
                .servers
                .values()
                .filter(|server| {
                    server.state == MetaEntityState::Normal
                        && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
                })
                .map(|server| server.server_addr.clone())
                .collect::<Vec<_>>();
            let stale_proxies = state
                .proxies
                .values()
                .filter(|proxy| {
                    proxy.state == MetaEntityState::Normal
                        && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
                })
                .map(|proxy| proxy.proxy_addr.clone())
                .collect::<Vec<_>>();
            (stale_servers, stale_proxies)
        };

        let mut frozen_servers = Vec::new();
        for endpoint in stale_servers {
            let response = self.freeze_server(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
            });
            if !response.status.ok {
                return StaleResourceReport {
                    status: response.status,
                    frozen_servers,
                    frozen_proxies: Vec::new(),
                };
            }
            frozen_servers.push(endpoint);
        }

        let mut frozen_proxies = Vec::new();
        for endpoint in stale_proxies {
            let response = self.freeze_proxy(StateChangeRequest {
                endpoint: endpoint.clone(),
                freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
            });
            if !response.status.ok {
                return StaleResourceReport {
                    status: response.status,
                    frozen_servers,
                    frozen_proxies,
                };
            }
            frozen_proxies.push(endpoint);
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
            frozen_proxies,
        }
    }

    pub fn safe_mode_report(&self) -> SafeModeReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let now = now_ms();
        let blocked_servers = state
            .servers
            .values()
            .filter(|server| {
                server.state == MetaEntityState::Frozen && server.freeze_cooldown_until_ms > now
            })
            .map(|server| server.server_addr.clone())
            .collect::<Vec<_>>();
        let blocked_proxies = state
            .proxies
            .values()
            .filter(|proxy| {
                proxy.state == MetaEntityState::Frozen && proxy.freeze_cooldown_until_ms > now
            })
            .map(|proxy| proxy.proxy_addr.clone())
            .collect::<Vec<_>>();
        SafeModeReport {
            status: Status::ok(),
            blocked_servers,
            blocked_proxies,
            server_count: state.servers.len(),
            proxy_count: state.proxies.len(),
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
        self.set_server_state(request, MetaEntityState::Frozen)
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        self.set_server_state(request, MetaEntityState::Dropped)
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Frozen)
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        self.set_proxy_state(request, MetaEntityState::Dropped)
    }

    pub fn server_notify_stop(&self, request: StateChangeRequest) -> AckResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let Some(server) = state.servers.get(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        };
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error("failed_precondition", "state not normal"),
            };
        }
        drop(state);
        self.drop_server(request)
    }

    pub fn proxy_notify_stop(&self, request: StateChangeRequest) -> AckResponse {
        let state = self.inner.read().expect("meta lock poisoned");
        let Some(proxy) = state.proxies.get(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        };
        if proxy.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error("failed_precondition", "state not normal"),
            };
        }
        drop(state);
        self.drop_proxy(request)
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
        let Some(server) = state.servers.get(&request.server_addr) else {
            return AckResponse {
                status: Status::error(
                    "server_not_found",
                    "server must register before finish_load",
                ),
            };
        };
        if server.state != MetaEntityState::Normal {
            return AckResponse {
                status: Status::error("resource_frozen", "server is not serving"),
            };
        }
        if let Some(newer_state) = server
            .shard_states
            .iter()
            .filter(|state| state.shard_id == request.shard_id)
            .map(|state| state.load_version)
            .max()
            .filter(|load_version| *load_version > request.load_version)
        {
            return AckResponse {
                status: Status::error(
                    "stale_load_version",
                    format!(
                        "finish_load version {} is older than server-reported version {newer_state}",
                        request.load_version
                    ),
                ),
            };
        }
        if request.scheduler_task_id.is_some() && request.scheduler_generation.is_none() {
            return AckResponse {
                status: Status::error(
                    "scheduler_generation_required",
                    "scheduler-owned finish_load must include scheduler_generation",
                ),
            };
        }
        if let Some(generation) = request.scheduler_generation {
            let generation_key =
                scheduler_finish_generation_key(request.shard_id, &request.server_addr);
            if let Some(previous) = state.scheduler_finish_generations.get(&generation_key) {
                if generation < *previous {
                    return AckResponse {
                        status: Status::error(
                            "stale_scheduler_generation",
                            format!(
                                "finish_load scheduler_generation {generation} is older than accepted generation {previous}"
                            ),
                        ),
                    };
                }
            }
        }
        if let Some(table) = table_for_shard(&state, request.shard_id) {
            if table.info.state == MetaEntityState::Dropped {
                return AckResponse {
                    status: Status::error("table_not_found", "table is dropped"),
                };
            }
            if table.info.state == MetaEntityState::Frozen {
                return AckResponse {
                    status: Status::error("resource_frozen", "table is frozen"),
                };
            }
        }
        let latest_snapshot = state
            .shards
            .get(&request.shard_id)
            .and_then(|location| location.latest_snapshot.clone());
        let server_addr = request.server_addr.clone();
        state.shards.insert(
            request.shard_id,
            ShardLocation {
                shard_id: request.shard_id,
                server_addr: server_addr.clone(),
                latest_snapshot,
            },
        );
        if let Some(generation) = request.scheduler_generation {
            let generation_key = scheduler_finish_generation_key(request.shard_id, &server_addr);
            state
                .scheduler_finish_generations
                .entry(generation_key)
                .and_modify(|previous| *previous = (*previous).max(generation))
                .or_insert(generation);
        }
        record_topology_event(
            &mut state,
            "finish_load",
            format!("shard:{}", request.shard_id),
            format!("server_addr={server_addr}"),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    pub fn control_plane_parity_report(&self) -> MetaControlPlaneParityReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let table_topology_ready = !state.tables.is_empty()
            && state
                .tables
                .values()
                .all(|table| table.info.shard_count > 0 && table.info.replica_count > 0);
        let transitional_state_model_ready = state
            .tables
            .values()
            .any(|table| table.info.state != MetaEntityState::Normal)
            || state
                .servers
                .values()
                .any(|server| server.state != MetaEntityState::Normal)
            || state
                .proxies
                .values()
                .any(|proxy| proxy.state != MetaEntityState::Normal)
            || state.topology_events.iter().any(|event| {
                matches!(
                    event.kind.as_str(),
                    "table_state" | "server_state" | "proxy_state"
                )
            });
        let topology_history_ready =
            state.topology_version > 0 && !state.topology_events.is_empty();
        let scheduler_owned_finish_load_ready = !state.scheduler_finish_generations.is_empty();
        let scheduler_generation_check_ready = scheduler_owned_finish_load_ready;
        let durable_replay_ready = self.mutation_log.is_some();
        let real_data_node_coordination_ready = state.servers.values().any(|server| {
            !server.shard_states.is_empty() || !server.runtime_load.degraded_reasons.is_empty()
        });

        let mut blockers = Vec::new();
        if !table_topology_ready {
            blockers.push("table/shard topology evidence missing".to_string());
        }
        if !transitional_state_model_ready {
            blockers.push("transitional table/server/proxy state evidence missing".to_string());
        }
        if !topology_history_ready {
            blockers.push("topology history evidence missing".to_string());
        }
        if !scheduler_owned_finish_load_ready {
            blockers.push("scheduler-owned finish_load token evidence missing".to_string());
        }
        if !durable_replay_ready {
            blockers.push("durable mutation log replay evidence missing".to_string());
        }
        if !real_data_node_coordination_ready {
            blockers.push(
                "real data-node heartbeat/lifecycle coordination evidence missing".to_string(),
            );
        }

        MetaControlPlaneParityReport {
            status: if blockers.is_empty() {
                Status::ok()
            } else {
                Status::error(
                    "metaserver_control_plane_parity_blocked",
                    blockers.join("; "),
                )
            },
            table_topology_ready,
            transitional_state_model_ready,
            topology_history_ready,
            scheduler_owned_finish_load_ready,
            scheduler_generation_check_ready,
            durable_replay_ready,
            real_data_node_coordination_ready,
            scheduler_finish_generation_count: state.scheduler_finish_generations.len(),
            topology_event_count: state.topology_events.len(),
            topology_version: state.topology_version,
            blockers,
        }
    }

    pub fn info(&self) -> MetaInfo {
        let state = self.inner.read().expect("meta lock poisoned");
        let management_info = state.management_info.clone();
        MetaInfo {
            status: Status::ok(),
            stats: stats_from_state(&state),
            boot_time_ms: self.boot_time_ms,
            durable_mutation_log: self.mutation_log.is_some(),
            management_info: management_info.clone(),
            manage_info: management_info,
        }
    }

    pub fn stats(&self) -> MetaStats {
        let state = self.inner.read().expect("meta lock poisoned");
        stats_from_state(&state)
    }

    pub fn preflight_report(&self) -> MetaPreflightReport {
        let state = self.inner.read().expect("meta lock poisoned");
        let normal_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .count();
        let frozen_servers = state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .count();
        let normal_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Normal)
            .count();
        let frozen_proxies = state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .count();
        let dropped_tables = state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Dropped)
            .count();
        let mut degraded_reasons = Vec::new();
        if frozen_servers > 0 {
            degraded_reasons.push("frozen_servers".to_string());
        }
        if frozen_proxies > 0 {
            degraded_reasons.push("frozen_proxies".to_string());
        }
        if normal_servers == 0 && !state.shards.is_empty() {
            degraded_reasons.push("no_normal_servers_for_registered_shards".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        MetaPreflightReport {
            status,
            stats: stats_from_state(&state),
            normal_servers,
            frozen_servers,
            normal_proxies,
            frozen_proxies,
            dropped_tables,
            shard_routes: state.shards.len(),
            degraded_reasons,
        }
    }

    pub fn topology_version_report(
        &self,
        request: TopologyVersionRequest,
    ) -> TopologyVersionReport {
        let state = self.inner.read().expect("meta lock poisoned");
        topology_version_report_from_state(&state, request.old_topology_version)
    }

    fn set_server_state(&self, request: StateChangeRequest, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .servers
            .contains_key(&request.endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        }
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeServer(request.clone()));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropServer(request.clone()));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_server_state(request, next)
    }

    fn apply_set_server_state(
        &self,
        request: StateChangeRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(server) = state.servers.get_mut(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "server not found"),
            };
        };
        let now = now_ms();
        server.state = next;
        match next {
            MetaEntityState::Frozen => {
                server.frozen_since_ms = now;
                server.freeze_cooldown_until_ms = now.saturating_add(request.freeze_cooldown_ms);
            }
            MetaEntityState::Normal => {
                server.frozen_since_ms = 0;
                server.freeze_cooldown_until_ms = 0;
            }
            MetaEntityState::Dropped => {}
        }
        record_topology_event(
            &mut state,
            "server_state",
            format!("server:{}", request.endpoint),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    fn set_proxy_state(&self, request: StateChangeRequest, next: MetaEntityState) -> AckResponse {
        if !self
            .inner
            .read()
            .expect("meta lock poisoned")
            .proxies
            .contains_key(&request.endpoint)
        {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        }
        match next {
            MetaEntityState::Frozen => {
                self.record_mutation(MetaMutation::FreezeProxy(request.clone()));
            }
            MetaEntityState::Dropped => {
                self.record_mutation(MetaMutation::DropProxy(request.clone()));
            }
            MetaEntityState::Normal => {}
        }
        self.apply_set_proxy_state(request, next)
    }

    fn apply_set_proxy_state(
        &self,
        request: StateChangeRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(proxy) = state.proxies.get_mut(&request.endpoint) else {
            return AckResponse {
                status: Status::error("not_found", "proxy not found"),
            };
        };
        let now = now_ms();
        proxy.state = next;
        match next {
            MetaEntityState::Frozen => {
                proxy.frozen_since_ms = now;
                proxy.freeze_cooldown_until_ms = now.saturating_add(request.freeze_cooldown_ms);
            }
            MetaEntityState::Normal => {
                proxy.frozen_since_ms = 0;
                proxy.freeze_cooldown_until_ms = 0;
            }
            MetaEntityState::Dropped => {}
        }
        record_topology_event(
            &mut state,
            "proxy_state",
            format!("proxy:{}", request.endpoint),
            format!("state={}", next.as_str()),
        );
        AckResponse {
            status: Status::ok(),
        }
    }

    fn apply_set_table_state(
        &self,
        request: DeleteTableRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        if request.namespace.is_empty() || request.table_name.is_empty() {
            return AckResponse {
                status: Status::error("bad_request", "namespace and table_name are required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let key = table_key(&request.namespace, &request.table_name);
        let Some(existing) = state.tables.get(&key).map(|table| table.info.state) else {
            return AckResponse {
                status: Status::error("table_not_found", "table not found"),
            };
        };
        if existing == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("table_not_found", "table is dropped"),
            };
        }
        if existing == next {
            return AckResponse {
                status: Status::error("not_modified", "table state is unchanged"),
            };
        }
        let topology_version = record_topology_event(
            &mut state,
            "table_state",
            format!("table:{}/{}", request.namespace, request.table_name),
            format!("state={}", next.as_str()),
        );
        let table = state
            .tables
            .get_mut(&key)
            .expect("table exists after state validation");
        table.info.state = next;
        table.info.topology_version = topology_version;
        AckResponse {
            status: Status::ok(),
        }
    }

    fn apply_set_partition_state(
        &self,
        request: PartitionStateChangeRequest,
        next: MetaEntityState,
    ) -> AckResponse {
        if request.partition_id == 0 {
            return AckResponse {
                status: Status::error("bad_request", "partition_id is required"),
            };
        }
        let mut state = self.inner.write().expect("meta lock poisoned");
        let Some(table) =
            table_for_shard(&state, request.partition_id).map(|table| table.info.clone())
        else {
            return AckResponse {
                status: Status::error("partition_not_found", "partition not found"),
            };
        };
        if table.state == MetaEntityState::Dropped {
            return AckResponse {
                status: Status::error("partition_not_found", "table is dropped"),
            };
        }
        let existing = state
            .partition_states
            .get(&request.partition_id)
            .copied()
            .unwrap_or(MetaEntityState::Normal);
        match next {
            MetaEntityState::Frozen => {
                if existing == MetaEntityState::Dropped {
                    return AckResponse {
                        status: Status::error("partition_not_found", "partition is dropped"),
                    };
                }
                if existing == MetaEntityState::Frozen {
                    return AckResponse {
                        status: Status::error("not_modified", "partition is already frozen"),
                    };
                }
            }
            MetaEntityState::Dropped => {
                if existing != MetaEntityState::Frozen {
                    return AckResponse {
                        status: Status::error("failed_precondition", "partition is not frozen"),
                    };
                }
            }
            MetaEntityState::Normal => {}
        }
        state.partition_states.insert(request.partition_id, next);
        let topology_version = record_topology_event(
            &mut state,
            "partition_state",
            format!("partition:{}", request.partition_id),
            format!("state={}", next.as_str()),
        );
        let key = table_key(&table.namespace, &table.table_name);
        if let Some(record) = state.tables.get_mut(&key) {
            record.info.topology_version = topology_version;
        }
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
            MetaMutation::UpdateServer(request) => self.apply_update_server(request).status,
            MetaMutation::RegisterProxy(request) => self.apply_register_proxy(request).status,
            MetaMutation::PutProxyGroup(request) => self.apply_put_proxy_group(request).status,
            MetaMutation::DropProxyGroup(request) => self.apply_drop_proxy_group(request).status,
            MetaMutation::UpdateManageInfo(request) => {
                self.apply_update_manage_info(request).status
            }
            MetaMutation::MuteMetaChange => self.apply_set_meta_change_readonly(true).status,
            MetaMutation::ResumeMetaChange => self.apply_set_meta_change_readonly(false).status,
            MetaMutation::AddNamespace(request) => self.apply_add_namespace(request).status,
            MetaMutation::AddTable(request) => self.apply_add_table(request).status,
            MetaMutation::DeleteTable(request) => self.apply_delete_table(request).status,
            MetaMutation::UpdateTable(request) => self.apply_update_table(request).status,
            MetaMutation::FreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::UnfreezeTable(request) => {
                self.apply_set_table_state(request, MetaEntityState::Normal)
                    .status
            }
            MetaMutation::FreezePartition(request) => {
                self.apply_set_partition_state(request, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::DropPartition(request) => {
                self.apply_set_partition_state(request, MetaEntityState::Dropped)
                    .status
            }
            MetaMutation::FinishLoad(request) => self.apply_finish_load(request).status,
            MetaMutation::FreezeServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::DropServer(request) => {
                self.apply_set_server_state(request, MetaEntityState::Dropped)
                    .status
            }
            MetaMutation::FreezeProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Frozen)
                    .status
            }
            MetaMutation::DropProxy(request) => {
                self.apply_set_proxy_state(request, MetaEntityState::Dropped)
                    .status
            }
        }
    }
}

fn scheduler_finish_generation_key(shard_id: ShardId, server_addr: &str) -> String {
    format!("{shard_id}@{server_addr}")
}

fn build_partitions(state: &MetaState, table: &TableMetaInfo) -> Vec<TablePartition> {
    #[derive(Debug)]
    struct PlacementCandidate {
        server_addr: String,
        location: String,
        degraded: bool,
        queue_depth: usize,
        background_queue_depth: usize,
        running_shard_count: usize,
        dirty_object_count: usize,
        dirty_shard_count: usize,
        shard_state_penalty: u8,
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
            let shard_state_penalty = server
                .shard_states
                .iter()
                .map(|state| placement_shard_state_penalty(&state.serving_state))
                .max()
                .unwrap_or_default();
            PlacementCandidate {
                server_addr: server.server_addr.clone(),
                location: server.location.clone(),
                degraded: !server.runtime_load.degraded_reasons.is_empty(),
                queue_depth: server.runtime_load.queue_depth,
                background_queue_depth: server.runtime_load.background_queue_depth,
                running_shard_count: server.runtime_load.running_shard_count,
                dirty_object_count: server.runtime_load.dirty_object_count,
                dirty_shard_count: server.runtime_load.dirty_shard_count,
                shard_state_penalty,
                key_count,
                memory_bytes,
            }
        })
        .collect::<Vec<_>>();
    normal_servers.sort_by(|left, right| {
        (
            left.degraded,
            left.shard_state_penalty,
            left.queue_depth,
            left.background_queue_depth,
            left.running_shard_count,
            left.dirty_object_count,
            left.dirty_shard_count,
            left.key_count,
            left.memory_bytes,
            &left.server_addr,
        )
            .cmp(&(
                right.degraded,
                right.shard_state_penalty,
                right.queue_depth,
                right.background_queue_depth,
                right.running_shard_count,
                right.dirty_object_count,
                right.dirty_shard_count,
                right.key_count,
                right.memory_bytes,
                &right.server_addr,
            ))
    });
    let slot_count = 1_u64 << 30;
    let mut partitions = Vec::new();
    for offset in 0..table.shard_count {
        let shard_id = table_shard_id(table, offset).unwrap_or(table.first_shard_id + offset);
        let partition_state = state
            .partition_states
            .get(&shard_id)
            .copied()
            .unwrap_or(MetaEntityState::Normal);
        if partition_state == MetaEntityState::Dropped {
            continue;
        }
        let start_slot = slot_count * offset / table.shard_count;
        let end_slot = (slot_count * (offset + 1) / table.shard_count).saturating_sub(1);
        let mut replicas = Vec::new();
        let mut seen_replicas = BTreeSet::new();
        let mut used_locations = BTreeSet::new();
        let mut used_hosts = BTreeSet::new();
        if let Some(location) = state.shards.get(&shard_id) {
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
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
            let host = server_host(&candidate.server_addr);
            if !host.is_empty() && used_hosts.contains(&host) {
                continue;
            }
            push_replica(
                state,
                &mut replicas,
                &mut seen_replicas,
                &mut used_locations,
                &mut used_hosts,
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
                &mut used_hosts,
                &candidate.server_addr,
            );
        }
        let primary = state
            .shards
            .get(&shard_id)
            .map(|location| location.server_addr.clone())
            .or_else(|| replicas.first().cloned());
        let primary_endpoint = primary
            .as_ref()
            .map(|server_addr| server_endpoint(state, server_addr));
        let replica_endpoints = replicas
            .iter()
            .map(|server_addr| server_endpoint(state, server_addr))
            .collect();
        partitions.push(TablePartition {
            shard_id,
            state: partition_state,
            start_slot,
            end_slot,
            primary,
            replicas,
            primary_endpoint,
            replica_endpoints,
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

fn table_for_shard<'a>(state: &'a MetaState, shard_id: ShardId) -> Option<&'a TableRecord> {
    state.tables.values().find(|table| {
        (0..table.info.shard_count).any(|offset| {
            table_shard_id(&table.info, offset)
                .map(|candidate| candidate == shard_id)
                .unwrap_or(false)
        })
    })
}

fn push_replica(
    state: &MetaState,
    replicas: &mut Vec<String>,
    seen_replicas: &mut BTreeSet<String>,
    used_locations: &mut BTreeSet<String>,
    used_hosts: &mut BTreeSet<String>,
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
    let host = server_host(server_addr);
    if !host.is_empty() {
        used_hosts.insert(host);
    }
    replicas.push(server_addr.to_string());
}

fn placement_shard_state_penalty(serving_state: &str) -> u8 {
    match serving_state {
        "serving" | "readonly" => 0,
        "queued" | "running" | "loading" => 1,
        "freezing" | "unloading" => 2,
        "failed" => 3,
        _ => 1,
    }
}

fn server_host(server_addr: &str) -> String {
    if let Some(stripped) = server_addr.strip_prefix('[') {
        return stripped
            .split_once(']')
            .map(|(host, _)| host.to_string())
            .unwrap_or_else(|| server_addr.to_string());
    }
    server_addr
        .rsplit_once(':')
        .map(|(host, port)| {
            if port.chars().all(|ch| ch.is_ascii_digit()) {
                host.to_string()
            } else {
                server_addr.to_string()
            }
        })
        .unwrap_or_else(|| server_addr.to_string())
}

fn server_endpoint(state: &MetaState, server_addr: &str) -> ServerEndpoint {
    ServerEndpoint {
        server_addr: server_addr.to_string(),
        location: state
            .servers
            .get(server_addr)
            .map(|server| server.location.clone())
            .unwrap_or_default(),
    }
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
            frozen_since_ms: 0,
            freeze_cooldown_until_ms: 0,
            boot_time_ms: 0,
            binary_version: String::new(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
}

fn stats_from_state(state: &MetaState) -> MetaStats {
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

const TOPOLOGY_EVENT_HISTORY_LIMIT: usize = 256;

fn topology_version_report_from_state(
    state: &MetaState,
    old_topology_version: u64,
) -> TopologyVersionReport {
    let changed_tables = state
        .tables
        .values()
        .filter(|table| table.info.topology_version > old_topology_version)
        .map(|table| table.info.clone())
        .collect::<Vec<_>>();
    let events = state
        .topology_events
        .iter()
        .filter(|event| event.topology_version > old_topology_version)
        .cloned()
        .collect::<Vec<_>>();
    let event_history_truncated = old_topology_version < state.topology_version
        && state
            .topology_events
            .front()
            .is_some_and(|event| old_topology_version < event.topology_version.saturating_sub(1));
    TopologyVersionReport {
        status: Status::ok(),
        current_topology_version: state.topology_version,
        old_topology_version,
        unchanged: old_topology_version >= state.topology_version,
        server_count: state.servers.len(),
        proxy_count: state.proxies.len(),
        table_count: state.tables.len(),
        shard_route_count: state.shards.len(),
        normal_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Normal)
            .count(),
        frozen_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Frozen)
            .count(),
        dropped_servers: state
            .servers
            .values()
            .filter(|server| server.state == MetaEntityState::Dropped)
            .count(),
        normal_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Normal)
            .count(),
        frozen_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Frozen)
            .count(),
        dropped_proxies: state
            .proxies
            .values()
            .filter(|proxy| proxy.state == MetaEntityState::Dropped)
            .count(),
        normal_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Normal)
            .count(),
        frozen_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Frozen)
            .count(),
        dropped_tables: state
            .tables
            .values()
            .filter(|table| table.info.state == MetaEntityState::Dropped)
            .count(),
        changed_tables,
        events,
        event_history_truncated,
    }
}

fn record_topology_event(
    state: &mut MetaState,
    kind: impl Into<String>,
    resource: impl Into<String>,
    detail: impl Into<String>,
) -> u64 {
    state.topology_version += 1;
    state.topology_events.push_back(TopologyChangeEvent {
        topology_version: state.topology_version,
        timestamp_ms: now_ms(),
        kind: kind.into(),
        resource: resource.into(),
        detail: detail.into(),
    });
    while state.topology_events.len() > TOPOLOGY_EVENT_HISTORY_LIMIT {
        state.topology_events.pop_front();
    }
    state.topology_version
}

fn counters_from_stats(stats: &MetaStats) -> MetaCounters {
    MetaCounters {
        register_shard_total: stats.register_shard_total,
        get_shard_total: stats.get_shard_total,
        server_register_total: stats.server_register_total,
        server_heartbeat_total: stats.server_heartbeat_total,
        proxy_register_total: stats.proxy_register_total,
        proxy_heartbeat_total: stats.proxy_heartbeat_total,
        namespace_create_total: stats.namespace_create_total,
        table_create_total: stats.table_create_total,
        topology_query_total: stats.topology_query_total,
        load_finish_total: stats.load_finish_total,
    }
}

fn table_key(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
}

fn proxy_group_key(namespace: &str, placement: &serde_json::Value) -> String {
    let placement = serde_json::to_string(placement).unwrap_or_else(|_| "null".to_string());
    format!("{namespace}/{placement}")
}

fn default_replica_count() -> u64 {
    1
}

fn default_pin_primary() -> bool {
    true
}

fn default_replica_read_policy() -> String {
    "pin_primary".to_string()
}

fn default_max_read_retries() -> u32 {
    1
}

fn default_retry_backoff_ms() -> u64 {
    2
}

fn default_continuous_failed_time_ms() -> u64 {
    10_000
}

fn default_table_io_timeout_ms() -> u64 {
    200
}

fn default_table_connect_timeout_ms() -> u64 {
    200
}

fn validate_serving_options(options: &TableServingOptions) -> Result<(), String> {
    match options.replica_read_policy.as_str() {
        "pin_primary" | "first_replica" | "round_robin_replica" => {}
        _ => {
            return Err(format!(
                "unsupported replica_read_policy {:?}",
                options.replica_read_policy
            ));
        }
    }
    if options.drop_percent > 100 {
        return Err("drop_percent must be <= 100".to_string());
    }
    if options.io_timeout_ms == 0 || options.connect_timeout_ms == 0 {
        return Err("io/connect timeout must be > 0".to_string());
    }
    Ok(())
}

fn proxy_serving_mode_for_state(state: MetaEntityState) -> &'static str {
    match state {
        MetaEntityState::Normal => "serving",
        MetaEntityState::Frozen | MetaEntityState::Dropped => "not_serving",
    }
}

fn apply_serving_options_patch(
    mut options: TableServingOptions,
    patch: &TableServingOptionsPatch,
) -> TableServingOptions {
    if let Some(pin_primary) = patch.pin_primary {
        options.pin_primary = pin_primary;
    }
    if let Some(replica_read_policy) = &patch.replica_read_policy {
        options.replica_read_policy = replica_read_policy.clone();
    }
    if let Some(preferred_location) = &patch.preferred_location {
        options.preferred_location = preferred_location.clone();
    }
    if let Some(drop_percent) = patch.drop_percent {
        options.drop_percent = drop_percent;
    }
    if let Some(max_read_retries) = patch.max_read_retries {
        options.max_read_retries = max_read_retries;
    }
    if let Some(max_write_retries) = patch.max_write_retries {
        options.max_write_retries = max_write_retries;
    }
    if let Some(retry_backoff_ms) = patch.retry_backoff_ms {
        options.retry_backoff_ms = retry_backoff_ms;
    }
    if let Some(continuous_failed_time_ms) = patch.continuous_failed_time_ms {
        options.continuous_failed_time_ms = continuous_failed_time_ms;
    }
    if let Some(io_timeout_ms) = patch.io_timeout_ms {
        options.io_timeout_ms = io_timeout_ms;
    }
    if let Some(connect_timeout_ms) = patch.connect_timeout_ms {
        options.connect_timeout_ms = connect_timeout_ms;
    }
    options
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
        let heartbeat = meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "127.0.0.1:18001".to_string(),
            boot_time_ms: 11,
            binary_version: "v1".to_string(),
            shard_loads: vec![ShardLoad {
                shard_id: 7,
                key_count: 10,
                memory_bytes: 100,
            }],
            partition_loads: vec![PartitionLoad {
                shard_id: 7,
                partition_info: crate::control::PartitionInfoStats {
                    shard_id: 7,
                    loaded: true,
                    readonly: false,
                    load_version: 11,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_slot: 1,
                    end_routing_slot: 2,
                    total_records: 10,
                    storage_bytes: 100,
                    object_manager: crate::control::ObjectManagerStats {
                        object_count: 10,
                        page_ref_count: 10,
                        dirty_object_count: 1,
                        dirty_slot_count: 1,
                        routing_slot_count: 2,
                    },
                },
            }],
            runtime_load: ServerRuntimeLoad {
                queue_depth: 2,
                background_queue_depth: 1,
                queued_shard_count: 1,
                running_shard_count: 1,
                dirty_object_count: 3,
                dirty_shard_count: 1,
                rejected_total: 4,
                rejected_background_total: 1,
                dump_runs: 5,
                compaction_runs: 6,
                gc_runs: 7,
                storage_lifecycle_runs: 8,
                last_meta_topology_version: 12,
                meta_heartbeat_consecutive_failures: 2,
                degraded_reasons: vec!["background_queue_full".to_string()],
                ..ServerRuntimeLoad::default()
            },
            shard_states: vec![ServerShardServingState {
                shard_id: 7,
                serving_state: "serving".to_string(),
                worker_index: 3,
                worker_threads: 4,
                loaded: true,
                readonly: false,
                load_version: 11,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_slot: 1,
                end_routing_slot: 2,
                total_records: 10,
                storage_bytes: 100,
                cache_memory_bytes: 64,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 100,
                oplog_sequence: 9,
                dirty_object_count: 1,
                dirty_slot_count: 1,
            }],
        });
        assert!(heartbeat.status.ok);
        assert_eq!(heartbeat.topology_version, meta.stats().topology_version);
        assert_eq!(heartbeat.server_state, "normal");
        let server = meta.list_servers().servers.remove(0);
        assert_eq!(server.shard_loads[0].key_count, 10);
        assert_eq!(server.partition_loads[0].partition_info.table_name, "tbl");
        assert_eq!(server.partition_loads[0].partition_info.storage_bytes, 100);
        assert_eq!(server.runtime_load.queue_depth, 2);
        assert_eq!(server.runtime_load.storage_lifecycle_runs, 8);
        assert_eq!(server.runtime_load.last_meta_topology_version, 12);
        assert_eq!(server.runtime_load.meta_heartbeat_consecutive_failures, 2);
        assert_eq!(
            server.runtime_load.degraded_reasons,
            vec!["background_queue_full"]
        );
        assert_eq!(server.shard_states[0].serving_state, "serving");
        assert_eq!(server.shard_states[0].oplog_sequence, 9);
        assert_eq!(meta.stats().server_heartbeat_total, 1);
    }

    #[test]
    fn metaserver_preflight_reports_inventory_and_frozen_resources() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.register_server(RegisterServerRequest {
                server_addr: "server-a".to_string(),
                node_id: 7,
                location: "zone-a".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register_proxy(RegisterProxyRequest {
                proxy_addr: "proxy-a".to_string(),
                namespace: "ns".to_string(),
                location: "zone-a".to_string(),
                config_version: 1,
                binary_version: "v1".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.register(RegisterShardRequest {
                shard_id: 1,
                server_addr: "server-a".to_string(),
            })
            .status
            .ok
        );

        let healthy = meta.preflight_report();
        assert!(healthy.status.ok);
        assert_eq!(healthy.normal_servers, 1);
        assert_eq!(healthy.normal_proxies, 1);
        assert_eq!(healthy.shard_routes, 1);
        assert!(healthy.degraded_reasons.is_empty());

        assert!(
            meta.freeze_server(StateChangeRequest {
                endpoint: "server-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );
        let degraded = meta.preflight_report();
        assert!(!degraded.status.ok);
        assert_eq!(degraded.frozen_servers, 1);
        assert_eq!(degraded.frozen_proxies, 1);
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_servers".to_string()));
        assert!(degraded
            .degraded_reasons
            .contains(&"frozen_proxies".to_string()));
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
    fn metaserver_safe_mode_cooldown_blocks_rejoin_and_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("safe-mode-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            server_addr: "cooldown-server".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "cooldown-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "z".to_string(),
            config_version: 1,
            binary_version: "v".to_string(),
        });
        std::thread::sleep(std::time::Duration::from_millis(2));

        let report = meta.freeze_stale_resources_with_policy(
            0,
            SafeModePolicy {
                server_freeze_cooldown_ms: 60_000,
                proxy_freeze_cooldown_ms: 60_000,
            },
        );
        assert!(report.status.ok);
        assert_eq!(report.frozen_servers, vec!["cooldown-server".to_string()]);
        assert_eq!(report.frozen_proxies, vec!["cooldown-proxy".to_string()]);

        let safe_mode = meta.safe_mode_report();
        assert!(safe_mode.status.ok);
        assert_eq!(
            safe_mode.blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            safe_mode.blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );
        assert_eq!(
            meta.register_server(RegisterServerRequest {
                server_addr: "cooldown-server".to_string(),
                node_id: 1,
                location: "z".to_string(),
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );
        assert_eq!(
            meta.register_proxy(RegisterProxyRequest {
                proxy_addr: "cooldown-proxy".to_string(),
                namespace: "ns".to_string(),
                location: "z".to_string(),
                config_version: 1,
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );

        let server_heartbeat = meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "cooldown-server".to_string(),
            boot_time_ms: 1,
            binary_version: "v".to_string(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        });
        assert_eq!(server_heartbeat.status.code, "resource_frozen");
        assert!(server_heartbeat.forbid_auto_register);
        assert_eq!(
            meta.proxy_heartbeat(ProxyHeartbeatRequest {
                proxy_addr: "cooldown-proxy".to_string(),
                namespace: "ns".to_string(),
                config_version: 1,
                binary_version: "v".to_string(),
            })
            .status
            .code,
            "resource_frozen"
        );

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.safe_mode_report().blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            recovered.safe_mode_report().blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );

        let snapshot = meta.export_snapshot();
        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        assert_eq!(
            restored.safe_mode_report().blocked_servers,
            vec!["cooldown-server".to_string()]
        );
        assert_eq!(
            restored.safe_mode_report().blocked_proxies,
            vec!["cooldown-proxy".to_string()]
        );
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
                serving_options: crate::meta::TableServingOptions::default(),
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
        let topology_version = topo.table.as_ref().unwrap().topology_version;
        let report = meta.topology_version_report(TopologyVersionRequest {
            old_topology_version: 0,
        });
        assert!(report.status.ok);
        assert_eq!(
            report.current_topology_version,
            meta.stats().topology_version
        );
        assert_eq!(report.server_count, 2);
        assert_eq!(report.table_count, 1);
        assert_eq!(report.shard_route_count, 1);
        assert_eq!(report.changed_tables.len(), 1);
        assert_eq!(report.changed_tables[0].topology_version, topology_version);
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "register_server" && event.resource == "server:s1"));
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "register_shard" && event.resource == "shard:10"));
        assert!(report
            .events
            .iter()
            .any(|event| event.kind == "add_table" && event.resource == "table:ns/tbl"));

        let unchanged_report = meta.topology_version_report(TopologyVersionRequest {
            old_topology_version: report.current_topology_version,
        });
        assert!(unchanged_report.unchanged);
        assert!(unchanged_report.changed_tables.is_empty());
        assert!(unchanged_report.events.is_empty());

        let unchanged = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: topology_version,
        });
        assert!(unchanged.status.ok);
        assert!(unchanged.unchanged);
        assert!(unchanged.partitions.is_empty());
    }

    #[test]
    fn metaserver_delete_table_marks_dropped_and_rejects_topology() {
        let meta = SingleNodeMeta::default();
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 10,
                shard_count: 2,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        let created_version = meta.list_tables().tables[0].topology_version;

        let deleted = meta.delete_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(deleted.status.ok, "{deleted:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(table.state, MetaEntityState::Dropped);
        assert!(table.topology_version > created_version);
        assert_eq!(meta.list_namespaces().namespaces[0].table_count, 0);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(topology.status.code, "table_not_found");
        assert_eq!(topology.table.unwrap().state, MetaEntityState::Dropped);
        assert!(topology.partitions.is_empty());

        let duplicate = meta.delete_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert_eq!(duplicate.status.code, "table_not_found");
    }

    #[test]
    fn metaserver_freeze_table_blocks_topology_update_and_finish_load_until_unfrozen() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 42,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );

        let frozen = meta.freeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(frozen.status.ok);
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Frozen);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(topology.status.code, "resource_frozen");
        assert!(topology.partitions.is_empty());

        let update = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(2),
            replica_count: None,
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(update.status.code, "resource_frozen");

        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(finish.status.code, "resource_frozen");

        let unfrozen = meta.unfreeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        assert!(unfrozen.status.ok);
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Normal);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.partitions.len(), 1);
        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert!(finish.status.ok);
    }

    #[test]
    fn metaserver_update_table_expands_topology_and_guards_unsafe_changes() {
        let meta = SingleNodeMeta::default();
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        let created = meta.list_tables().tables[0].clone();

        let updated = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(4),
            replica_count: Some(2),
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert!(updated.status.ok, "{updated:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(table.shard_count, 4);
        assert_eq!(table.replica_count, 2);
        assert!(table.topology_version > created.topology_version);

        let topology = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: created.topology_version,
        });
        assert!(topology.status.ok, "{topology:?}");
        assert_eq!(topology.partitions.len(), 4);
        assert_eq!(topology.partitions[3].shard_id, 103);

        let unchanged = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(4),
            replica_count: Some(2),
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(unchanged.status.code, "not_modified");

        let shrink = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(1),
            replica_count: None,
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(shrink.status.code, "bad_request");

        let retarget = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: Some(200),
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(retarget.status.code, "bad_request");

        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
            })
            .status
            .ok
        );
        let dropped_update = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(5),
            replica_count: None,
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: None,
        });
        assert_eq!(dropped_update.status.code, "table_not_found");
    }

    #[test]
    fn metaserver_table_serving_options_update_and_snapshot_round_trip() {
        let meta = SingleNodeMeta::default();
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "opts".to_string(),
                first_shard_id: 10,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: TableServingOptions {
                    pin_primary: false,
                    replica_read_policy: "first_replica".to_string(),
                    preferred_location: "zone-a".to_string(),
                    drop_percent: 7,
                    max_read_retries: 3,
                    max_write_retries: 2,
                    retry_backoff_ms: 11,
                    continuous_failed_time_ms: 22,
                    io_timeout_ms: 333,
                    connect_timeout_ms: 444,
                },
            })
            .status
            .ok
        );
        assert_eq!(
            meta.get_table_topology(GetTableTopologyRequest {
                namespace: "ns".to_string(),
                table_name: "opts".to_string(),
                old_topology_version: 0,
            })
            .table
            .unwrap()
            .serving_options
            .drop_percent,
            7
        );

        let updated = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "opts".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                replica_read_policy: Some("round_robin_replica".to_string()),
                drop_percent: Some(19),
                max_read_retries: Some(4),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert!(updated.status.ok, "{updated:?}");
        let table = meta.list_tables().tables[0].clone();
        assert_eq!(
            table.serving_options.replica_read_policy,
            "round_robin_replica"
        );
        assert_eq!(table.serving_options.drop_percent, 19);
        assert_eq!(table.serving_options.max_read_retries, 4);
        assert_eq!(table.serving_options.max_write_retries, 2);

        let invalid = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "opts".to_string(),
            shard_count: None,
            replica_count: None,
            first_shard_id: None,
            use_cpp_partition_ids: None,
            partition_version: None,
            serving_options: Some(TableServingOptionsPatch {
                replica_read_policy: Some("unknown_policy".to_string()),
                ..TableServingOptionsPatch::default()
            }),
        });
        assert_eq!(invalid.status.code, "bad_request");

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(meta.export_snapshot()).status.ok);
        assert_eq!(
            restored.list_tables().tables[0]
                .serving_options
                .drop_percent,
            19
        );
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
            serving_options: crate::meta::TableServingOptions::default(),
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
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
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
            serving_options: crate::meta::TableServingOptions::default(),
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
    fn metaserver_topology_uses_runtime_load_when_record_load_ties() {
        let meta = SingleNodeMeta::default();
        for (server_addr, queue_depth, dirty_object_count, degraded) in [
            ("busy", 50, 100, false),
            ("cool", 0, 0, false),
            ("degraded", 0, 0, true),
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
                    key_count: 10,
                    memory_bytes: 10,
                }],
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad {
                    queue_depth,
                    dirty_object_count,
                    degraded_reasons: degraded
                        .then(|| "background_queue_full".to_string())
                        .into_iter()
                        .collect(),
                    ..ServerRuntimeLoad::default()
                },
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "runtime_load".to_string(),
            first_shard_id: 400,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "runtime_load".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["cool".to_string(), "busy".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_avoids_unhealthy_shard_serving_states() {
        let meta = SingleNodeMeta::default();
        for (server_addr, serving_state) in [
            ("freezing", "freezing"),
            ("failed", "failed"),
            ("serving", "serving"),
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
                    key_count: 10,
                    memory_bytes: 10,
                }],
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: vec![ServerShardServingState {
                    shard_id: 1,
                    serving_state: serving_state.to_string(),
                    loaded: serving_state != "failed",
                    ..ServerShardServingState::default()
                }],
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "serving_state".to_string(),
            first_shard_id: 500,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "serving_state".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["serving".to_string(), "freezing".to_string()]
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
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
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
            serving_options: crate::meta::TableServingOptions::default(),
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
    fn metaserver_topology_prefers_host_diversity_before_same_host_load() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("10.0.0.1:18001", "zone-a", 10, 10),
            ("10.0.0.1:18002", "zone-b", 20, 20),
            ("10.0.0.2:18001", "zone-c", 10_000, 10_000),
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
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 300,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["10.0.0.1:18001".to_string(), "10.0.0.2:18001".to_string()]
        );
    }

    #[test]
    fn metaserver_topology_fills_same_host_when_distinct_hosts_are_insufficient() {
        let meta = SingleNodeMeta::default();
        for (server_addr, location, key_count, memory_bytes) in [
            ("10.0.0.1:18001", "zone-a", 10, 10),
            ("10.0.0.1:18002", "zone-b", 20, 20),
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
                partition_loads: Vec::new(),
                runtime_load: ServerRuntimeLoad::default(),
                shard_states: Vec::new(),
            });
        }
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 301,
            shard_count: 1,
            replica_count: 2,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.partitions[0].replicas,
            vec!["10.0.0.1:18001".to_string(), "10.0.0.1:18002".to_string()]
        );
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
        assert_eq!(response.serving_mode, "serving");
        assert_eq!(response.drop_percent, 0);
        assert_eq!(meta.list_proxies().proxies[0].binary_version, "v2");

        let frozen = meta.freeze_proxy(StateChangeRequest {
            endpoint: "p1".to_string(),
            freeze_cooldown_ms: 0,
        });
        assert!(frozen.status.ok, "{frozen:?}");
        let response = meta.proxy_heartbeat(ProxyHeartbeatRequest {
            proxy_addr: "p1".to_string(),
            namespace: "ns".to_string(),
            config_version: 3,
            binary_version: "v3".to_string(),
        });
        assert_eq!(response.status.code, "resource_frozen");
        assert_eq!(response.serving_mode, "not_serving");
        assert!(response.config_changed);
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
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert!(ack.status.ok);
        assert_eq!(meta.get(42).location.unwrap().server_addr, "s1");
        assert_eq!(meta.stats().load_finish_total, 1);
    }

    // shared-corpus: control_scheduler_token_stale_rejection;
    #[test]
    fn metaserver_finish_load_enforces_scheduler_generation_and_replays_it() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("scheduler-finish-load.jsonl");
        let snapshot_path = dir.path().join("scheduler-finish-load-snapshot.json");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 42,
                serving_state: "serving".to_string(),
                worker_index: 0,
                worker_threads: 1,
                loaded: true,
                readonly: false,
                load_version: 9,
                table_name: "tbl".to_string(),
                shard_uri: "file:///tmp/shard-42".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 1024,
                total_records: 1,
                storage_bytes: 10,
                cache_memory_bytes: 1,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 10,
                oplog_sequence: 1,
                dirty_object_count: 0,
                dirty_slot_count: 0,
            }],
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 42,
            shard_count: 1,
            replica_count: 1,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        meta.freeze_proxy(StateChangeRequest {
            endpoint: "proxy-does-not-exist".to_string(),
            freeze_cooldown_ms: 0,
        });

        let missing_generation = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(100),
            scheduler_generation: None,
        });
        assert_eq!(
            missing_generation.status.code,
            "scheduler_generation_required"
        );

        let accepted = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(100),
            scheduler_generation: Some(10),
        });
        assert!(accepted.status.ok, "{accepted:?}");

        let stale = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 42,
            load_version: 9,
            status: Status::ok(),
            scheduler_task_id: Some(101),
            scheduler_generation: Some(9),
        });
        assert_eq!(stale.status.code, "stale_scheduler_generation");

        let snapshot = meta.save_snapshot(&snapshot_path).unwrap();
        assert_eq!(
            snapshot.scheduler_finish_generations.get("42@s1").copied(),
            Some(10)
        );

        let recovered_from_log = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered_from_log
                .export_snapshot()
                .scheduler_finish_generations
                .get("42@s1")
                .copied(),
            Some(10)
        );
        assert_eq!(
            recovered_from_log
                .finish_load(LoadFinishRequest {
                    server_addr: "s1".to_string(),
                    shard_id: 42,
                    load_version: 9,
                    status: Status::ok(),
                    scheduler_task_id: Some(102),
                    scheduler_generation: Some(8),
                })
                .status
                .code,
            "stale_scheduler_generation"
        );

        let recovered_from_snapshot = SingleNodeMeta::default();
        assert!(
            recovered_from_snapshot
                .install_snapshot_from_file(&snapshot_path)
                .unwrap()
                .status
                .ok
        );
        assert_eq!(
            recovered_from_snapshot
                .export_snapshot()
                .scheduler_finish_generations
                .get("42@s1")
                .copied(),
            Some(10)
        );
    }

    // shared-corpus: control_metaserver_scheduler_lifecycle_workflow;
    #[test]
    fn metaserver_control_plane_parity_report_covers_scheduler_topology_and_node_coordination() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("control-plane-parity.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 77,
                serving_state: "serving".to_string(),
                worker_index: 0,
                worker_threads: 1,
                loaded: true,
                readonly: false,
                load_version: 5,
                table_name: "tbl".to_string(),
                shard_uri: "file:///tmp/shard-77".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 2048,
                total_records: 2,
                storage_bytes: 20,
                cache_memory_bytes: 2,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 20,
                oplog_sequence: 2,
                dirty_object_count: 0,
                dirty_slot_count: 0,
            }],
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 77,
            shard_count: 1,
            replica_count: 1,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });
        meta.freeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        meta.unfreeze_table(DeleteTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
        });
        let finish = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 77,
            load_version: 5,
            status: Status::ok(),
            scheduler_task_id: Some(200),
            scheduler_generation: Some(20),
        });
        assert!(finish.status.ok, "{finish:?}");

        let report = meta.control_plane_parity_report();
        assert!(report.status.ok, "{report:?}");
        assert!(report.table_topology_ready);
        assert!(report.transitional_state_model_ready);
        assert!(report.topology_history_ready);
        assert!(report.scheduler_owned_finish_load_ready);
        assert!(report.scheduler_generation_check_ready);
        assert!(report.durable_replay_ready);
        assert!(report.real_data_node_coordination_ready);
        assert_eq!(report.scheduler_finish_generation_count, 1);
        assert!(report.topology_event_count >= 4);
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn metaserver_finish_load_rejects_unknown_frozen_and_stale_servers() {
        let meta = SingleNodeMeta::default();
        let missing = meta.finish_load(LoadFinishRequest {
            server_addr: "missing".to_string(),
            shard_id: 7,
            load_version: 1,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(missing.status.code, "server_not_found");

        meta.register_server(RegisterServerRequest {
            server_addr: "s1".to_string(),
            node_id: 1,
            location: "z".to_string(),
            binary_version: "v".to_string(),
        });
        meta.server_heartbeat(ServerHeartbeatRequest {
            server_addr: "s1".to_string(),
            boot_time_ms: 1,
            binary_version: "v".to_string(),
            shard_loads: Vec::new(),
            partition_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: vec![ServerShardServingState {
                shard_id: 7,
                load_version: 9,
                serving_state: "serving".to_string(),
                ..ServerShardServingState::default()
            }],
        });
        let stale = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 7,
            load_version: 8,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(stale.status.code, "stale_load_version");

        meta.freeze_server(StateChangeRequest {
            endpoint: "s1".to_string(),
            freeze_cooldown_ms: 0,
        });
        let frozen = meta.finish_load(LoadFinishRequest {
            server_addr: "s1".to_string(),
            shard_id: 7,
            load_version: 10,
            status: Status::ok(),
            scheduler_task_id: None,
            scheduler_generation: None,
        });
        assert_eq!(frozen.status.code, "resource_frozen");
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
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.update_table(UpdateTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                shard_count: Some(2),
                replica_count: Some(2),
                first_shard_id: None,
                use_cpp_partition_ids: None,
                partition_version: None,
                serving_options: None,
            })
            .status
            .ok
        );
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped_tbl".to_string(),
                first_shard_id: 11,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        assert!(
            meta.delete_table(DeleteTableRequest {
                namespace: "ns".to_string(),
                table_name: "dropped_tbl".to_string(),
            })
            .status
            .ok
        );
        assert!(
            meta.freeze_proxy(StateChangeRequest {
                endpoint: "proxy-a".to_string(),
                freeze_cooldown_ms: 0,
            })
            .status
            .ok
        );

        let mutations = LocalMetaMutationLog::new(&log_path)
            .unwrap()
            .load()
            .unwrap();
        assert_eq!(mutations.len(), 9);

        let recovered = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert_eq!(
            recovered.get(10).location.unwrap(),
            ShardLocation {
                shard_id: 10,
                server_addr: "server-a".to_string(),
                latest_snapshot: None,
            }
        );
        let recovered_tables = recovered.list_tables().tables;
        assert_eq!(recovered_tables.len(), 2);
        assert_eq!(
            recovered_tables
                .iter()
                .find(|table| table.table_name == "dropped_tbl")
                .unwrap()
                .state,
            MetaEntityState::Dropped
        );
        assert_eq!(recovered.list_namespaces().namespaces[0].table_count, 1);
        let recovered_topology = recovered.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(recovered_topology.table.unwrap().replica_count, 2);
        assert_eq!(recovered_topology.partitions.len(), 2);
        assert_eq!(
            recovered_topology.partitions[0].primary.as_deref(),
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
    fn metaserver_snapshot_round_trips_full_metabase_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-snapshot.json");
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_server(RegisterServerRequest {
            server_addr: "server-b".to_string(),
            node_id: 2,
            location: "zone-b".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 77,
            server_addr: "server-a".to_string(),
        });
        let latest_snapshot = ShardSnapshotRef {
            uri: "s3://cluster/shards/77/snapshots/2-19/manifest.json".to_string(),
            checksum: "sha256:meta".to_string(),
            byte_size: 4096,
            last_log_index: 19,
            created_at_ms: 123,
        };
        meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: 77,
            snapshot: latest_snapshot.clone(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 9,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            endpoint: "proxy-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "ns".to_string(),
        });
        assert!(
            meta.add_table(AddTableRequest {
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                first_shard_id: 77,
                shard_count: 2,
                replica_count: 2,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            })
            .status
            .ok
        );
        let snapshot = meta.save_snapshot(&snapshot_path).unwrap();
        assert_eq!(snapshot.format_version, 1);
        assert_eq!(snapshot.stats.server_count, 2);
        assert_eq!(snapshot.stats.proxy_count, 1);
        assert_eq!(snapshot.stats.table_count, 1);

        let recovered = SingleNodeMeta::default();
        assert!(
            recovered
                .install_snapshot_from_file(&snapshot_path)
                .unwrap()
                .status
                .ok
        );
        assert_eq!(
            recovered.get(77).location.unwrap().latest_snapshot,
            Some(latest_snapshot)
        );
        assert_eq!(
            recovered.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
        let topology = recovered.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert!(topology.status.ok);
        assert_eq!(topology.partitions.len(), 2);
        assert_eq!(topology.partitions[0].primary.as_deref(), Some("server-a"));
        assert_eq!(
            topology.partitions[0].replicas,
            vec!["server-a".to_string(), "server-b".to_string()]
        );
        assert_eq!(
            recovered.stats().topology_version,
            snapshot.topology_version
        );
    }

    #[test]
    fn metaserver_management_info_replays_and_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();

        assert!(
            meta.update_manage_info(UpdateManageInfoRequest {
                info: ManagementInfo {
                    readonly: false,
                    reserved_namespace_name_list: vec!["system".to_string()],
                    reserved_table_name_list: vec!["meta".to_string()],
                    reserved_consul_name_list: vec!["consul-a".to_string()],
                },
            })
            .status
            .ok
        );
        assert!(meta.mute_meta_change().status.ok);
        assert!(meta.info().manage_info.readonly);
        let snapshot = meta.export_snapshot();
        assert_eq!(
            snapshot.management_info.reserved_namespace_name_list,
            vec!["system".to_string()]
        );

        let replayed = SingleNodeMeta::with_mutation_log(&log_path).unwrap();
        assert!(replayed.info().manage_info.readonly);
        assert_eq!(
            replayed.info().manage_info.reserved_table_name_list,
            vec!["meta".to_string()]
        );
        assert!(replayed.resume_meta_change().status.ok);
        assert!(!replayed.info().manage_info.readonly);

        let restored = SingleNodeMeta::default();
        assert!(restored.install_snapshot(snapshot).status.ok);
        assert!(restored.info().manage_info.readonly);
    }

    #[test]
    fn metaserver_mutation_log_ignores_failed_state_changes() {
        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("meta-mutations.jsonl");
        let meta = SingleNodeMeta::with_mutation_log(&log_path).unwrap();

        let missing_server = meta.freeze_server(StateChangeRequest {
            endpoint: "missing-server".to_string(),
            freeze_cooldown_ms: 0,
        });
        let missing_proxy = meta.drop_proxy(StateChangeRequest {
            endpoint: "missing-proxy".to_string(),
            freeze_cooldown_ms: 0,
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
