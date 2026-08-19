// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{ShardStatInfo, ShardCanonicalStorageStats};
use crate::types::{ShardId, Status};
mod partitioning;
mod lifecycle;
mod snapshot_ops;
mod node_reports;
mod state_setters;
mod registration;
mod table_ops;
mod topology_helpers;
mod auto_rebalance;
mod failure_detector;
mod raft_failover;
mod shard_check;
mod retention;
use self::partitioning::*;
use self::topology_helpers::*;
pub use self::auto_rebalance::{
    compute_auto_rebalance, AutoRebalanceOptions, ShardReassignment, ShardReassignmentReason,
};
pub use self::failure_detector::{
    plan_conviction, AdaptiveConvictionReport, ConvictionCandidate, ConvictionPlan,
    ConvictionPolicy, DamageSeverity, Diagnosis, FailureDetectorOptions, LocationDamage,
    MetaFailureDetector,
};
pub use self::raft_failover::{compute_raft_failover_triggers, RaftFailoverTrigger};
pub use self::shard_check::{
    ShardCheckOptions, ShardCheckReport, ShardChecker, ShardDivergence,
};
pub use self::retention::{
    plan_meta_retention, MetaRetentionOptions, MetaRetentionPlan, MetaRetentionReport,
    RetentionCandidate,
};

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
pub struct ServerHeartbeatRequest {
    pub server_addr: String,
    #[serde(default)]
    pub boot_time_ms: u64,
    #[serde(default)]
    pub binary_version: String,
    #[serde(default)]
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
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
pub struct ShardStatLoad {
    pub shard_id: ShardId,
    #[serde(alias = "partition_info")]
    pub shard_stat_info: ShardStatInfo,
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
    #[serde(rename = "start_routing_slot")]
    pub start_routing_bucket: u32,
    #[serde(rename = "end_routing_slot")]
    pub end_routing_bucket: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub cache_memory_bytes: u64,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    #[serde(alias = "page_store_bytes_written")]
    pub block_store_bytes_written: u64,
    #[serde(rename = "wal_sequence")]
    pub wal_sequence: u64,
    pub dirty_object_count: u64,
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: u64,
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
    /// The boot time the metaserver anchored on for this server: the first
    /// non-zero boot time it heartbeated after registering. `boot_time_ms`
    /// tracks whatever the latest heartbeat claimed; this one does not move, so
    /// the two disagreeing is the signal that the process restarted.
    #[serde(default)]
    pub reported_boot_time_ms: u64,
    /// Set once the server heartbeats a boot time different from the anchored
    /// one, meaning the process restarted without re-registering. Sticky until
    /// the server registers again, because a restarted datanode has lost the
    /// shards the metaserver still believes it is serving.
    #[serde(default)]
    pub reboot_detected: bool,
    /// Set once this server has been seen reporting `shard_states` at all.
    /// Until then an empty report is indistinguishable from an old build that
    /// does not send them, so the shard-divergence check declines to judge it.
    #[serde(default)]
    pub reports_shard_states: bool,
    pub binary_version: String,
    pub shard_loads: Vec<ShardLoad>,
    #[serde(default)]
    #[serde(alias = "partition_loads")]
    pub shard_stat_loads: Vec<ShardStatLoad>,
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
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeleteTableRequest {
    pub namespace: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpdateTableRequest {
    pub namespace: String,
    pub table_name: String,
    #[serde(default)]
    pub shard_count: Option<u64>,
    #[serde(default)]
    pub replica_count: Option<u64>,
    #[serde(default)]
    pub first_shard_id: Option<ShardId>,
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
    pub partition_version: u32,
    #[serde(default)]
    pub serving_options: TableServingOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableShard {
    pub shard_id: ShardId,
    #[serde(rename = "start_slot")]
    pub start_bucket: u64,
    #[serde(rename = "end_slot")]
    pub end_bucket: u64,
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
    #[serde(alias = "partitions")]
    pub shards: Vec<TableShard>,
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
    RegisterProxy(RegisterProxyRequest),
    AddNamespace(AddNamespaceRequest),
    AddTable(AddTableRequest),
    DeleteTable(DeleteTableRequest),
    UpdateTable(UpdateTableRequest),
    FreezeTable(DeleteTableRequest),
    UnfreezeTable(DeleteTableRequest),
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
    namespaces: BTreeMap<String, MetaEntityState>,
    tables: BTreeMap<String, TableRecord>,
    counters: MetaCounters,
    next_table_id: u64,
    topology_version: u64,
    topology_events: VecDeque<TopologyChangeEvent>,
    scheduler_finish_generations: BTreeMap<String, u64>,
    /// When each dropped resource was dropped, keyed `server:<addr>`,
    /// `proxy:<addr>` or `table:<namespace.table>`. Dropping previously recorded
    /// no timestamp at all, so "dropped long enough ago to forget" was not
    /// expressible; retention ages against this. Kept beside the resources
    /// rather than inside them so the wire types are unchanged.
    dropped_since_ms: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaSnapshot {
    pub format_version: u32,
    pub created_at_ms: u64,
    pub shards: HashMap<ShardId, ShardLocation>,
    pub servers: BTreeMap<String, ServerMetaInfo>,
    pub proxies: BTreeMap<String, ProxyMetaInfo>,
    pub namespaces: BTreeMap<String, MetaEntityState>,
    pub tables: Vec<TableMetaInfo>,
    pub stats: MetaStats,
    pub next_table_id: u64,
    pub topology_version: u64,
    #[serde(default)]
    pub scheduler_finish_generations: BTreeMap<String, u64>,
    /// Drop timestamps for the tombstones in this snapshot. Carried so a peer
    /// that installs the snapshot can keep ageing them instead of restarting
    /// every tombstone's clock.
    #[serde(default)]
    pub dropped_since_ms: BTreeMap<String, u64>,
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

fn table_key(namespace: &str, table_name: &str) -> String {
    format!("{namespace}/{table_name}")
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
            shard_stat_loads: vec![ShardStatLoad {
                shard_id: 7,
                shard_stat_info: crate::control::ShardStatInfo {
                    shard_id: 7,
                    loaded: true,
                    readonly: false,
                    load_version: 11,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_bucket: 1,
                    end_routing_bucket: 2,
                    total_records: 10,
                    storage_bytes: 100,
                    object_manager: crate::control::ObjectManagerStats {
                        object_count: 10,
                        page_ref_count: 10,
                        dirty_object_count: 1,
                        dirty_bucket_count: 1,
                        routing_bucket_count: 2,
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
                start_routing_bucket: 1,
                end_routing_bucket: 2,
                total_records: 10,
                storage_bytes: 100,
                cache_memory_bytes: 64,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 100,
                wal_sequence: 9,
                dirty_object_count: 1,
                dirty_bucket_count: 1,
            }],
        });
        assert!(heartbeat.status.ok);
        assert_eq!(heartbeat.topology_version, meta.stats().topology_version);
        assert_eq!(heartbeat.server_state, "normal");
        let server = meta.list_servers().servers.remove(0);
        assert_eq!(server.shard_loads[0].key_count, 10);
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.table_name, "tbl");
        assert_eq!(server.shard_stat_loads[0].shard_stat_info.storage_bytes, 100);
        assert_eq!(server.runtime_load.queue_depth, 2);
        assert_eq!(server.runtime_load.storage_lifecycle_runs, 8);
        assert_eq!(server.runtime_load.last_meta_topology_version, 12);
        assert_eq!(server.runtime_load.meta_heartbeat_consecutive_failures, 2);
        assert_eq!(
            server.runtime_load.degraded_reasons,
            vec!["background_queue_full"]
        );
        assert_eq!(server.shard_states[0].serving_state, "serving");
        assert_eq!(server.shard_states[0].wal_sequence, 9);
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
    fn metaserver_detects_a_datanode_that_restarted_in_place() {
        // The heartbeats never stop, so nothing in the stale-timeout path notices
        // this. But the restarted node has dropped every shard the metaserver
        // still believes it serves, and reads routed there will all miss.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });

        let heartbeat = |boot_time_ms: u64| ServerHeartbeatRequest {
            server_addr: "node-a".to_string(),
            boot_time_ms,
            binary_version: "v1".to_string(),
            shard_loads: Vec::new(),
            shard_stat_loads: Vec::new(),
            runtime_load: ServerRuntimeLoad::default(),
            shard_states: Vec::new(),
        };
        let server_state = || {
            meta.list_servers()
                .servers
                .into_iter()
                .find(|server| server.server_addr == "node-a")
                .expect("registered")
        };

        // The first heartbeat anchors the boot time; repeats of it are normal.
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(!server_state().reboot_detected);
        assert!(meta.server_heartbeat(heartbeat(1_000)).status.ok);
        assert!(!server_state().reboot_detected);

        // A different boot time means the process restarted underneath us.
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);
        // The anchor does not follow the new value, so the verdict is sticky
        // rather than resetting itself on the next beat.
        assert_eq!(server_state().reported_boot_time_ms, 1_000);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert!(server_state().reboot_detected);

        // Re-registering is how a datanode says it is ready to be trusted again.
        assert!(meta
            .register_server(RegisterServerRequest {
                server_addr: "node-a".to_string(),
                node_id: 1,
                location: "rack-1".to_string(),
                binary_version: "v1".to_string(),
            })
            .status
            .ok);
        assert!(!server_state().reboot_detected);
        assert_eq!(server_state().reported_boot_time_ms, 0);
        assert!(meta.server_heartbeat(heartbeat(2_000)).status.ok);
        assert_eq!(server_state().reported_boot_time_ms, 2_000);
        assert!(!server_state().reboot_detected);
    }

    #[test]
    fn a_datanode_that_never_reports_a_boot_time_is_not_flagged() {
        // Older datanodes send 0. Treating that as a changed boot time would
        // convict the entire fleet on upgrade.
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "node-a".to_string(),
            node_id: 1,
            location: "rack-1".to_string(),
            binary_version: "v1".to_string(),
        });
        for boot_time_ms in [0, 0, 0] {
            assert!(meta
                .server_heartbeat(ServerHeartbeatRequest {
                    server_addr: "node-a".to_string(),
                    boot_time_ms,
                    binary_version: "v1".to_string(),
                    shard_loads: Vec::new(),
                    shard_stat_loads: Vec::new(),
                    runtime_load: ServerRuntimeLoad::default(),
                    shard_states: Vec::new(),
                })
                .status
                .ok);
        }
        let server = meta
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == "node-a")
            .expect("registered");
        assert!(!server.reboot_detected);
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
            shard_stat_loads: Vec::new(),
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
        assert_eq!(topo.shards.len(), 2);
        assert_eq!(topo.shards[0].primary.as_deref(), Some("s1"));
        assert_eq!(topo.shards[0].replicas.len(), 2);
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
        assert!(unchanged.shards.is_empty());
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
        assert!(topology.shards.is_empty());

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
        assert!(topology.shards.is_empty());

        let update = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(2),
            replica_count: None,
            first_shard_id: None,
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
        assert_eq!(topology.shards.len(), 1);
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
        assert_eq!(topology.shards.len(), 4);
        assert_eq!(topology.shards[3].shard_id, 103);

        let unchanged = meta.update_table(UpdateTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            shard_count: Some(4),
            replica_count: Some(2),
            first_shard_id: None,
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "runtime_load".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "serving_state".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
            vec!["zone-a-cool".to_string(), "zone-b-hot".to_string()]
        );
        assert_eq!(topo.shards[0].primary.as_deref(), Some("zone-a-cool"));
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
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
                shard_stat_loads: Vec::new(),
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
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        });

        let topo = meta.get_table_topology(GetTableTopologyRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            old_topology_version: 0,
        });
        assert_eq!(
            topo.shards[0].replicas,
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
            shard_stat_loads: Vec::new(),
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
                start_routing_bucket: 0,
                end_routing_bucket: 1024,
                total_records: 1,
                storage_bytes: 10,
                cache_memory_bytes: 1,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 10,
                wal_sequence: 1,
                dirty_object_count: 0,
                dirty_bucket_count: 0,
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
            shard_stat_loads: Vec::new(),
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
                start_routing_bucket: 0,
                end_routing_bucket: 2048,
                total_records: 2,
                storage_bytes: 20,
                cache_memory_bytes: 2,
                storage: ShardCanonicalStorageStats::default(),
                block_store_bytes_written: 20,
                wal_sequence: 2,
                dirty_object_count: 0,
                dirty_bucket_count: 0,
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
            shard_stat_loads: Vec::new(),
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
        assert_eq!(recovered_topology.shards.len(), 2);
        assert_eq!(
            recovered_topology.shards[0].primary.as_deref(),
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
        assert_eq!(topology.shards.len(), 2);
        assert_eq!(topology.shards[0].primary.as_deref(), Some("server-a"));
        assert_eq!(
            topology.shards[0].replicas,
            vec!["server-a".to_string(), "server-b".to_string()]
        );
        assert_eq!(
            recovered.stats().topology_version,
            snapshot.topology_version
        );
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
