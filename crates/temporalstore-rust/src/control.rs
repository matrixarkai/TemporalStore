use serde::{Deserialize, Serialize};

use crate::cache::CacheStats;
use crate::oplog::OplogStats;
use crate::page_store::PageStoreStats;
use crate::types::{BatchExecuteResponse, Command, ExecuteResponse};
use crate::types::{ShardId, Status};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub version: u64,
    pub maxmemory_bytes: Option<u64>,
    pub read_qps: Option<u64>,
    pub write_qps: Option<u64>,
    pub feature_max_size: usize,
    pub async_storage: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            maxmemory_bytes: None,
            read_qps: None,
            write_qps: None,
            feature_max_size: 5000,
            async_storage: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    pub shard_uri: String,
    pub start_routing_slot: u32,
    pub end_routing_slot: u32,
    pub readonly: bool,
    pub table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardRequest {
    pub shard_id: ShardId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnloadShardResponse {
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SetConfigRequest {
    pub shard_id: ShardId,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetConfigResponse {
    pub status: Status,
    pub config: Config,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MembershipUpdateRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardInfo {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_slot: u32,
    pub end_routing_slot: u32,
    pub readonly: bool,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    #[serde(default)]
    pub membership_version: u64,
    #[serde(default)]
    pub replica_membership_version: u64,
    #[serde(default = "default_membership_valid")]
    pub membership_valid: bool,
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
}

fn default_membership_valid() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetInfoResponse {
    pub status: Status,
    pub info: Option<ShardInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectManagerStats {
    pub object_count: usize,
    pub page_ref_count: usize,
    pub dirty_object_count: usize,
    pub dirty_slot_count: usize,
    pub routing_slot_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionInfoStats {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub table_name: String,
    pub shard_uri: String,
    pub start_routing_slot: u32,
    pub end_routing_slot: u32,
    pub total_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub loaded: bool,
    pub readonly: bool,
    pub load_version: u64,
    pub total_records: usize,
    pub string_records: usize,
    pub hash_records: usize,
    pub set_records: usize,
    pub feature_records: usize,
    pub sequence_records: usize,
    pub ips_records: usize,
    pub risk_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
    pub partition_info: PartitionInfoStats,
    pub cache: CacheStats,
    pub page_store: PageStoreStats,
    pub oplog: OplogStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetStatsResponse {
    pub status: Status,
    pub stats: Option<ShardStats>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedExecuteRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    pub command: Command,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedExecuteResponse {
    pub status: Status,
    pub response: ExecuteResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedBatchExecuteRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    pub commands: Vec<Command>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckedBatchExecuteResponse {
    pub status: Status,
    pub response: BatchExecuteResponse,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Index,
    IndexLog,
    Oplog,
    Page,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReadRequest {
    pub shard_id: ShardId,
    pub stream_kind: StreamKind,
    pub page_segment_id: u64,
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReadResponse {
    pub status: Status,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanStreamRequest {
    pub shard_id: ShardId,
    pub stream_kind: StreamKind,
    pub page_segment_id: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub max_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamRecord {
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScanStreamResponse {
    pub status: Status,
    pub records: Vec<StreamRecord>,
    pub end_of_stream: bool,
}
