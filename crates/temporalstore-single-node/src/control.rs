use serde::{Deserialize, Serialize};

use crate::cache::CacheStats;
use crate::page_store::PageStoreStats;
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
    pub replica_node_ids: Vec<u64>,
    pub leader_node_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetInfoResponse {
    pub status: Status,
    pub info: Option<ShardInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStats {
    pub shard_id: ShardId,
    pub string_records: usize,
    pub hash_records: usize,
    pub set_records: usize,
    pub feature_records: usize,
    pub sequence_records: usize,
    pub ips_records: usize,
    pub risk_records: usize,
    pub cache: CacheStats,
    pub page_store: PageStoreStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GetStatsResponse {
    pub status: Status,
    pub stats: Option<ShardStats>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Index,
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
