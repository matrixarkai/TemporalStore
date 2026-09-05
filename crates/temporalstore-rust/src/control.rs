// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::block_store::{BlockStoreBandSummary, BlockStoreStats};
use crate::types::{BatchExecuteResponse, Command, ExecuteResponse};
use crate::types::{ShardId, Status};
use crate::wal::WriteAheadLogStats;
use matrixcache::CacheStats;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Config {
    pub version: u64,
    pub maxmemory_bytes: Option<u64>,
    #[serde(default)]
    pub tenant_name: Option<String>,
    pub read_qps: Option<u64>,
    pub write_qps: Option<u64>,
    #[serde(default)]
    pub table_read_qps: Option<u64>,
    #[serde(default)]
    pub table_write_qps: Option<u64>,
    #[serde(default)]
    pub tenant_read_qps: Option<u64>,
    #[serde(default)]
    pub tenant_write_qps: Option<u64>,
    #[serde(default)]
    pub extend_config: BTreeMap<String, String>,
    pub feature_max_size: usize,
    pub async_storage: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            version: 1,
            maxmemory_bytes: None,
            tenant_name: None,
            read_qps: None,
            write_qps: None,
            table_read_qps: None,
            table_write_qps: None,
            tenant_read_qps: None,
            tenant_write_qps: None,
            extend_config: BTreeMap::new(),
            // Default retained points (and default read bound) per feature/sequence timeline.
            // Doubled from the historical 5000 to better serve long-sequence feature use cases
            // out of the box; still overridable per shard via set_config.
            feature_max_size: 10000,
            async_storage: false,
        }
    }
}

impl Config {
    /// Truthy check for an `extend_config` gate flag.
    pub fn flag(&self, name: &str) -> bool {
        self.flag_or(name, false)
    }

    /// `flag`, for a gate whose default is ON. An absent key takes `default`; a key that is
    /// present is read exactly as `flag` reads it, so an explicit false value opts out.
    pub fn flag_or(&self, name: &str, default: bool) -> bool {
        match self.extend_config.get(name) {
            None => default,
            Some(v) => matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes" | "enabled"
            ),
        }
    }

    /// Gate for the Control State rollup ladder (O(levels) sum-family window aggregates).
    /// Off by default so the live scan path is unchanged.
    pub fn control_rollup_enabled(&self) -> bool {
        self.flag("control_rollup")
    }

    /// Gate for coalesced Control State counter persistence: skip the per-write whole-series
    /// page rewrite and rely on the index snapshot + WAL replay (same durability model as
    /// control_state_changes/fol). Effective only with async_storage (WAL) on.
    ///
    /// DEFAULT ON. The rewrite it skips costs the length of the series on every increment --
    /// measured at 313,309 bytes per increment on a 3,200-point series against 2,459 coalesced,
    /// a 127x difference that grows without bound. What it relies on instead is covered by
    /// `control_state_coalesced_write_survives_restart_via_wal_replay`, which writes counters,
    /// reopens the engine and requires the counts to come back.
    ///
    /// Set the key to a false value to opt out; the async_storage gate still applies, so a
    /// deployment without a WAL keeps the per-write page rewrite either way.
    pub fn control_coalesce_persist_enabled(&self) -> bool {
        self.flag_or("control_coalesce_persist", true)
    }

    /// Gate for bounded distinct: convert oversized exact CHANGE sets to fixed-size HLL sketches
    /// (approximate distinct counts past the threshold). Off by default so distinct stays exact.
    pub fn control_distinct_sketch_enabled(&self) -> bool {
        self.flag("control_distinct_sketch")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LoadShardRequest {
    pub shard_id: ShardId,
    pub load_version: u64,
    #[serde(default)]
    pub local_node_id: Option<u64>,
    pub shard_uri: String,
    #[serde(rename = "start_routing_slot")]
    pub start_routing_bucket: u32,
    #[serde(rename = "end_routing_slot")]
    pub end_routing_bucket: u32,
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
    #[serde(rename = "start_routing_slot")]
    pub start_routing_bucket: u32,
    #[serde(rename = "end_routing_slot")]
    pub end_routing_bucket: u32,
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
    // True only while the shard's WAL is being replayed on load. keeps a partition in
    // PartitionLoadStage::LOADING (not serving) until the shard load and WAL replay
    // finishes; Rust must likewise refuse client commands during replay so a concurrent
    // write cannot interleave with replay and regress the WAL anchor (double-apply on the
    // next restart) or expose a stale mid-replay read. The replay thread itself bypasses
    // this gate via replaying_wal().
    #[serde(default)]
    pub recovering: bool,
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
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    #[serde(rename = "routing_slot_count")]
    pub routing_bucket_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardStatInfo {
    pub shard_id: ShardId,
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
    pub object_manager: ObjectManagerStats,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardCanonicalStorageStats {
    pub page_index_entries: u64,
    pub block_index_entries: u64,
    pub object_index_entries: u64,
    #[serde(rename = "slot_entries")]
    pub bucket_entries: u64,
    pub storage_zone_count: u64,
    pub active_storage_zones: u64,
    pub sealed_storage_zones: u64,
    #[serde(alias = "stream_segment_count")]
    pub stream_slab_count: u64,
    pub storage_zone_total_bytes: u64,
    pub storage_zone_used_bytes: u64,
    pub storage_zone_stale_bytes: u64,
    pub page_reads: u64,
    pub page_writes: u64,
    pub block_reads: u64,
    pub block_writes: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub append_watermark: u64,
    pub compaction_watermark: u64,
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
    pub control_state_records: usize,
    pub storage_bytes: u64,
    pub object_manager: ObjectManagerStats,
    #[serde(alias = "partition_info")]
    pub shard_stat_info: ShardStatInfo,
    #[serde(default)]
    pub storage: ShardCanonicalStorageStats,
    pub cache: CacheStats,
    #[serde(default)]
    pub page_store: BlockStoreStats,
    #[serde(default)]
    pub page_store_zones: BlockStoreBandSummary,
    pub block_store: BlockStoreStats,
    #[serde(default)]
    #[serde(alias = "block_store_zones")]
    pub block_store_bands: BlockStoreBandSummary,
    pub write_ahead_log: WriteAheadLogStats,
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
    Wal,
    Block,
    Page,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamReadRequest {
    pub shard_id: ShardId,
    pub stream_kind: StreamKind,
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
    #[serde(rename = "page_segment_id")]
    pub page_slab_id: u64,
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
