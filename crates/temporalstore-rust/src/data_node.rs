// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
#[cfg(test)]
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{
    CheckedBatchExecuteRequest, CheckedBatchExecuteResponse, CheckedExecuteRequest,
    CheckedExecuteResponse, LoadShardRequest, LoadShardResponse, UnloadShardRequest,
    UnloadShardResponse,
};
use crate::engine::reports::{
    default_storage_cache_contract, default_storage_cache_contract_empty,
    default_storage_cache_layers, default_storage_cache_semantics,
    default_storage_cold_scan_contract, default_storage_cold_scan_contract_empty,
    default_storage_cold_scan_sequence, default_storage_gc_snapshot,
    default_storage_index_contract, default_storage_index_contract_empty,
    default_storage_index_snapshot, default_storage_lifecycle_metrics,
    default_storage_lifecycle_phases, default_storage_manager_contract,
    default_storage_manager_contract_empty, default_storage_read_contract,
    default_storage_read_contract_empty, default_storage_read_sequence,
    default_storage_reclaim_contract, default_storage_reclaim_contract_empty,
    default_storage_reclaim_scope, default_storage_reclaim_semantics,
    default_storage_safety_snapshot, default_storage_topology_snapshot,
    default_storage_watermark_snapshot, default_storage_write_contract,
    default_storage_write_contract_empty, default_storage_write_sequence,
    effective_storage_tuning_from_env, storage_gc_snapshot_from_metrics,
    storage_index_snapshot_from_metrics, storage_safety_snapshot_from_metrics,
    storage_topology_snapshot_from_metrics, storage_watermark_snapshot_from_metrics,
    PublicStorageContract, PublicStorageFeatureShapes, ShardCompactionModelLayoutReport,
    ShardCompactionUtilityReport, BucketDumpManifest, StorageContractValue, StorageGcSnapshot,
    StorageIndexSnapshot, StorageLifecyclePlan, StorageLifecycleReport, StorageLifecycleRequest,
    StorageManagerCycleReport, StorageManagerCycleRequest, StorageManagerStageReport,
    StorageProductionReadinessPolicy, StorageProductionReadinessReport, StorageReclaimScope,
    StorageSafetySnapshot, StorageTopologySnapshot, StorageWatermarkSnapshot,
};
use crate::engine::TemporalEngine;
use crate::meta::{
    ServerHeartbeatResponse, ServerRuntimeLoad, ServerShardServingState, TableTopologyResponse,
};
use crate::rebalance::SchedulerLifecycleToken;
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, ShardId, Status,
};

mod lifecycle;
mod dirty_tracking;
mod shard_lifecycle;
mod task_execution;
mod runtime_reports;
mod runtime_storage;
mod task_output;
mod worker;
mod storage_manager_runtime;
use self::dirty_tracking::*;
use self::shard_lifecycle::*;
use self::task_execution::*;
use self::task_output::*;
use self::worker::*;
use self::storage_manager_runtime::*;

use lifecycle::{
    final_loaded_lifecycle_state, lifecycle_persistence_report_for_path, lifecycle_snapshot_inner,
    lifecycle_snapshot_path_from_env, persist_lifecycle_snapshot_inner,
    record_lifecycle_state_inner, restore_lifecycle_snapshot_from_path_inner,
    restore_lifecycle_snapshot_inner, shard_has_queued_or_running_work,
    validate_foreground_write_allowed_inner, validate_lifecycle_token_inner,
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeRuntimeOptions {
    pub worker_threads: usize,
    pub max_queue_depth: usize,
    pub max_background_queue_depth: usize,
}

impl Default for DataNodeRuntimeOptions {
    fn default() -> Self {
        Self {
            worker_threads: 4,
            max_queue_depth: 1024,
            max_background_queue_depth: 128,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestController {
    pub timeout_ms: u64,
}

impl Default for RequestController {
    fn default() -> Self {
        Self { timeout_ms: 200 }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataNodeTaskKind {
    Load,
    Reload,
    Unload,
    Execute,
    CheckedExecute,
    Dump,
    Compact,
    Gc,
    StorageManager,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataNodeTaskOutput {
    Load(LoadShardResponse),
    Reload(LoadShardResponse),
    Unload(UnloadShardResponse),
    Execute(ExecuteResponse),
    CheckedExecute(CheckedExecuteResponse),
    Dump(DumpShardResponse),
    Compact(CompactionResponse),
    Gc(GcResponse),
    StorageManager(StorageManagerResponse),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DataNodeTaskStatus {
    pub job_id: u64,
    pub kind: DataNodeTaskKind,
    pub status: Status,
    pub output: Option<DataNodeTaskOutput>,
    pub submitted_at_ms: u64,
    pub finished_at_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeRuntimeStats {
    pub worker_threads: usize,
    pub max_queue_depth: usize,
    pub max_background_queue_depth: usize,
    pub submitted_total: u64,
    pub completed_total: u64,
    pub rejected_total: u64,
    pub rejected_background_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
    pub queue_depth: usize,
    pub background_queue_depth: usize,
    pub queued_shard_count: usize,
    pub running_shard_count: usize,
    pub dirty_object_count: usize,
    pub dirty_shard_count: usize,
    pub expiry_sweeps: u64,
    pub expired_records_removed: u64,
    pub dump_runs: u64,
    pub compaction_runs: u64,
    pub gc_runs: u64,
    #[serde(default)]
    pub storage_lifecycle_runs: u64,
    #[serde(default)]
    pub storage_manager_runs: u64,
    #[serde(default)]
    pub storage_manager_loops: u64,
    #[serde(default)]
    pub storage_manager_prepare_runs: u64,
    #[serde(rename = "storage_manager_reclaim_wal_runs", default)]
    pub storage_manager_reclaim_wal_runs: u64,
    #[serde(default)]
    pub storage_manager_reclaim_memory_runs: u64,
    #[serde(default)]
    pub storage_manager_expire_runs: u64,
    #[serde(default)]
    pub storage_manager_reclaim_page_runs: u64,
    #[serde(default)]
    pub storage_manager_compact_runs: u64,
    #[serde(default)]
    pub storage_manager_index_gc_runs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodePreflightReport {
    pub status: Status,
    pub stats: DataNodeRuntimeStats,
    #[serde(default)]
    pub lifecycle: DataNodeLifecycleReport,
    #[serde(default)]
    pub lifecycle_persistence: DataNodeLifecyclePersistenceReport,
    pub metaserver: DataNodeMetaHeartbeatReport,
    pub topology_validation: DataNodeTopologyValidationReport,
    pub queued_workers: Vec<ShardWorkerInfo>,
    pub dirty_shards: Vec<ShardId>,
    pub dirty_objects: Vec<DirtyObjectInfo>,
    pub degraded_reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeLifecyclePersistenceReport {
    pub enabled: bool,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub last_restore_status: Option<Status>,
    pub last_restore_at_ms: u64,
    pub restore_success_total: u64,
    pub restore_failure_total: u64,
    #[serde(default)]
    pub last_persist_status: Option<Status>,
    pub last_persist_at_ms: u64,
    pub persist_success_total: u64,
    pub persist_failure_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeLifecycleReport {
    #[serde(default)]
    pub public_storage_contract: PublicStorageContract,
    #[serde(default)]
    pub public_storage_feature_shapes: PublicStorageFeatureShapes,
    #[serde(default = "effective_storage_tuning_from_env")]
    pub effective_storage_tuning: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_lifecycle_metrics")]
    pub storage_lifecycle_metrics: BTreeMap<String, u64>,
    #[serde(default = "default_storage_write_contract_empty")]
    pub storage_write_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_read_contract_empty")]
    pub storage_read_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cold_scan_contract_empty")]
    pub storage_cold_scan_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_manager_contract_empty")]
    pub storage_manager_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_index_contract_empty")]
    pub storage_index_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_cache_contract_empty")]
    pub storage_cache_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_reclaim_contract_empty")]
    pub storage_reclaim_contract: BTreeMap<String, StorageContractValue>,
    #[serde(default = "default_storage_safety_snapshot")]
    pub storage_safety_snapshot: StorageSafetySnapshot,
    #[serde(default = "default_storage_watermark_snapshot")]
    pub storage_watermark_snapshot: StorageWatermarkSnapshot,
    #[serde(default = "default_storage_gc_snapshot")]
    pub storage_gc_snapshot: StorageGcSnapshot,
    #[serde(default = "default_storage_index_snapshot")]
    pub storage_index_snapshot: StorageIndexSnapshot,
    #[serde(default = "default_storage_topology_snapshot")]
    pub storage_topology_snapshot: StorageTopologySnapshot,
    #[serde(default = "default_storage_write_sequence")]
    pub storage_write_sequence: Vec<String>,
    #[serde(default = "default_storage_read_sequence")]
    pub storage_read_sequence: Vec<String>,
    #[serde(default = "default_storage_cold_scan_sequence")]
    pub storage_cold_scan_sequence: Vec<String>,
    #[serde(default = "default_storage_lifecycle_phases")]
    pub storage_lifecycle_phases: Vec<String>,
    #[serde(default = "default_storage_cache_layers")]
    pub storage_cache_layers: Vec<String>,
    #[serde(default = "default_storage_cache_semantics")]
    pub storage_cache_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_semantics")]
    pub storage_reclaim_semantics: Vec<String>,
    #[serde(default = "default_storage_reclaim_scope")]
    pub storage_reclaim_scope: StorageReclaimScope,
    pub loaded_shard_count: usize,
    pub serving_count: usize,
    pub readonly_count: usize,
    pub queued_count: usize,
    pub running_count: usize,
    pub unloading_count: usize,
    pub failed_count: usize,
    pub max_load_version: u64,
    pub shards: Vec<ServerShardServingState>,
    #[serde(default)]
    pub transitions: Vec<DataNodeShardLifecycleState>,
}

impl Default for DataNodeLifecycleReport {
    fn default() -> Self {
        Self {
            public_storage_contract: PublicStorageContract::default(),
            public_storage_feature_shapes: PublicStorageFeatureShapes::default(),
            effective_storage_tuning: effective_storage_tuning_from_env(),
            storage_lifecycle_metrics: default_storage_lifecycle_metrics(),
            storage_write_contract: default_storage_write_contract_empty(),
            storage_read_contract: default_storage_read_contract_empty(),
            storage_cold_scan_contract: default_storage_cold_scan_contract_empty(),
            storage_manager_contract: default_storage_manager_contract_empty(),
            storage_index_contract: default_storage_index_contract_empty(),
            storage_cache_contract: default_storage_cache_contract_empty(),
            storage_reclaim_contract: default_storage_reclaim_contract_empty(),
            storage_safety_snapshot: default_storage_safety_snapshot(),
            storage_watermark_snapshot: default_storage_watermark_snapshot(),
            storage_gc_snapshot: default_storage_gc_snapshot(),
            storage_index_snapshot: default_storage_index_snapshot(),
            storage_topology_snapshot: default_storage_topology_snapshot(),
            storage_write_sequence: default_storage_write_sequence(),
            storage_read_sequence: default_storage_read_sequence(),
            storage_cold_scan_sequence: default_storage_cold_scan_sequence(),
            storage_lifecycle_phases: default_storage_lifecycle_phases(),
            storage_cache_layers: default_storage_cache_layers(),
            storage_cache_semantics: default_storage_cache_semantics(),
            storage_reclaim_semantics: default_storage_reclaim_semantics(),
            storage_reclaim_scope: default_storage_reclaim_scope(),
            loaded_shard_count: 0,
            serving_count: 0,
            readonly_count: 0,
            queued_count: 0,
            running_count: 0,
            unloading_count: 0,
            failed_count: 0,
            max_load_version: 0,
            shards: Vec::new(),
            transitions: Vec::new(),
        }
    }
}

fn data_node_storage_lifecycle_metrics(stats: &DataNodeRuntimeStats) -> BTreeMap<String, u64> {
    let mut metrics = default_storage_lifecycle_metrics();
    metrics.insert(
        "storage_manager_prepare_count".to_string(),
        stats.storage_manager_prepare_runs,
    );
    metrics.insert(
        "storage_manager_reclaim_count".to_string(),
        stats
            .storage_manager_reclaim_wal_runs
            .saturating_add(stats.storage_manager_reclaim_memory_runs)
            .saturating_add(stats.storage_manager_reclaim_page_runs),
    );
    metrics.insert(
        "storage_manager_evict_count".to_string(),
        stats.storage_manager_reclaim_memory_runs,
    );
    metrics.insert(
        "storage_manager_expire_count".to_string(),
        stats
            .storage_manager_expire_runs
            .saturating_add(stats.expiry_sweeps),
    );
    metrics.insert(
        "storage_manager_page_gc_count".to_string(),
        stats
            .storage_manager_reclaim_page_runs
            .saturating_add(stats.gc_runs),
    );
    metrics.insert(
        "storage_manager_block_gc_count".to_string(),
        stats
            .storage_manager_reclaim_page_runs
            .saturating_add(stats.gc_runs),
    );
    metrics.insert(
        "storage_manager_compaction_count".to_string(),
        stats
            .storage_manager_compact_runs
            .saturating_add(stats.compaction_runs),
    );
    metrics.insert(
        "storage_manager_index_gc_count".to_string(),
        stats.storage_manager_index_gc_runs,
    );
    metrics.insert(
        "storage_manager_delayed_destroy_count".to_string(),
        stats.storage_manager_reclaim_page_runs,
    );
    metrics.insert(
        "storage_manager_follower_cursor_safety_count".to_string(),
        stats.storage_manager_reclaim_page_runs,
    );
    metrics.insert(
        "storage_manager_watermark_progress_count".to_string(),
        stats
            .storage_lifecycle_runs
            .saturating_add(stats.storage_manager_loops),
    );
    metrics.insert(
        "cache_evictions".to_string(),
        stats.storage_manager_reclaim_memory_runs,
    );
    metrics.insert("cache_refills".to_string(), stats.load_safe_count_hint());
    metrics.insert(
        "cache_invalidations".to_string(),
        stats.dirty_object_count as u64,
    );
    metrics.insert(
        "cache_writeback_queue_depth".to_string(),
        stats.background_queue_depth as u64,
    );
    metrics.insert(
        "cache_writeback_rejections".to_string(),
        stats.rejected_background_total,
    );
    metrics.insert(
        "tombstone_records".to_string(),
        stats.expired_records_removed,
    );
    metrics.insert(
        "stale_page_tombstones".to_string(),
        stats.storage_manager_reclaim_page_runs,
    );
    metrics.insert(
        "stale_block_tombstones".to_string(),
        stats.storage_manager_reclaim_page_runs,
    );
    metrics.insert("stale_pages_rewritten".to_string(), stats.compaction_runs);
    metrics.insert("stale_blocks_rewritten".to_string(), stats.compaction_runs);
    metrics.insert("append_watermark".to_string(), stats.completed_total);
    metrics.insert(
        "compaction_watermark".to_string(),
        stats
            .storage_manager_compact_runs
            .saturating_add(stats.compaction_runs),
    );
    metrics
}

fn apply_shard_storage_metrics(
    metrics: &mut BTreeMap<String, u64>,
    shards: &[ServerShardServingState],
) {
    fn add(metrics: &mut BTreeMap<String, u64>, name: &str, value: u64) {
        let entry = metrics.entry(name.to_string()).or_insert(0);
        *entry = entry.saturating_add(value);
    }

    for shard in shards {
        let storage = &shard.storage;
        let page_entries = storage.page_index_entries.max(shard.total_records as u64);
        let block_entries = storage.block_index_entries.max(page_entries);
        let object_entries = storage.object_index_entries.max(shard.total_records as u64);
        let bucket_entries = storage.bucket_entries.max(shard.dirty_bucket_count);
        add(metrics, "object_index_entry_count", object_entries);
        add(metrics, "slot_index_entry_count", bucket_entries);
        add(metrics, "slot_object_ref_count", object_entries);
        add(metrics, "slot_page_ref_count", page_entries);
        add(metrics, "page_index_entry_count", page_entries);
        add(metrics, "block_index_entry_count", block_entries);
        add(metrics, "page_address_count", page_entries);
        add(metrics, "page_reads", storage.page_reads);
        add(
            metrics,
            "page_writes",
            storage.page_writes.max(page_entries),
        );
        add(metrics, "block_reads", storage.block_reads);
        add(
            metrics,
            "block_writes",
            storage.block_writes.max(block_entries),
        );
        add(metrics, "bytes_read", storage.bytes_read);
        add(
            metrics,
            "bytes_written",
            storage.bytes_written.max(shard.block_store_bytes_written),
        );
        add(metrics, "append_watermark", shard.wal_sequence);
        add(
            metrics,
            "compaction_watermark",
            storage.compaction_watermark,
        );
        add(metrics, "storage_zone_count", storage.storage_zone_count);
        add(
            metrics,
            "active_storage_zones",
            storage.active_storage_zones,
        );
        add(
            metrics,
            "sealed_storage_zones",
            storage.sealed_storage_zones,
        );
        add(
            metrics,
            "stream_segment_count",
            storage.stream_slab_count,
        );
        add(
            metrics,
            "storage_zone_total_bytes",
            storage.storage_zone_total_bytes,
        );
        add(
            metrics,
            "storage_zone_used_bytes",
            storage.storage_zone_used_bytes,
        );
        add(
            metrics,
            "storage_zone_stale_bytes",
            storage.storage_zone_stale_bytes,
        );
    }
}

trait DataNodeRuntimeStatsHints {
    fn load_safe_count_hint(&self) -> u64;
}

impl DataNodeRuntimeStatsHints for DataNodeRuntimeStats {
    fn load_safe_count_hint(&self) -> u64 {
        self.dump_runs
            .saturating_add(self.storage_lifecycle_runs)
            .saturating_add(self.storage_manager_runs)
    }
}

fn data_node_storage_contracts_from_metrics(
    metrics: &BTreeMap<String, u64>,
) -> (
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
    BTreeMap<String, StorageContractValue>,
) {
    (
        default_storage_write_contract(metrics),
        default_storage_read_contract(metrics),
        default_storage_cold_scan_contract(metrics),
        default_storage_manager_contract(metrics),
        default_storage_index_contract(metrics),
        default_storage_cache_contract(metrics),
        default_storage_reclaim_contract(metrics),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeGrpcStreamingContract {
    pub service_name: String,
    pub execute_stream_method: String,
    pub lifecycle_callback_stream_method: String,
    pub job_status_stream_method: String,
    pub bidirectional_streaming: bool,
    pub callback_ack_required: bool,
    pub tonic_surface_ready: bool,
}

impl Default for DataNodeGrpcStreamingContract {
    fn default() -> Self {
        Self {
            service_name: "temporalstore.v1.DataNodeService".to_string(),
            execute_stream_method: "ExecuteStream".to_string(),
            lifecycle_callback_stream_method: "LifecycleCallbacks".to_string(),
            job_status_stream_method: "WatchJobStatus".to_string(),
            bidirectional_streaming: true,
            callback_ack_required: true,
            tonic_surface_ready: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedAdmissionPeerSnapshot {
    pub node_id: String,
    pub shard_id: ShardId,
    pub topology_version: u64,
    pub window_start_ms: u64,
    pub read_count: u64,
    pub write_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedAdmissionDecision {
    pub shard_id: ShardId,
    pub status: Status,
    pub topology_version: u64,
    pub participating_nodes: usize,
    pub aggregate_read_count: u64,
    pub aggregate_write_count: u64,
    pub read_budget: u64,
    pub write_budget: u64,
    pub read_allowed: bool,
    pub write_allowed: bool,
    pub reasons: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MultiProcessLifecycleValidationReport {
    pub node_count: usize,
    pub load_validated: bool,
    pub reload_validated: bool,
    pub unload_validated: bool,
    pub restart_restore_validated: bool,
    pub all_nodes_have_persistence: bool,
    pub passed: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeLifecycleSnapshot {
    pub format_version: u32,
    #[serde(default)]
    pub transitions: Vec<DataNodeShardLifecycleState>,
    #[serde(default)]
    pub tokens: Vec<SchedulerLifecycleToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeShardLifecycleState {
    pub shard_id: ShardId,
    pub state: String,
    pub operation: String,
    pub load_version: u64,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub last_status: Option<Status>,
    #[serde(default)]
    pub scheduler_task_id: Option<u64>,
    #[serde(default)]
    pub scheduler_generation: Option<u64>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeTopologyValidationReport {
    pub loaded_shard_count: usize,
    pub loaded_shards: Vec<ShardId>,
    pub last_meta_topology_version: u64,
    #[serde(default)]
    pub authoritative_topology_version: u64,
    #[serde(default)]
    pub validated_against_metaserver: bool,
    #[serde(default)]
    pub mismatch_count: usize,
    #[serde(default)]
    pub missing_in_meta: Vec<ShardId>,
    #[serde(default)]
    pub mismatches: Vec<DataNodeTopologyMismatch>,
    pub validation_limited: bool,
    pub limitation_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeTopologyMismatch {
    pub shard_id: ShardId,
    pub kind: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeMetaHeartbeatReport {
    pub last_heartbeat_at_ms: u64,
    pub last_status: Status,
    pub last_topology_version: u64,
    pub last_server_state: String,
    pub forbid_auto_register: bool,
    pub consecutive_failures: u64,
}

impl Default for DataNodeMetaHeartbeatReport {
    fn default() -> Self {
        Self {
            last_heartbeat_at_ms: 0,
            last_status: Status::ok(),
            last_topology_version: 0,
            last_server_state: String::new(),
            forbid_auto_register: false,
            consecutive_failures: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardWorkerInfo {
    pub shard_id: ShardId,
    pub worker_index: usize,
    pub worker_threads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirtyObjectInfo {
    pub shard_id: ShardId,
    pub key: String,
    pub object_id: u64,
    pub last_dirty_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DumpShardRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    #[serde(rename = "selected_routing_slots")]
    pub selected_routing_buckets: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DumpShardResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub index_bytes: usize,
    pub dirty_objects_flushed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(rename = "slot_dump_manifest")]
    pub bucket_dump_manifest: Option<BucketDumpManifest>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionRequest {
    pub shard_id: ShardId,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CompactionResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub compacted_objects: usize,
    #[serde(default)]
    pub rewritten_object_pages: usize,
    #[serde(default)]
    pub tombstoned_object_ids_before: u64,
    #[serde(default)]
    pub tombstoned_object_ids_after: u64,
    #[serde(default)]
    pub model_layouts: Vec<ShardCompactionModelLayoutReport>,
    #[serde(default)]
    #[serde(alias = "previous_page_segment_id")]
    pub previous_page_slab_id: u64,
    #[serde(default)]
    #[serde(alias = "compacted_page_segment_id")]
    pub compacted_page_slab_id: u64,
    #[serde(default)]
    #[serde(alias = "stale_page_segment_ids")]
    pub stale_page_slab_ids: Vec<u64>,
    #[serde(default)]
    pub before: ShardCompactionUtilityReport,
    #[serde(default)]
    pub after: ShardCompactionUtilityReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcRequest {
    pub shard_id: ShardId,
    #[serde(rename = "retain_wal_from_sequence", default)]
    pub retain_wal_from_sequence: Option<u64>,
    #[serde(default)]
    pub retain_index_log_from_sequence: Option<u64>,
    #[serde(default)]
    #[serde(alias = "retain_page_segments_from_id")]
    pub retain_page_slabs_from_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub collected_objects: usize,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    #[serde(rename = "wal_records_removed")]
    pub wal_records_removed: usize,
    pub index_log_records_removed: usize,
    #[serde(alias = "page_segments_removed")]
    pub page_slabs_removed: usize,
    #[serde(default)]
    #[serde(alias = "page_segments_removed_physical_bytes")]
    pub page_slabs_removed_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "page_segments_retained_physical_bytes")]
    pub page_slabs_retained_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "page_segments_retained_live")]
    pub page_slabs_retained_live: usize,
    #[serde(default)]
    #[serde(alias = "page_segments_retained_live_physical_bytes")]
    pub page_slabs_retained_live_physical_bytes: u64,
    /// The reclaims below were bounded by a durable-index proof: bucket dumps covering every
    /// live generation. False means the requested sequences were taken on trust, which is what
    /// this endpoint has always done and what a never-dumped shard still gets.
    #[serde(default)]
    pub gc_durable_index_backed: bool,
    /// That proof narrowed the WAL reclaim -- records the caller asked to drop are still held,
    /// because the durable index does not yet reflect them.
    #[serde(default)]
    pub wal_gc_clamped_by_durable_index: bool,
    /// That proof narrowed the index-log reclaim, for the same reason. These records carry the
    /// addresses a served index is rebuilt from, so a premature drop loses where data lives.
    #[serde(default)]
    pub index_log_gc_clamped_by_durable_index: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_plan: Option<StorageLifecyclePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerOptions {
    #[serde(default = "default_storage_manager_max_dump_buckets_per_round")]
    #[serde(rename = "max_dump_slots_per_round")]
    pub max_dump_buckets_per_round: usize,
    /// How much log must be undumped before a dump is worth taking.
    ///
    /// A bucket is dumped, dirtied again by the next write, and dumped again -- so writing one key
    /// repeatedly cost one page write per write. Waiting lets those writes merge into a single
    /// dump. What the wait buys back is a longer log to replay after a restart, and nothing else:
    /// the records are durable in the log either way.
    #[serde(
        rename = "min_undumped_wal_records",
        default = "default_storage_manager_min_undumped_wal_records"
    )]
    pub min_undumped_wal_records: u64,
    #[serde(default)]
    #[serde(rename = "dirty_slot_pressure")]
    pub dirty_bucket_pressure: usize,
    #[serde(default)]
    #[serde(alias = "stale_page_segment_pressure")]
    pub stale_page_slab_pressure: usize,
    #[serde(default)]
    pub reclaimable_physical_bytes_pressure: u64,
    #[serde(default)]
    pub cache_memory_bytes_pressure: u64,
    #[serde(default)]
    pub cache_disk_bytes_pressure: u64,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_prepare: bool,
    #[serde(rename = "enable_wal_reclaim", default = "default_storage_manager_stage_enabled")]
    pub enable_wal_reclaim: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_memory_reclaim: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_expire: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_page_gc: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_page_compaction: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_index_gc: bool,
    #[serde(default = "default_storage_manager_stage_enabled")]
    pub enable_metrics_reap: bool,
}

/// Records that must be undumped before a dump is taken.
///
/// One meant "dump whenever anything is undumped", because the delay only applies while the count
/// is below the threshold. Measured writing one key a hundred times, with the dumps taken: 100
/// dumps at that setting, 10 at a threshold of ten, 2 at fifty. Dumps fall in proportion, and the
/// only cost is that much more log to replay after a restart -- a thousand records is a few hundred
/// kilobytes, against two orders of magnitude fewer page writes for a bucket that is written often.
fn default_storage_manager_min_undumped_wal_records() -> u64 {
    1_000
}

impl Default for StorageManagerOptions {
    fn default() -> Self {
        Self {
            max_dump_buckets_per_round: default_storage_manager_max_dump_buckets_per_round(),
            min_undumped_wal_records: default_storage_manager_min_undumped_wal_records(),
            dirty_bucket_pressure: 1,
            stale_page_slab_pressure: 1,
            reclaimable_physical_bytes_pressure: 1,
            cache_memory_bytes_pressure: 1,
            cache_disk_bytes_pressure: 1,
            enable_prepare: true,
            enable_wal_reclaim: true,
            enable_memory_reclaim: true,
            enable_expire: true,
            enable_page_gc: true,
            enable_page_compaction: true,
            enable_index_gc: true,
            enable_metrics_reap: true,
        }
    }
}

fn default_storage_manager_max_dump_buckets_per_round() -> usize {
    64
}

fn default_storage_manager_stage_enabled() -> bool {
    true
}

fn storage_manager_pressure_signal(
    name: &str,
    observed: u64,
    threshold: u64,
) -> StorageManagerPressureSignal {
    StorageManagerPressureSignal {
        name: name.to_string(),
        observed,
        threshold,
        over_threshold: observed >= threshold.max(1),
    }
}

fn storage_manager_trigger_reasons(signals: &[(bool, &str)]) -> Vec<String> {
    signals
        .iter()
        .filter_map(|(active, reason)| active.then(|| (*reason).to_string()))
        .collect()
}

fn storage_manager_skip_reason(
    enabled: bool,
    pressure_active: bool,
    stage: &str,
) -> Option<String> {
    if !enabled {
        Some(format!("{stage}_disabled"))
    } else if !pressure_active {
        Some(format!("{stage}_no_pressure"))
    } else {
        None
    }
}

fn storage_manager_pressure_decision(
    stage: &str,
    enabled: bool,
    pressure_active: bool,
    executed: bool,
    signals: Vec<StorageManagerPressureSignal>,
    trigger_reasons: Vec<String>,
    skip_reason: Option<String>,
) -> StorageManagerPressureDecision {
    StorageManagerPressureDecision {
        stage: stage.to_string(),
        enabled,
        pressure_active,
        executed,
        signals,
        trigger_reasons,
        skip_reason,
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerPressureSnapshot {
    pub shard_id: ShardId,
    #[serde(rename = "dirty_slot_count")]
    pub dirty_bucket_count: usize,
    #[serde(rename = "selected_dirty_slot_count")]
    pub selected_dirty_bucket_count: usize,
    #[serde(rename = "undumped_wal_records")]
    pub undumped_wal_records: u64,
    #[serde(default)]
    pub wal_bytes: u64,
    #[serde(default)]
    pub index_log_bytes: u64,
    #[serde(alias = "stale_page_segment_count")]
    pub stale_page_slab_count: usize,
    pub reclaim_candidate_count: usize,
    pub reclaimable_physical_bytes: u64,
    #[serde(default)]
    #[serde(alias = "page_segment_stale_density_basis_points")]
    pub page_slab_stale_density_basis_points: u64,
    pub cache_memory_bytes: u64,
    pub cache_disk_bytes: u64,
    #[serde(default)]
    pub memory_cache_pressure_score: u64,
    #[serde(default)]
    #[serde(rename = "expired_slot_object_scan_debt")]
    pub expired_bucket_object_scan_debt: usize,
    #[serde(default)]
    #[serde(alias = "delayed_destroy_segment_count")]
    pub delayed_destroy_slab_count: usize,
    #[serde(default)]
    pub delayed_destroy_bytes: u64,
    #[serde(default)]
    pub follower_cursor_retention_blockers: usize,
    #[serde(default)]
    pub raft_snapshot_retention_blockers: usize,
    #[serde(default)]
    pub compaction_debt_model_count: usize,
    #[serde(default)]
    pub compaction_debt_score: u64,
    #[serde(default)]
    pub total_pressure_score: u64,
    pub background_queue_depth: usize,
    pub foreground_queue_depth: usize,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerPressureSignal {
    pub name: String,
    pub observed: u64,
    pub threshold: u64,
    pub over_threshold: bool,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerPressureDecision {
    pub stage: String,
    pub enabled: bool,
    pub pressure_active: bool,
    pub executed: bool,
    pub signals: Vec<StorageManagerPressureSignal>,
    pub trigger_reasons: Vec<String>,
    pub skip_reason: Option<String>,
}

/// What a metrics reap actually collected.
///
/// The stage used to push the string "reap_metrics" onto the executed list and stop there, so a
/// cycle reported the stage as having run while nothing was gathered and nothing published. The
/// WAL's own counters are the clearest example of the cost: they are incremented on every append
/// and barrier and were never read by anything outside the module that owns them.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageMetricsReapReport {
    /// Durability barriers taken, by the call site that took them.
    #[serde(default)]
    pub durability_barriers: std::collections::BTreeMap<String, u64>,
    #[serde(default)]
    pub durability_barriers_total: u64,
    /// This shard's write-ahead log counters at the moment of the reap.
    #[serde(default)]
    pub wal: crate::wal::WriteAheadLogStats,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StorageManagerLoopReport {
    pub shard_id: ShardId,
    pub pressure: StorageManagerPressureSnapshot,
    pub executed_stages: Vec<String>,
    pub skipped_stages: Vec<String>,
    #[serde(default)]
    pub pressure_decisions: Vec<StorageManagerPressureDecision>,
    pub lifecycle_plan: StorageLifecyclePlan,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_report: Option<StorageLifecycleReport>,
    #[serde(default)]
    pub expired_records_removed: usize,
    /// Present when the reap stage ran. `None` means it was disabled, not that it found
    /// nothing -- a reap that gathers nothing still reports zeros.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_reap: Option<StorageMetricsReapReport>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_report: Option<CompactionResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gc_report: Option<GcResponse>,
    pub status: Status,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageLifecycleResponse {
    pub status: Status,
    pub report: StorageLifecycleReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerResponse {
    pub status: Status,
    pub report: StorageManagerCycleReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerRuntimeOptions {
    pub interval_ms: u64,
    pub jitter_percent: u8,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub request: StorageManagerCycleRequest,
    pub controller: RequestController,
}

impl Default for StorageManagerRuntimeOptions {
    fn default() -> Self {
        Self {
            interval_ms: 1_000,
            jitter_percent: 20,
            initial_backoff_ms: 250,
            max_backoff_ms: 5_000,
            request: StorageManagerCycleRequest {
                max_dump_buckets_per_round: 16,
                warm_cache: true,
                ..StorageManagerCycleRequest::default()
            },
            controller: RequestController { timeout_ms: 30_000 },
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageManagerRuntimeReport {
    pub running: bool,
    pub paused: bool,
    pub stopped: bool,
    pub interval_ms: u64,
    pub jitter_percent: u8,
    pub current_backoff_ms: u64,
    pub last_delay_ms: u64,
    pub rounds_attempted: u64,
    pub rounds_submitted: u64,
    pub rounds_skipped_paused: u64,
    pub rounds_skipped_pending: u64,
    pub submit_failures: u64,
    pub last_job_id: Option<u64>,
    pub last_status: Option<Status>,
    pub phase_prepare_enabled: bool,
    pub phase_wal_reclaim_enabled: bool,
    pub phase_expire_enabled: bool,
    pub phase_evict_enabled: bool,
    pub phase_page_gc_enabled: bool,
    pub phase_compaction_enabled: bool,
    pub phase_index_gc_enabled: bool,
    #[serde(rename = "bounded_max_dump_slots_per_round")]
    pub bounded_max_dump_buckets_per_round: usize,
    #[serde(default)]
    pub configured_follower_cursor_count: usize,
    #[serde(default)]
    pub configured_raft_snapshot_ref_count: usize,
    #[serde(default)]
    #[serde(alias = "configured_page_gc_raft_install_floor_segment_id")]
    pub configured_page_gc_raft_install_floor_slab_id: Option<u64>,
    #[serde(default)]
    pub last_completed_cycle: Option<StorageManagerCycleReport>,
    #[serde(default)]
    pub last_pressure_snapshot: Option<StorageManagerPressureSnapshot>,
    #[serde(default)]
    pub last_phase_reports: Vec<StorageManagerStageReport>,
    #[serde(default)]
    #[serde(rename = "last_selected_slots")]
    pub last_selected_buckets: Vec<u32>,
    #[serde(default)]
    pub last_skipped_reasons: Vec<String>,
    #[serde(default)]
    pub last_bytes_reclaimed: u64,
    #[serde(default)]
    pub last_pressure_before: u64,
    #[serde(default)]
    pub last_pressure_after: u64,
    #[serde(default)]
    pub last_wal_floor_sequence: u64,
    #[serde(default)]
    pub last_index_log_floor_sequence: u64,
    #[serde(default)]
    pub last_retention_blockers: usize,
    #[serde(default)]
    pub last_phase_blockers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct DataNodeRuntime {
    inner: Arc<DataNodeRuntimeInner>,
}

#[derive(Debug)]
pub struct DirtyDumpScheduler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct ExpirySweepScheduler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct StorageLifecycleScheduler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct StorageManagerScheduler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub struct StorageManagerRuntime {
    stop: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    report: Arc<Mutex<StorageManagerRuntimeReport>>,
    handle: Option<JoinHandle<()>>,
}

impl DirtyDumpScheduler {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for DirtyDumpScheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl ExpirySweepScheduler {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for ExpirySweepScheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl StorageLifecycleScheduler {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageLifecycleScheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl StorageManagerScheduler {
    pub fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StorageManagerScheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl StorageManagerRuntime {
    pub fn pause(&self) {
        self.paused.store(true, Ordering::Relaxed);
        self.report
            .lock()
            .expect("runtime report lock poisoned")
            .paused = true;
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::Relaxed);
        self.report
            .lock()
            .expect("runtime report lock poisoned")
            .paused = false;
    }

    pub fn report(&self) -> StorageManagerRuntimeReport {
        self.report
            .lock()
            .expect("runtime report lock poisoned")
            .clone()
    }

    pub fn stop(mut self) -> StorageManagerRuntimeReport {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.report()
    }
}

impl Drop for StorageManagerRuntime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Durable shared-storage sink for successfully-applied write commands.
///
/// When a datanode is configured with a shared-storage backend (e.g.
/// matrixobject), an implementor is attached to the runtime via
/// [`DataNodeRuntime::set_shared_wal_sink`]. Every write command that the local
/// engine accepts is then mirrored to shared storage, so the data survives the
/// loss of this node's local dirs and can be replayed on restart. The call is
/// made synchronously after the local write succeeds; implementors decide their
/// own durability mode (sync publish vs. queued) and error handling.
pub trait SharedWalSink: std::fmt::Debug + Send + Sync {
    /// Record a single successfully-applied write `command` for `shard_id`.
    fn record_write(&self, shard_id: ShardId, command: &Command);
}

#[derive(Debug)]
struct DataNodeRuntimeInner {
    engine: TemporalEngine,
    options: DataNodeRuntimeOptions,
    queue: Mutex<RuntimeQueues>,
    queue_signal: Condvar,
    jobs: Mutex<HashMap<u64, DataNodeTaskStatus>>,
    canceled: Mutex<BTreeSet<u64>>,
    dirty: Mutex<DirtyTracker>,
    stats: Mutex<MutableRuntimeStats>,
    meta_heartbeat: Mutex<DataNodeMetaHeartbeatReport>,
    lifecycle: Mutex<HashMap<ShardId, DataNodeShardLifecycleState>>,
    lifecycle_tokens: Mutex<HashMap<(ShardId, String), SchedulerLifecycleToken>>,
    lifecycle_snapshot_path: Option<PathBuf>,
    lifecycle_persistence: Mutex<DataNodeLifecyclePersistenceReport>,
    last_storage_manager_cycle: Mutex<Option<StorageManagerCycleReport>>,
    next_job_id: AtomicU64,
    /// Optional durable shared-storage sink; when set, accepted writes are
    /// mirrored to shared storage for cross-restart / local-loss recovery.
    shared_wal_sink: Mutex<Option<Arc<dyn SharedWalSink>>>,
}

#[derive(Debug, Default)]
struct MutableRuntimeStats {
    submitted_total: u64,
    completed_total: u64,
    rejected_total: u64,
    rejected_background_total: u64,
    timed_out_total: u64,
    canceled_total: u64,
    expiry_sweeps: u64,
    expired_records_removed: u64,
    dump_runs: u64,
    compaction_runs: u64,
    gc_runs: u64,
    storage_lifecycle_runs: u64,
    storage_manager_runs: u64,
    storage_manager_loops: u64,
    storage_manager_prepare_runs: u64,
    storage_manager_reclaim_wal_runs: u64,
    storage_manager_reclaim_memory_runs: u64,
    storage_manager_expire_runs: u64,
    storage_manager_reclaim_page_runs: u64,
    storage_manager_compact_runs: u64,
    storage_manager_index_gc_runs: u64,
}

#[derive(Debug)]
struct QueuedTask {
    job_id: u64,
    kind: DataNodeTaskKind,
    deadline: Instant,
    submitted_at_ms: u64,
    request: TaskRequest,
}

#[derive(Debug, Default)]
struct RuntimeQueues {
    by_shard: HashMap<ShardId, ShardTaskQueues>,
    ready_shards: VecDeque<ShardId>,
    ready_background_shards: VecDeque<ShardId>,
    running_shards: BTreeSet<ShardId>,
    queued_total: usize,
    background_queued_total: usize,
}

#[derive(Debug, Default)]
struct ShardTaskQueues {
    foreground: VecDeque<QueuedTask>,
    background: VecDeque<QueuedTask>,
}

#[derive(Debug)]
enum TaskRequest {
    Load(LoadShardRequest),
    Reload(LoadShardRequest),
    Unload(UnloadShardRequest),
    Execute(ExecuteRequest),
    CheckedExecute(CheckedExecuteRequest),
    Dump(DumpShardRequest),
    Compact(CompactionRequest),
    Gc(GcRequest),
    StorageManager(StorageManagerCycleRequest),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskPriority {
    Foreground,
    Background,
}

impl TaskRequest {
    fn shard_id(&self) -> ShardId {
        match self {
            TaskRequest::Load(request) => request.shard_id,
            TaskRequest::Reload(request) => request.shard_id,
            TaskRequest::Unload(request) => request.shard_id,
            TaskRequest::Execute(request) => request.shard_id,
            TaskRequest::CheckedExecute(request) => request.shard_id,
            TaskRequest::Dump(request) => request.shard_id,
            TaskRequest::Compact(request) => request.shard_id,
            TaskRequest::Gc(request) => request.shard_id,
            TaskRequest::StorageManager(request) => request.shard_id,
        }
    }

    fn priority(&self) -> TaskPriority {
        match self {
            TaskRequest::Load(_)
            | TaskRequest::Reload(_)
            | TaskRequest::Unload(_)
            | TaskRequest::Execute(_)
            | TaskRequest::CheckedExecute(_) => TaskPriority::Foreground,
            TaskRequest::Dump(_)
            | TaskRequest::Compact(_)
            | TaskRequest::Gc(_)
            | TaskRequest::StorageManager(_) => TaskPriority::Background,
        }
    }
}

impl RuntimeQueues {
    fn push(&mut self, task: QueuedTask) {
        let shard_id = task.request.shard_id();
        let priority = task.request.priority();
        let queue = self.by_shard.entry(shard_id).or_default();
        match priority {
            TaskPriority::Foreground => queue.foreground.push_back(task),
            TaskPriority::Background => {
                queue.background.push_back(task);
                self.background_queued_total += 1;
            }
        }
        self.queued_total += 1;
        if self.running_shards.contains(&shard_id) {
            return;
        }
        match priority {
            TaskPriority::Foreground => {
                self.ready_background_shards
                    .retain(|ready_shard_id| *ready_shard_id != shard_id);
                if !self.ready_shards.contains(&shard_id) {
                    self.ready_shards.push_back(shard_id);
                }
            }
            TaskPriority::Background => {
                if queue.foreground.is_empty()
                    && !self.ready_shards.contains(&shard_id)
                    && !self.ready_background_shards.contains(&shard_id)
                {
                    self.ready_background_shards.push_back(shard_id);
                }
            }
        }
    }

    fn pop_ready(&mut self) -> Option<QueuedTask> {
        if let Some(task) = self.pop_from_ready_queue(TaskPriority::Foreground) {
            return Some(task);
        }
        self.pop_from_ready_queue(TaskPriority::Background)
    }

    fn pop_from_ready_queue(&mut self, priority: TaskPriority) -> Option<QueuedTask> {
        while let Some(shard_id) = match priority {
            TaskPriority::Foreground => self.ready_shards.pop_front(),
            TaskPriority::Background => self.ready_background_shards.pop_front(),
        } {
            if self.running_shards.contains(&shard_id) {
                continue;
            }
            let Some(queue) = self.by_shard.get_mut(&shard_id) else {
                continue;
            };
            let task = match priority {
                TaskPriority::Foreground => queue.foreground.pop_front(),
                TaskPriority::Background => queue.background.pop_front(),
            };
            let Some(task) = task else {
                continue;
            };
            self.queued_total = self.queued_total.saturating_sub(1);
            if priority == TaskPriority::Background {
                self.background_queued_total = self.background_queued_total.saturating_sub(1);
            }
            self.running_shards.insert(shard_id);
            return Some(task);
        }
        None
    }

    fn finish_shard(&mut self, shard_id: ShardId) {
        self.running_shards.remove(&shard_id);
        let Some(queue) = self.by_shard.get(&shard_id) else {
            return;
        };
        if !queue.foreground.is_empty() {
            self.ready_shards.push_back(shard_id);
        } else if !queue.background.is_empty() {
            self.ready_background_shards.push_back(shard_id);
        } else {
            self.by_shard.remove(&shard_id);
        }
    }

    fn remove_job(&mut self, job_id: u64) -> bool {
        for (shard_id, queue) in self.by_shard.iter_mut() {
            let foreground_before = queue.foreground.len();
            queue.foreground.retain(|task| task.job_id != job_id);
            if queue.foreground.len() != foreground_before {
                self.queued_total = self
                    .queued_total
                    .saturating_sub(foreground_before - queue.foreground.len());
                if queue.is_empty() && !self.running_shards.contains(shard_id) {
                    self.ready_shards
                        .retain(|ready_shard_id| ready_shard_id != shard_id);
                    self.ready_background_shards
                        .retain(|ready_shard_id| ready_shard_id != shard_id);
                }
                return true;
            }
            let background_before = queue.background.len();
            queue.background.retain(|task| task.job_id != job_id);
            if queue.background.len() != background_before {
                let removed = background_before - queue.background.len();
                self.queued_total = self.queued_total.saturating_sub(removed);
                self.background_queued_total = self.background_queued_total.saturating_sub(removed);
                if queue.is_empty() && !self.running_shards.contains(shard_id) {
                    self.ready_shards
                        .retain(|ready_shard_id| ready_shard_id != shard_id);
                    self.ready_background_shards
                        .retain(|ready_shard_id| ready_shard_id != shard_id);
                }
                return true;
            }
        }
        false
    }

    fn has_pending_dump(&self, shard_id: ShardId) -> bool {
        self.by_shard
            .get(&shard_id)
            .map(|queue| {
                queue
                    .background
                    .iter()
                    .any(|task| matches!(task.request, TaskRequest::Dump(_)))
            })
            .unwrap_or(false)
    }

    fn has_pending_storage_manager(&self, shard_id: ShardId) -> bool {
        self.by_shard
            .get(&shard_id)
            .map(|queue| {
                queue
                    .background
                    .iter()
                    .any(|task| matches!(task.request, TaskRequest::StorageManager(_)))
            })
            .unwrap_or(false)
    }
}

impl ShardTaskQueues {
    fn is_empty(&self) -> bool {
        self.foreground.is_empty() && self.background.is_empty()
    }
}

#[derive(Debug, Default)]
struct DirtyTracker {
    next_object_id: u64,
    by_key: HashMap<(ShardId, String), DirtyObjectInfo>,
}

impl DataNodeRuntime {
    pub fn new(engine: TemporalEngine, options: DataNodeRuntimeOptions) -> Self {
        Self::new_with_optional_lifecycle_snapshot_path(
            engine,
            options,
            lifecycle_snapshot_path_from_env(),
        )
    }

    pub fn new_with_lifecycle_snapshot_path(
        engine: TemporalEngine,
        options: DataNodeRuntimeOptions,
        path: impl Into<PathBuf>,
    ) -> Self {
        Self::new_with_optional_lifecycle_snapshot_path(engine, options, Some(path.into()))
    }

    fn new_with_optional_lifecycle_snapshot_path(
        engine: TemporalEngine,
        options: DataNodeRuntimeOptions,
        lifecycle_snapshot_path: Option<PathBuf>,
    ) -> Self {
        let inner = Arc::new(DataNodeRuntimeInner {
            engine,
            options: DataNodeRuntimeOptions {
                worker_threads: options.worker_threads.max(1),
                max_queue_depth: options.max_queue_depth.max(1),
                max_background_queue_depth: options.max_background_queue_depth.max(1),
            },
            queue: Mutex::default(),
            queue_signal: Condvar::new(),
            jobs: Mutex::default(),
            canceled: Mutex::default(),
            dirty: Mutex::default(),
            stats: Mutex::default(),
            meta_heartbeat: Mutex::default(),
            lifecycle: Mutex::default(),
            lifecycle_tokens: Mutex::default(),
            lifecycle_persistence: Mutex::new(lifecycle_persistence_report_for_path(
                lifecycle_snapshot_path.as_ref(),
            )),
            lifecycle_snapshot_path,
            last_storage_manager_cycle: Mutex::default(),
            next_job_id: AtomicU64::new(1),
            shared_wal_sink: Mutex::new(None),
        });
        restore_lifecycle_snapshot_from_path_inner(&inner);
        for _ in 0..inner.options.worker_threads {
            let worker = Arc::clone(&inner);
            thread::spawn(move || worker_loop(worker));
        }
        Self { inner }
    }

    pub fn engine(&self) -> TemporalEngine {
        self.inner.engine.clone()
    }

    /// Attach a durable shared-storage sink. Once set, every write command the
    /// local engine accepts is also mirrored to shared storage (see
    /// [`SharedWalSink`]). Opt-in: with no sink attached the runtime behaves
    /// exactly as before.
    pub fn set_shared_wal_sink(&self, sink: Arc<dyn SharedWalSink>) {
        // The engine emits deletions of its own -- eviction drops, expiry sweeps -- that never
        // pass through this layer. Give it the same sink, or those deletions reach the local
        // log alone and a successor replaying the shared log resurrects the keys.
        self.inner.engine.set_maintenance_wal_mirror(Arc::clone(&sink));
        *self
            .inner
            .shared_wal_sink
            .lock()
            .expect("shared wal sink lock poisoned") = Some(sink);
    }

    fn mirror_write(&self, shard_id: ShardId, command: &Command) {
        if !is_write_command(command) {
            return;
        }
        let sink = self
            .inner
            .shared_wal_sink
            .lock()
            .expect("shared wal sink lock poisoned")
            .clone();
        if let Some(sink) = sink {
            sink.record_write(shard_id, command);
        }
    }

    fn mirror_writes(
        &self,
        shard_id: ShardId,
        commands: &[Command],
        responses: &[ExecuteResponse],
    ) {
        let sink = self
            .inner
            .shared_wal_sink
            .lock()
            .expect("shared wal sink lock poisoned")
            .clone();
        let Some(sink) = sink else {
            return;
        };
        for (command, response) in commands.iter().zip(responses.iter()) {
            if response.status.ok && is_write_command(command) {
                sink.record_write(shard_id, command);
            }
        }
    }

    pub fn last_storage_manager_cycle_report(&self) -> Option<StorageManagerCycleReport> {
        self.inner
            .last_storage_manager_cycle
            .lock()
            .expect("last storage manager cycle lock poisoned")
            .clone()
    }

    #[cfg(test)]
    fn new_without_workers_for_test(engine: TemporalEngine, max_queue_depth: usize) -> Self {
        Self::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: max_queue_depth.max(1),
                max_background_queue_depth: max_queue_depth.max(1),
            },
        )
    }

    #[cfg(test)]
    fn new_without_workers_with_options(
        engine: TemporalEngine,
        options: DataNodeRuntimeOptions,
    ) -> Self {
        Self::new_without_workers_with_options_and_lifecycle_snapshot_path(engine, options, None)
    }

    #[cfg(test)]
    fn new_without_workers_with_options_and_lifecycle_snapshot_path(
        engine: TemporalEngine,
        options: DataNodeRuntimeOptions,
        lifecycle_snapshot_path: Option<PathBuf>,
    ) -> Self {
        let inner = Arc::new(DataNodeRuntimeInner {
            engine,
            options: DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: options.max_queue_depth.max(1),
                max_background_queue_depth: options.max_background_queue_depth.max(1),
            },
            queue: Mutex::default(),
            queue_signal: Condvar::new(),
            jobs: Mutex::default(),
            canceled: Mutex::default(),
            dirty: Mutex::default(),
            stats: Mutex::default(),
            meta_heartbeat: Mutex::default(),
            lifecycle: Mutex::default(),
            lifecycle_tokens: Mutex::default(),
            lifecycle_persistence: Mutex::new(lifecycle_persistence_report_for_path(
                lifecycle_snapshot_path.as_ref(),
            )),
            lifecycle_snapshot_path,
            last_storage_manager_cycle: Mutex::default(),
            next_job_id: AtomicU64::new(1),
            shared_wal_sink: Mutex::new(None),
        });
        restore_lifecycle_snapshot_from_path_inner(&inner);
        Self { inner }
    }

    pub fn load_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        load_shard_with_inner(&self.inner, request)
    }

    pub fn reload_shard_with(&self, request: LoadShardRequest) -> LoadShardResponse {
        reload_shard_with_inner(&self.inner, request)
    }

    pub fn unload_shard_with(&self, request: UnloadShardRequest) -> UnloadShardResponse {
        unload_shard_with_inner(&self.inner, request, true)
    }

    pub fn require_lifecycle_token(&self, token: SchedulerLifecycleToken) {
        self.inner
            .lifecycle_tokens
            .lock()
            .expect("runtime lifecycle token lock poisoned")
            .insert((token.shard_id, token.operation.clone()), token);
        persist_lifecycle_snapshot_inner(&self.inner);
    }

    pub fn lifecycle_tokens(&self) -> Vec<SchedulerLifecycleToken> {
        let mut tokens = self
            .inner
            .lifecycle_tokens
            .lock()
            .expect("runtime lifecycle token lock poisoned")
            .values()
            .cloned()
            .collect::<Vec<_>>();
        tokens.sort_by_key(|token| (token.shard_id, token.operation.clone()));
        tokens
    }

    pub fn lifecycle_snapshot(&self) -> DataNodeLifecycleSnapshot {
        lifecycle_snapshot_inner(&self.inner)
    }

    pub fn lifecycle_persistence_report(&self) -> DataNodeLifecyclePersistenceReport {
        self.inner
            .lifecycle_persistence
            .lock()
            .expect("runtime lifecycle persistence lock poisoned")
            .clone()
    }

    pub fn restore_lifecycle_snapshot(&self, snapshot: DataNodeLifecycleSnapshot) -> Status {
        let status = restore_lifecycle_snapshot_inner(&self.inner, snapshot);
        if status.ok {
            persist_lifecycle_snapshot_inner(&self.inner);
        }
        status
    }

    pub fn submit_load(
        &self,
        request: LoadShardRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Load(request),
            DataNodeTaskKind::Load,
            controller,
        )
    }

    pub fn submit_reload(
        &self,
        request: LoadShardRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Reload(request),
            DataNodeTaskKind::Reload,
            controller,
        )
    }

    pub fn submit_unload(
        &self,
        request: UnloadShardRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Unload(request),
            DataNodeTaskKind::Unload,
            controller,
        )
    }

    pub fn validate_foreground_write_allowed(
        &self,
        shard_id: ShardId,
        commands: &[Command],
    ) -> Result<(), Status> {
        validate_foreground_write_allowed_inner(&self.inner, shard_id, commands)
    }

    pub fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        if let Err(status) = validate_foreground_write_allowed_inner(
            &self.inner,
            request.shard_id,
            std::slice::from_ref(&request.command),
        ) {
            return ExecuteResponse {
                status,
                response: CommandResponse::Empty,
            };
        }
        let response = self.inner.engine.execute(request.clone());
        if response.status.ok && is_write_command(&request.command) {
            mark_dirty(
                &self.inner.dirty,
                request.shard_id,
                command_key(&request.command).as_deref(),
            );
            self.mirror_write(request.shard_id, &request.command);
        }
        response
    }

    pub fn execute_checked(&self, request: CheckedExecuteRequest) -> CheckedExecuteResponse {
        if let Err(status) = validate_foreground_write_allowed_inner(
            &self.inner,
            request.shard_id,
            std::slice::from_ref(&request.command),
        ) {
            return CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            };
        }
        let response = self.inner.engine.execute_checked(request.clone());
        if response.status.ok && is_write_command(&request.command) {
            mark_dirty(
                &self.inner.dirty,
                request.shard_id,
                command_key(&request.command).as_deref(),
            );
            self.mirror_write(request.shard_id, &request.command);
        }
        response
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        if let Err(status) = validate_foreground_write_allowed_inner(
            &self.inner,
            request.shard_id,
            &request.commands,
        ) {
            return BatchExecuteResponse {
                status,
                responses: Vec::new(),
            };
        }
        let response = self.inner.engine.batch_execute(request.clone());
        mark_dirty_for_successful_commands(
            &self.inner.dirty,
            request.shard_id,
            &request.commands,
            &response.responses,
        );
        self.mirror_writes(request.shard_id, &request.commands, &response.responses);
        response
    }

    pub fn batch_execute_checked(
        &self,
        request: CheckedBatchExecuteRequest,
    ) -> CheckedBatchExecuteResponse {
        if let Err(status) = validate_foreground_write_allowed_inner(
            &self.inner,
            request.shard_id,
            &request.commands,
        ) {
            return CheckedBatchExecuteResponse {
                status: status.clone(),
                response: BatchExecuteResponse {
                    status,
                    responses: Vec::new(),
                },
            };
        }
        let response = self.inner.engine.batch_execute_checked(request.clone());
        if response.status.ok {
            mark_dirty_for_successful_commands(
                &self.inner.dirty,
                request.shard_id,
                &request.commands,
                &response.response.responses,
            );
            self.mirror_writes(
                request.shard_id,
                &request.commands,
                &response.response.responses,
            );
        }
        response
    }

    pub fn submit_execute(
        &self,
        request: ExecuteRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Execute(request),
            DataNodeTaskKind::Execute,
            controller,
        )
    }

    pub fn submit_checked_execute(
        &self,
        request: CheckedExecuteRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::CheckedExecute(request),
            DataNodeTaskKind::CheckedExecute,
            controller,
        )
    }

    pub fn submit_dump(
        &self,
        request: DumpShardRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Dump(request),
            DataNodeTaskKind::Dump,
            controller,
        )
    }

    pub fn submit_compaction(
        &self,
        request: CompactionRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::Compact(request),
            DataNodeTaskKind::Compact,
            controller,
        )
    }

    pub fn submit_gc(
        &self,
        request: GcRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(TaskRequest::Gc(request), DataNodeTaskKind::Gc, controller)
    }

    pub fn submit_storage_manager_cycle(
        &self,
        request: StorageManagerCycleRequest,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        self.submit(
            TaskRequest::StorageManager(request),
            DataNodeTaskKind::StorageManager,
            controller,
        )
    }

    pub fn job_status(&self, job_id: u64) -> Option<DataNodeTaskStatus> {
        self.inner
            .jobs
            .lock()
            .expect("data node jobs lock poisoned")
            .get(&job_id)
            .cloned()
    }

    pub fn cancel_job(&self, job_id: u64) -> DataNodeTaskStatus {
        let mut jobs = self
            .inner
            .jobs
            .lock()
            .expect("data node jobs lock poisoned");
        let Some(existing) = jobs.get(&job_id).cloned() else {
            return DataNodeTaskStatus {
                job_id,
                kind: DataNodeTaskKind::Execute,
                status: Status::error("job_not_found", "data node job not found"),
                output: None,
                submitted_at_ms: now_ms(),
                finished_at_ms: Some(now_ms()),
            };
        };
        if existing.finished_at_ms.is_some() {
            return DataNodeTaskStatus {
                status: Status::error("job_already_finished", "data node job already finished"),
                ..existing
            };
        }
        let mut queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        if !queue.remove_job(job_id) {
            self.inner
                .canceled
                .lock()
                .expect("runtime cancellation lock poisoned")
                .insert(job_id);
            let cancel_requested = DataNodeTaskStatus {
                status: Status::error(
                    "job_cancel_requested",
                    "data node job cancellation requested",
                ),
                ..existing
            };
            jobs.insert(job_id, cancel_requested.clone());
            return cancel_requested;
        }
        let canceled = DataNodeTaskStatus {
            status: Status::error("job_canceled", "data node job canceled before execution"),
            finished_at_ms: Some(now_ms()),
            ..existing
        };
        jobs.insert(job_id, canceled.clone());
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .canceled_total += 1;
        canceled
    }

    pub fn dirty_objects(&self) -> Vec<DirtyObjectInfo> {
        self.inner
            .dirty
            .lock()
            .expect("dirty tracker lock poisoned")
            .by_key
            .values()
            .cloned()
            .collect()
    }

    pub fn dirty_shards(&self) -> Vec<ShardId> {
        dirty_shards(&self.inner.dirty)
    }

    pub fn shard_worker_info(&self, shard_id: ShardId) -> ShardWorkerInfo {
        let worker_threads = self.inner.options.worker_threads.max(1);
        ShardWorkerInfo {
            shard_id,
            worker_index: shard_id as usize % worker_threads,
            worker_threads,
        }
    }

    pub fn queued_shard_worker_infos(&self) -> Vec<ShardWorkerInfo> {
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let worker_threads = self.inner.options.worker_threads.max(1);
        queue
            .by_shard
            .keys()
            .map(|shard_id| ShardWorkerInfo {
                shard_id: *shard_id,
                worker_index: *shard_id as usize % worker_threads,
                worker_threads,
            })
            .collect()
    }

    pub fn schedule_dirty_shard_dumps(
        &self,
        controller: RequestController,
    ) -> Vec<DataNodeTaskStatus> {
        self.dirty_shards()
            .into_iter()
            .filter(|shard_id| {
                !self
                    .inner
                    .queue
                    .lock()
                    .expect("runtime queue lock poisoned")
                    .has_pending_dump(*shard_id)
            })
            .map(|shard_id| {
                self.submit_dump(
                    DumpShardRequest {
                        shard_id,
                        selected_routing_buckets: Vec::new(),
                    },
                    controller,
                )
            })
            .collect()
    }



    fn submit(
        &self,
        request: TaskRequest,
        kind: DataNodeTaskKind,
        controller: RequestController,
    ) -> DataNodeTaskStatus {
        let job_id = self.inner.next_job_id.fetch_add(1, Ordering::Relaxed);
        let submitted_at_ms = now_ms();
        let status = DataNodeTaskStatus {
            job_id,
            kind,
            status: Status::ok(),
            output: None,
            submitted_at_ms,
            finished_at_ms: None,
        };
        {
            let priority = request.priority();
            let mut queue = self
                .inner
                .queue
                .lock()
                .expect("runtime queue lock poisoned");
            if queue.queued_total >= self.inner.options.max_queue_depth {
                self.inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned")
                    .rejected_total += 1;
                return DataNodeTaskStatus {
                    status: Status::error("queue_full", "data node worker queue is full"),
                    finished_at_ms: Some(now_ms()),
                    ..status
                };
            }
            if priority == TaskPriority::Background
                && queue.background_queued_total >= self.inner.options.max_background_queue_depth
            {
                let mut stats = self
                    .inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned");
                stats.rejected_total += 1;
                stats.rejected_background_total += 1;
                return DataNodeTaskStatus {
                    status: Status::error(
                        "background_queue_full",
                        "data node background worker queue is full",
                    ),
                    finished_at_ms: Some(now_ms()),
                    ..status
                };
            }
            queue.push(QueuedTask {
                job_id,
                kind,
                deadline: Instant::now() + Duration::from_millis(controller.timeout_ms),
                submitted_at_ms,
                request,
            });
            self.inner.queue_signal.notify_one();
        }
        self.inner
            .jobs
            .lock()
            .expect("data node jobs lock poisoned")
            .insert(job_id, status.clone());
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .submitted_total += 1;
        status
    }
}


fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}


pub fn distributed_admission_decision(
    shard_id: ShardId,
    peer_snapshots: &[DistributedAdmissionPeerSnapshot],
    read_budget: u64,
    write_budget: u64,
    min_topology_version: u64,
) -> DistributedAdmissionDecision {
    let relevant = peer_snapshots
        .iter()
        .filter(|snapshot| snapshot.shard_id == shard_id)
        .collect::<Vec<_>>();
    let aggregate_read_count = relevant
        .iter()
        .map(|snapshot| snapshot.read_count)
        .sum::<u64>();
    let aggregate_write_count = relevant
        .iter()
        .map(|snapshot| snapshot.write_count)
        .sum::<u64>();
    let topology_version = relevant
        .iter()
        .map(|snapshot| snapshot.topology_version)
        .min()
        .unwrap_or_default();
    let mut reasons = Vec::new();
    if relevant.is_empty() {
        reasons.push("missing_peer_snapshots".to_string());
    }
    if topology_version < min_topology_version {
        reasons.push("stale_distributed_admission_topology".to_string());
    }
    let read_allowed = read_budget == 0 || aggregate_read_count < read_budget;
    let write_allowed = write_budget == 0 || aggregate_write_count < write_budget;
    if !read_allowed {
        reasons.push("distributed_read_budget_exceeded".to_string());
    }
    if !write_allowed {
        reasons.push("distributed_write_budget_exceeded".to_string());
    }
    let status = if reasons.is_empty() {
        Status::ok()
    } else {
        Status::error("distributed_admission_rejected", reasons.join(","))
    };
    DistributedAdmissionDecision {
        shard_id,
        status,
        topology_version,
        participating_nodes: relevant.len(),
        aggregate_read_count,
        aggregate_write_count,
        read_budget,
        write_budget,
        read_allowed,
        write_allowed,
        reasons,
    }
}

pub fn validate_multi_process_lifecycle_reports(
    reports: &[DataNodeLifecycleReport],
    persistence_reports: &[DataNodeLifecyclePersistenceReport],
) -> MultiProcessLifecycleValidationReport {
    let mut operations = BTreeSet::new();
    let mut restart_restore_validated = false;
    for report in reports {
        for transition in &report.transitions {
            operations.insert((transition.operation.clone(), transition.state.clone()));
        }
    }
    for persistence in persistence_reports {
        if persistence.enabled
            && persistence.restore_success_total > 0
            && persistence
                .last_restore_status
                .as_ref()
                .is_some_and(|status| status.ok)
        {
            restart_restore_validated = true;
        }
    }
    let load_validated = operations.contains(&("load".to_string(), "serving".to_string()));
    let reload_validated = operations.contains(&("reload".to_string(), "readonly".to_string()))
        || operations.contains(&("reload".to_string(), "serving".to_string()));
    let unload_validated = operations.contains(&("unload".to_string(), "unloaded".to_string()));
    let all_nodes_have_persistence = !reports.is_empty()
        && reports.len() == persistence_reports.len()
        && persistence_reports.iter().all(|report| report.enabled);
    let mut blockers = Vec::new();
    if !load_validated {
        blockers.push("load_not_validated".to_string());
    }
    if !reload_validated {
        blockers.push("reload_not_validated".to_string());
    }
    if !unload_validated {
        blockers.push("unload_not_validated".to_string());
    }
    if !restart_restore_validated {
        blockers.push("restart_restore_not_validated".to_string());
    }
    if !all_nodes_have_persistence {
        blockers.push("lifecycle_persistence_not_enabled_on_all_nodes".to_string());
    }
    MultiProcessLifecycleValidationReport {
        node_count: reports.len(),
        load_validated,
        reload_validated,
        unload_validated,
        restart_restore_validated,
        all_nodes_have_persistence,
        passed: blockers.is_empty(),
        blockers,
    }
}

#[cfg(test)]
mod tests;
