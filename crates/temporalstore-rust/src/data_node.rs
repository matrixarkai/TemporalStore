use std::collections::{BTreeSet, HashMap, VecDeque};
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
use crate::engine::{
    ShardCompactionUtilityReport, SlotDumpManifest, StorageLifecyclePlan, StorageLifecycleReport,
    StorageLifecycleRequest, StorageProductionReadinessPolicy, StorageProductionReadinessReport,
    TemporalEngine,
};
use crate::meta::{
    ServerHeartbeatResponse, ServerRuntimeLoad, ServerShardServingState, TableTopologyResponse,
};
use crate::rebalance::SchedulerLifecycleToken;
use crate::types::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, ExecuteRequest,
    ExecuteResponse, ShardId, Status,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeLifecycleReport {
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
    pub selected_routing_slots: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DumpShardResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub index_bytes: usize,
    pub dirty_objects_flushed: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_dump_manifest: Option<SlotDumpManifest>,
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
    pub previous_page_segment_id: u64,
    #[serde(default)]
    pub compacted_page_segment_id: u64,
    #[serde(default)]
    pub stale_page_segment_ids: Vec<u64>,
    #[serde(default)]
    pub before: ShardCompactionUtilityReport,
    #[serde(default)]
    pub after: ShardCompactionUtilityReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcRequest {
    pub shard_id: ShardId,
    #[serde(default)]
    pub retain_oplog_from_sequence: Option<u64>,
    #[serde(default)]
    pub retain_index_log_from_sequence: Option<u64>,
    #[serde(default)]
    pub retain_page_segments_from_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GcResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub collected_objects: usize,
    pub cache_entries_removed: usize,
    pub cache_disk_bytes_removed: u64,
    pub oplog_records_removed: usize,
    pub index_log_records_removed: usize,
    pub page_segments_removed: usize,
    #[serde(default)]
    pub page_segments_removed_physical_bytes: u64,
    #[serde(default)]
    pub page_segments_retained_physical_bytes: u64,
    #[serde(default)]
    pub page_segments_retained_live: usize,
    #[serde(default)]
    pub page_segments_retained_live_physical_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle_plan: Option<StorageLifecyclePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageLifecycleResponse {
    pub status: Status,
    pub report: StorageLifecycleReport,
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
    next_job_id: AtomicU64,
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
        }
    }

    fn priority(&self) -> TaskPriority {
        match self {
            TaskRequest::Load(_)
            | TaskRequest::Reload(_)
            | TaskRequest::Unload(_)
            | TaskRequest::Execute(_)
            | TaskRequest::CheckedExecute(_) => TaskPriority::Foreground,
            TaskRequest::Dump(_) | TaskRequest::Compact(_) | TaskRequest::Gc(_) => {
                TaskPriority::Background
            }
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
            next_job_id: AtomicU64::new(1),
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
            next_job_id: AtomicU64::new(1),
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
                command_key(&request.command),
            );
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
                command_key(&request.command),
            );
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
                        selected_routing_slots: Vec::new(),
                    },
                    controller,
                )
            })
            .collect()
    }

    pub fn storage_lifecycle_plan(&self, request: StorageLifecycleRequest) -> StorageLifecyclePlan {
        self.inner.engine.storage_lifecycle_plan(request)
    }

    pub fn storage_production_readiness_report(
        &self,
        shard_id: ShardId,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report(shard_id)
    }

    pub fn storage_production_readiness_report_with_policy(
        &self,
        shard_id: ShardId,
        policy: StorageProductionReadinessPolicy,
    ) -> StorageProductionReadinessReport {
        self.inner
            .engine
            .storage_production_readiness_report_with_policy(shard_id, policy)
    }

    pub fn apply_storage_lifecycle(
        &self,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleResponse {
        let report = self.inner.engine.apply_storage_lifecycle(request);
        self.inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .storage_lifecycle_runs += 1;
        StorageLifecycleResponse {
            status: Status::ok(),
            report,
        }
    }

    pub fn start_dirty_dump_scheduler(
        &self,
        interval: Duration,
        controller: RequestController,
    ) -> DirtyDumpScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.schedule_dirty_shard_dumps(controller);
            }
        });
        DirtyDumpScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn start_storage_lifecycle_scheduler(
        &self,
        interval: Duration,
        request: StorageLifecycleRequest,
    ) -> StorageLifecycleScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.apply_storage_lifecycle(request.clone());
            }
        });
        StorageLifecycleScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn sweep_expired_records(&self) -> usize {
        let reports = self.inner.engine.sweep_all_expired_records();
        let removed = reports
            .iter()
            .map(|report| report.expired_records_removed)
            .sum::<usize>();
        let mut stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        stats.expiry_sweeps += 1;
        stats.expired_records_removed += removed as u64;
        removed
    }

    pub fn start_expiry_sweep_scheduler(&self, interval: Duration) -> ExpirySweepScheduler {
        let runtime = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let scheduler_stop = Arc::clone(&stop);
        let sleep_interval = interval.max(Duration::from_millis(1));
        let handle = thread::spawn(move || {
            while !scheduler_stop.load(Ordering::Relaxed) {
                thread::sleep(sleep_interval);
                if scheduler_stop.load(Ordering::Relaxed) {
                    break;
                }
                runtime.sweep_expired_records();
            }
        });
        ExpirySweepScheduler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stats(&self) -> DataNodeRuntimeStats {
        let stats = self
            .inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned");
        let dirty = self
            .inner
            .dirty
            .lock()
            .expect("dirty tracker lock poisoned");
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let queue_depth = queue.queued_total;
        let queued_shard_count = queue.by_shard.len();
        let running_shard_count = queue.running_shards.len();
        let dirty_shard_count = dirty
            .by_key
            .values()
            .map(|info| info.shard_id)
            .collect::<BTreeSet<_>>()
            .len();
        DataNodeRuntimeStats {
            worker_threads: self.inner.options.worker_threads,
            max_queue_depth: self.inner.options.max_queue_depth,
            max_background_queue_depth: self.inner.options.max_background_queue_depth,
            submitted_total: stats.submitted_total,
            completed_total: stats.completed_total,
            rejected_total: stats.rejected_total,
            rejected_background_total: stats.rejected_background_total,
            timed_out_total: stats.timed_out_total,
            canceled_total: stats.canceled_total,
            queue_depth,
            background_queue_depth: queue.background_queued_total,
            queued_shard_count,
            running_shard_count,
            dirty_object_count: dirty.by_key.len(),
            dirty_shard_count,
            expiry_sweeps: stats.expiry_sweeps,
            expired_records_removed: stats.expired_records_removed,
            dump_runs: stats.dump_runs,
            compaction_runs: stats.compaction_runs,
            gc_runs: stats.gc_runs,
            storage_lifecycle_runs: stats.storage_lifecycle_runs,
        }
    }

    pub fn preflight_report(&self) -> DataNodePreflightReport {
        let stats = self.stats();
        let metaserver = self.metaserver_heartbeat_report();
        let mut degraded_reasons = Vec::new();
        if stats.queue_depth >= stats.max_queue_depth && stats.max_queue_depth > 0 {
            degraded_reasons.push("foreground_queue_full".to_string());
        }
        if stats.background_queue_depth >= stats.max_background_queue_depth
            && stats.max_background_queue_depth > 0
        {
            degraded_reasons.push("background_queue_full".to_string());
        }
        if stats.rejected_total > 0 {
            degraded_reasons.push("rejected_requests".to_string());
        }
        if stats.timed_out_total > 0 {
            degraded_reasons.push("timed_out_requests".to_string());
        }
        if metaserver.last_heartbeat_at_ms > 0 && !metaserver.last_status.ok {
            degraded_reasons.push("metaserver_heartbeat_failed".to_string());
        }
        if metaserver.forbid_auto_register {
            degraded_reasons.push("metaserver_forbid_auto_register".to_string());
        }
        let lifecycle_persistence = self.lifecycle_persistence_report();
        if lifecycle_persistence
            .last_restore_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_restore_failed".to_string());
        }
        if lifecycle_persistence
            .last_persist_status
            .as_ref()
            .map(|status| !status.ok)
            .unwrap_or(false)
        {
            degraded_reasons.push("lifecycle_snapshot_persist_failed".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        DataNodePreflightReport {
            status,
            stats,
            lifecycle: self.lifecycle_report(),
            lifecycle_persistence,
            topology_validation: self.topology_validation_report(&metaserver),
            metaserver,
            queued_workers: self.queued_shard_worker_infos(),
            dirty_shards: self.dirty_shards(),
            dirty_objects: self.dirty_objects(),
            degraded_reasons,
        }
    }

    pub fn lifecycle_report(&self) -> DataNodeLifecycleReport {
        let mut shards = self.shard_serving_states();
        shards.sort_by_key(|state| state.shard_id);
        let mut transitions = self.lifecycle_states();
        transitions.sort_by_key(|state| state.shard_id);
        let loaded_shard_count = shards.iter().filter(|state| state.loaded).count();
        let serving_count = shards
            .iter()
            .filter(|state| state.serving_state == "serving")
            .count();
        let readonly_count = shards.iter().filter(|state| state.readonly).count();
        let queued_count = shards
            .iter()
            .filter(|state| state.serving_state == "queued")
            .count();
        let running_count = shards
            .iter()
            .filter(|state| state.serving_state == "running")
            .count();
        let unloading_count = shards
            .iter()
            .filter(|state| state.serving_state == "unloading")
            .count();
        let failed_shards = shards
            .iter()
            .filter(|state| state.serving_state == "failed")
            .map(|state| state.shard_id)
            .chain(
                transitions
                    .iter()
                    .filter(|state| state.state == "failed")
                    .map(|state| state.shard_id),
            )
            .collect::<BTreeSet<_>>();
        let failed_count = failed_shards.len();
        let max_load_version = shards
            .iter()
            .map(|state| state.load_version)
            .chain(transitions.iter().map(|state| state.load_version))
            .max()
            .unwrap_or_default();
        DataNodeLifecycleReport {
            loaded_shard_count,
            serving_count,
            readonly_count,
            queued_count,
            running_count,
            unloading_count,
            failed_count,
            max_load_version,
            shards,
            transitions,
        }
    }

    pub fn grpc_streaming_contract(&self) -> DataNodeGrpcStreamingContract {
        DataNodeGrpcStreamingContract::default()
    }

    pub fn distributed_admission_decision(
        &self,
        shard_id: ShardId,
        peer_snapshots: &[DistributedAdmissionPeerSnapshot],
        read_budget: u64,
        write_budget: u64,
        min_topology_version: u64,
    ) -> DistributedAdmissionDecision {
        distributed_admission_decision(
            shard_id,
            peer_snapshots,
            read_budget,
            write_budget,
            min_topology_version,
        )
    }

    pub fn lifecycle_states(&self) -> Vec<DataNodeShardLifecycleState> {
        self.inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub fn topology_validation_report(
        &self,
        metaserver: &DataNodeMetaHeartbeatReport,
    ) -> DataNodeTopologyValidationReport {
        let mut loaded_shards = self
            .shard_serving_states()
            .into_iter()
            .filter(|state| state.loaded)
            .map(|state| state.shard_id)
            .collect::<Vec<_>>();
        loaded_shards.sort_unstable();
        DataNodeTopologyValidationReport {
            loaded_shard_count: loaded_shards.len(),
            loaded_shards,
            last_meta_topology_version: metaserver.last_topology_version,
            authoritative_topology_version: 0,
            validated_against_metaserver: false,
            mismatch_count: 0,
            missing_in_meta: Vec::new(),
            mismatches: Vec::new(),
            validation_limited: true,
            limitation_reason:
                "metaserver topology partition map is not attached to node preflight".to_string(),
        }
    }

    pub fn validate_topology_against_metaserver(
        &self,
        server_addr: &str,
        topologies: &[TableTopologyResponse],
    ) -> DataNodeTopologyValidationReport {
        let metaserver = self.metaserver_heartbeat_report();
        let loaded_states = self
            .shard_serving_states()
            .into_iter()
            .filter(|state| state.loaded)
            .collect::<Vec<_>>();
        let mut loaded_shards = loaded_states
            .iter()
            .map(|state| state.shard_id)
            .collect::<Vec<_>>();
        loaded_shards.sort_unstable();

        let authoritative_topology_version = topologies
            .iter()
            .filter_map(|topology| topology.table.as_ref().map(|table| table.topology_version))
            .max()
            .unwrap_or_default();
        let mut missing_in_meta = Vec::new();
        let mut mismatches = Vec::new();
        for state in &loaded_states {
            let partition = topologies
                .iter()
                .flat_map(|topology| topology.partitions.iter())
                .find(|partition| partition.shard_id == state.shard_id);
            let Some(partition) = partition else {
                missing_in_meta.push(state.shard_id);
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "missing_partition".to_string(),
                    detail: "loaded shard is not present in supplied metaserver topology"
                        .to_string(),
                });
                continue;
            };
            if !state.readonly && partition.primary.as_deref() != Some(server_addr) {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "primary_mismatch".to_string(),
                    detail: format!(
                        "node serves primary but metaserver primary is {:?}",
                        partition.primary
                    ),
                });
            }
            if state.readonly
                && partition.primary.as_deref() != Some(server_addr)
                && !partition
                    .replicas
                    .iter()
                    .any(|replica| replica == server_addr)
            {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "replica_mismatch".to_string(),
                    detail: "readonly loaded shard is neither primary nor replica in metaserver topology"
                        .to_string(),
                });
            }
            if u64::from(state.start_routing_slot) != partition.start_slot
                || u64::from(state.end_routing_slot) != partition.end_slot
            {
                mismatches.push(DataNodeTopologyMismatch {
                    shard_id: state.shard_id,
                    kind: "routing_slot_mismatch".to_string(),
                    detail: format!(
                        "local={}..{}, meta={}..{}",
                        state.start_routing_slot,
                        state.end_routing_slot,
                        partition.start_slot,
                        partition.end_slot
                    ),
                });
            }
        }
        let mismatch_count = mismatches.len();
        DataNodeTopologyValidationReport {
            loaded_shard_count: loaded_shards.len(),
            loaded_shards,
            last_meta_topology_version: metaserver.last_topology_version,
            authoritative_topology_version,
            validated_against_metaserver: true,
            mismatch_count,
            missing_in_meta,
            mismatches,
            validation_limited: false,
            limitation_reason: String::new(),
        }
    }

    pub fn metaserver_heartbeat_report(&self) -> DataNodeMetaHeartbeatReport {
        self.inner
            .meta_heartbeat
            .lock()
            .expect("meta heartbeat lock poisoned")
            .clone()
    }

    pub fn record_metaserver_heartbeat(&self, response: &ServerHeartbeatResponse) {
        let mut report = self
            .inner
            .meta_heartbeat
            .lock()
            .expect("meta heartbeat lock poisoned");
        report.last_heartbeat_at_ms = now_ms();
        report.last_status = response.status.clone();
        report.last_topology_version = response.topology_version;
        report.last_server_state = response.server_state.clone();
        report.forbid_auto_register = response.forbid_auto_register;
        report.consecutive_failures = if response.status.ok {
            0
        } else {
            report.consecutive_failures.saturating_add(1)
        };
    }

    pub fn server_runtime_load(&self) -> ServerRuntimeLoad {
        let preflight = self.preflight_report();
        let metaserver = preflight.metaserver.clone();
        ServerRuntimeLoad {
            queue_depth: preflight.stats.queue_depth,
            background_queue_depth: preflight.stats.background_queue_depth,
            queued_shard_count: preflight.stats.queued_shard_count,
            running_shard_count: preflight.stats.running_shard_count,
            dirty_object_count: preflight.stats.dirty_object_count,
            dirty_shard_count: preflight.stats.dirty_shard_count,
            rejected_total: preflight.stats.rejected_total,
            rejected_background_total: preflight.stats.rejected_background_total,
            timed_out_total: preflight.stats.timed_out_total,
            canceled_total: preflight.stats.canceled_total,
            dump_runs: preflight.stats.dump_runs,
            compaction_runs: preflight.stats.compaction_runs,
            gc_runs: preflight.stats.gc_runs,
            storage_lifecycle_runs: preflight.stats.storage_lifecycle_runs,
            last_meta_topology_version: metaserver.last_topology_version,
            meta_heartbeat_consecutive_failures: metaserver.consecutive_failures,
            meta_forbid_auto_register: metaserver.forbid_auto_register,
            degraded_reasons: preflight.degraded_reasons,
        }
    }

    pub fn shard_serving_states(&self) -> Vec<ServerShardServingState> {
        let dirty_objects = self.dirty_objects();
        let mut dirty_by_shard = HashMap::<ShardId, usize>::new();
        for dirty in dirty_objects {
            *dirty_by_shard.entry(dirty.shard_id).or_default() += 1;
        }
        let queue = self
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned");
        let queued_shards = queue.by_shard.keys().copied().collect::<BTreeSet<_>>();
        let running_shards = queue.running_shards.clone();
        drop(queue);
        let lifecycle_by_shard = self
            .inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned")
            .clone();

        self.inner
            .engine
            .loaded_shard_stats()
            .into_iter()
            .map(|stats| {
                let lifecycle_state = lifecycle_by_shard
                    .get(&stats.shard_id)
                    .map(|state| state.state.as_str());
                let serving_state = if matches!(
                    lifecycle_state,
                    Some("loading" | "reloading" | "unloading" | "failed")
                ) {
                    lifecycle_state.unwrap()
                } else if running_shards.contains(&stats.shard_id) {
                    "running"
                } else if queued_shards.contains(&stats.shard_id) {
                    "queued"
                } else if stats.readonly {
                    "readonly"
                } else if stats.loaded {
                    "serving"
                } else {
                    "unloaded"
                };
                let worker = self.shard_worker_info(stats.shard_id);
                ServerShardServingState {
                    shard_id: stats.shard_id,
                    serving_state: serving_state.to_string(),
                    worker_index: worker.worker_index,
                    worker_threads: worker.worker_threads,
                    loaded: stats.loaded,
                    readonly: stats.readonly,
                    load_version: stats.load_version,
                    table_name: stats.partition_info.table_name,
                    shard_uri: stats.partition_info.shard_uri,
                    start_routing_slot: stats.partition_info.start_routing_slot,
                    end_routing_slot: stats.partition_info.end_routing_slot,
                    total_records: stats.total_records,
                    storage_bytes: stats.storage_bytes,
                    cache_memory_bytes: stats.cache.memory_bytes,
                    page_store_bytes_written: stats.page_store.bytes_written,
                    oplog_sequence: stats.oplog.last_sequence,
                    dirty_object_count: dirty_by_shard
                        .get(&stats.shard_id)
                        .copied()
                        .unwrap_or_default() as u64,
                    dirty_slot_count: stats.object_manager.dirty_slot_count as u64,
                }
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

fn load_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: LoadShardRequest,
) -> LoadShardResponse {
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "load", request.load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "load",
            request.load_version,
            Some(status.clone()),
        );
        return LoadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "loading",
        "load",
        request.load_version,
        None,
    );
    let response = inner.engine.load_shard_with(request.clone());
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        final_loaded_lifecycle_state(request.readonly, &response.status),
        "load",
        request.load_version,
        Some(response.status.clone()),
    );
    response
}

fn reload_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: LoadShardRequest,
) -> LoadShardResponse {
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "reload", request.load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "reload",
            request.load_version,
            Some(status.clone()),
        );
        return LoadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "reloading",
        "reload",
        request.load_version,
        None,
    );
    let response = inner.engine.reload_shard_with(request.clone());
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        final_loaded_lifecycle_state(request.readonly, &response.status),
        "reload",
        request.load_version,
        Some(response.status.clone()),
    );
    response
}

fn unload_shard_with_inner(
    inner: &DataNodeRuntimeInner,
    request: UnloadShardRequest,
    reject_when_busy: bool,
) -> UnloadShardResponse {
    let load_version = inner
        .engine
        .get_info(request.shard_id)
        .info
        .map(|info| info.load_version)
        .unwrap_or_default();
    if reject_when_busy && shard_has_queued_or_running_work(inner, request.shard_id) {
        let status = Status::error(
            "shard_busy",
            format!(
                "cannot unload shard {} while data node work is queued or running",
                request.shard_id
            ),
        );
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "unload",
            load_version,
            Some(status.clone()),
        );
        return UnloadShardResponse { status };
    }
    if let Err(status) =
        validate_lifecycle_token_inner(inner, request.shard_id, "unload", load_version)
    {
        record_lifecycle_state_inner(
            inner,
            request.shard_id,
            "failed",
            "unload",
            load_version,
            Some(status.clone()),
        );
        return UnloadShardResponse { status };
    }
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        "unloading",
        "unload",
        load_version,
        None,
    );
    let response = inner.engine.unload_shard_with(request.clone());
    let state = if response.status.ok {
        "unloaded"
    } else {
        "failed"
    };
    record_lifecycle_state_inner(
        inner,
        request.shard_id,
        state,
        "unload",
        load_version,
        Some(response.status.clone()),
    );
    response
}

fn lifecycle_snapshot_path_from_env() -> Option<PathBuf> {
    std::env::var_os("TS_DATA_NODE_LIFECYCLE_SNAPSHOT").map(PathBuf::from)
}

fn lifecycle_persistence_report_for_path(
    path: Option<&PathBuf>,
) -> DataNodeLifecyclePersistenceReport {
    DataNodeLifecyclePersistenceReport {
        enabled: path.is_some(),
        path: path.map(|path| path.display().to_string()),
        ..DataNodeLifecyclePersistenceReport::default()
    }
}

fn lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) -> DataNodeLifecycleSnapshot {
    let mut transitions = inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    transitions.sort_by_key(|state| (state.shard_id, state.operation.clone()));
    let mut tokens = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .values()
        .cloned()
        .collect::<Vec<_>>();
    tokens.sort_by_key(|token| (token.shard_id, token.operation.clone()));
    DataNodeLifecycleSnapshot {
        format_version: 1,
        transitions,
        tokens,
    }
}

fn restore_lifecycle_snapshot_inner(
    inner: &DataNodeRuntimeInner,
    snapshot: DataNodeLifecycleSnapshot,
) -> Status {
    if snapshot.format_version != 1 {
        return Status::error(
            "bad_lifecycle_snapshot",
            "unsupported data node lifecycle snapshot version",
        );
    }
    {
        let mut lifecycle = inner
            .lifecycle
            .lock()
            .expect("runtime lifecycle lock poisoned");
        lifecycle.clear();
        for state in snapshot.transitions {
            lifecycle.insert(state.shard_id, state);
        }
    }
    {
        let mut tokens = inner
            .lifecycle_tokens
            .lock()
            .expect("runtime lifecycle token lock poisoned");
        tokens.clear();
        for token in snapshot.tokens {
            tokens.insert((token.shard_id, token.operation.clone()), token);
        }
    }
    Status::ok()
}

fn restore_lifecycle_snapshot_from_path_inner(inner: &DataNodeRuntimeInner) {
    let Some(path) = &inner.lifecycle_snapshot_path else {
        return;
    };
    let status = match fs::read(path) {
        Ok(bytes) => match serde_json::from_slice::<DataNodeLifecycleSnapshot>(&bytes) {
            Ok(snapshot) => restore_lifecycle_snapshot_inner(inner, snapshot),
            Err(err) => Status::error("bad_lifecycle_snapshot", err.to_string()),
        },
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Status::ok(),
        Err(err) => Status::error("lifecycle_snapshot_io", err.to_string()),
    };
    let mut report = inner
        .lifecycle_persistence
        .lock()
        .expect("runtime lifecycle persistence lock poisoned");
    report.last_restore_at_ms = now_ms();
    report.last_restore_status = Some(status.clone());
    if status.ok {
        report.restore_success_total += 1;
    } else {
        report.restore_failure_total += 1;
    }
}

fn persist_lifecycle_snapshot_inner(inner: &DataNodeRuntimeInner) {
    let Some(path) = &inner.lifecycle_snapshot_path else {
        return;
    };
    let result: Result<(), Status> = if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|err| Status::error("lifecycle_snapshot_io", err.to_string()))
        } else {
            Ok(())
        }
    } else {
        Ok(())
    }
    .and_then(|_| {
        serde_json::to_vec_pretty(&lifecycle_snapshot_inner(inner))
            .map_err(|err| Status::error("bad_lifecycle_snapshot", err.to_string()))
    })
    .and_then(|bytes| {
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, bytes)
            .and_then(|_| fs::rename(&tmp_path, path))
            .map_err(|err| Status::error("lifecycle_snapshot_io", err.to_string()))
    });
    let status = match result {
        Ok(()) => Status::ok(),
        Err(status) => status,
    };
    let mut report = inner
        .lifecycle_persistence
        .lock()
        .expect("runtime lifecycle persistence lock poisoned");
    report.last_persist_at_ms = now_ms();
    report.last_persist_status = Some(status.clone());
    if status.ok {
        report.persist_success_total += 1;
    } else {
        report.persist_failure_total += 1;
    }
}

fn shard_has_queued_or_running_work(inner: &DataNodeRuntimeInner, shard_id: ShardId) -> bool {
    let queue = inner.queue.lock().expect("runtime queue lock poisoned");
    queue.running_shards.contains(&shard_id)
        || queue
            .by_shard
            .get(&shard_id)
            .map(|queued| !queued.is_empty())
            .unwrap_or(false)
}

fn record_lifecycle_state_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    state: &str,
    operation: &str,
    load_version: u64,
    last_status: Option<Status>,
) {
    let token = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .get(&(shard_id, operation.to_string()))
        .cloned();
    inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .insert(
            shard_id,
            DataNodeShardLifecycleState {
                shard_id,
                state: state.to_string(),
                operation: operation.to_string(),
                load_version,
                updated_at_ms: now_ms(),
                last_status,
                scheduler_task_id: token.as_ref().map(|token| token.task_id),
                scheduler_generation: token.as_ref().map(|token| token.generation),
            },
        );
    persist_lifecycle_snapshot_inner(inner);
}

fn validate_lifecycle_token_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    operation: &str,
    load_version: u64,
) -> Result<(), Status> {
    let token = inner
        .lifecycle_tokens
        .lock()
        .expect("runtime lifecycle token lock poisoned")
        .get(&(shard_id, operation.to_string()))
        .cloned();
    let Some(token) = token else {
        return Ok(());
    };
    if token.operation != operation {
        return Err(Status::error(
            "lifecycle_token_mismatch",
            format!(
                "expected lifecycle operation {}, got {operation}",
                token.operation
            ),
        ));
    }
    if token.load_version != 0 && token.load_version != load_version {
        return Err(Status::error(
            "lifecycle_token_mismatch",
            format!(
                "expected lifecycle load_version {}, got {load_version}",
                token.load_version
            ),
        ));
    }
    Ok(())
}

fn validate_foreground_write_allowed_inner(
    inner: &DataNodeRuntimeInner,
    shard_id: ShardId,
    commands: &[Command],
) -> Result<(), Status> {
    if !commands.iter().any(is_write_command) {
        return Ok(());
    }
    let lifecycle = inner
        .lifecycle
        .lock()
        .expect("runtime lifecycle lock poisoned")
        .get(&shard_id)
        .cloned();
    let Some(lifecycle) = lifecycle else {
        return Ok(());
    };
    if matches!(
        lifecycle.state.as_str(),
        "loading" | "reloading" | "unloading"
    ) {
        return Err(Status::error(
            "lifecycle_write_blocked",
            format!(
                "foreground write rejected while shard {} is {} for {}",
                shard_id, lifecycle.state, lifecycle.operation
            ),
        ));
    }
    Ok(())
}

fn final_loaded_lifecycle_state(readonly: bool, status: &Status) -> &'static str {
    if !status.ok {
        "failed"
    } else if readonly {
        "readonly"
    } else {
        "serving"
    }
}

fn worker_loop(inner: Arc<DataNodeRuntimeInner>) {
    loop {
        let task = {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            loop {
                if let Some(task) = queue.pop_ready() {
                    break task;
                }
                queue = inner
                    .queue_signal
                    .wait(queue)
                    .expect("runtime queue lock poisoned");
            }
        };
        let shard_id = task.request.shard_id();
        let output = if Instant::now() > task.deadline {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .timed_out_total += 1;
            task_timeout_output(task.kind)
        } else if take_canceled(&inner, task.job_id) {
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .canceled_total += 1;
            task_canceled_output(&task, "data node task canceled before execution")
        } else {
            let output = execute_task(&inner, &task);
            if task_output_status(&output).code == "job_canceled"
                && take_canceled(&inner, task.job_id)
            {
                inner
                    .stats
                    .lock()
                    .expect("runtime stats lock poisoned")
                    .canceled_total += 1;
            } else {
                let _ = take_canceled(&inner, task.job_id);
            }
            output
        };
        {
            let mut queue = inner.queue.lock().expect("runtime queue lock poisoned");
            queue.finish_shard(shard_id);
            inner.queue_signal.notify_all();
        }
        let finished = DataNodeTaskStatus {
            job_id: task.job_id,
            kind: task.kind,
            status: task_output_status(&output),
            output: Some(output),
            submitted_at_ms: task.submitted_at_ms,
            finished_at_ms: Some(now_ms()),
        };
        inner
            .jobs
            .lock()
            .expect("data node jobs lock poisoned")
            .insert(task.job_id, finished);
        inner
            .stats
            .lock()
            .expect("runtime stats lock poisoned")
            .completed_total += 1;
    }
}

fn execute_task(inner: &DataNodeRuntimeInner, task: &QueuedTask) -> DataNodeTaskOutput {
    let cancellation = TaskCancellation {
        inner,
        job_id: task.job_id,
    };
    match &task.request {
        TaskRequest::Load(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node load canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Load(load_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Reload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node reload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Reload(reload_shard_with_inner(inner, request.clone()))
        }
        TaskRequest::Unload(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(
                    task,
                    "data node unload canceled before lifecycle start",
                );
            }
            DataNodeTaskOutput::Unload(unload_shard_with_inner(inner, request.clone(), false))
        }
        TaskRequest::Execute(request) => {
            if let Err(status) = validate_foreground_write_allowed_inner(
                inner,
                request.shard_id,
                std::slice::from_ref(&request.command),
            ) {
                return DataNodeTaskOutput::Execute(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
            }
            let response = inner.engine.execute(request.clone());
            if response.status.ok && is_write_command(&request.command) {
                mark_dirty(
                    &inner.dirty,
                    request.shard_id,
                    command_key(&request.command),
                );
            }
            DataNodeTaskOutput::Execute(response)
        }
        TaskRequest::CheckedExecute(request) => {
            if let Err(status) = validate_foreground_write_allowed_inner(
                inner,
                request.shard_id,
                std::slice::from_ref(&request.command),
            ) {
                return DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                    status: status.clone(),
                    response: ExecuteResponse {
                        status,
                        response: CommandResponse::Empty,
                    },
                });
            }
            let response = inner.engine.execute_checked(request.clone());
            if response.status.ok && is_write_command(&request.command) {
                mark_dirty(
                    &inner.dirty,
                    request.shard_id,
                    command_key(&request.command),
                );
            }
            DataNodeTaskOutput::CheckedExecute(response)
        }
        TaskRequest::Dump(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before index export");
            }
            let index_bytes = inner
                .engine
                .export_index_bytes(request.shard_id)
                .map(|bytes| bytes.len())
                .unwrap_or_default();
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node dump canceled before dirty flush");
            }
            let selected_slots = if request.selected_routing_slots.is_empty() {
                inner
                    .engine
                    .storage_lifecycle_plan(StorageLifecycleRequest {
                        shard_id: request.shard_id,
                        selected_dump_slots: Vec::new(),
                        max_dump_slots_per_round: 0,
                        min_undumped_oplog_records: 0,
                        purge_delayed_destroy: false,
                        prune_slot_dump_manifests: false,
                        roll_forward_slot_dump_installs: false,
                        follower_replay_cursors: Vec::new(),
                        invalidate_cache: false,
                        warm_cache: false,
                    })
                    .selected_dump_slots
            } else {
                request.selected_routing_slots.clone()
            };
            let slot_dump_manifest = inner
                .engine
                .create_slot_dump_manifest(request.shard_id, selected_slots.clone())
                .ok();
            let dirty_objects_flushed = clear_dirty_shard_slots(
                &inner.dirty,
                &inner.engine,
                request.shard_id,
                &selected_slots,
            );
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .dump_runs += 1;
            DataNodeTaskOutput::Dump(DumpShardResponse {
                status: Status::ok(),
                shard_id: request.shard_id,
                index_bytes,
                dirty_objects_flushed,
                slot_dump_manifest,
            })
        }
        TaskRequest::Compact(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node compaction canceled before scan");
            }
            let compaction = inner.engine.compact_shard_pages(request.shard_id);
            let (
                status,
                compacted_objects,
                previous_page_segment_id,
                compacted_page_segment_id,
                stale_page_segment_ids,
                before,
                after,
            ) = match compaction {
                Ok(report) => (
                    Status::ok(),
                    report.rewritten_page_refs,
                    report.previous_page_segment_id,
                    report.compacted_page_segment_id,
                    report.stale_page_segment_ids,
                    report.before,
                    report.after,
                ),
                Err(status) => (
                    status,
                    0,
                    0,
                    0,
                    Vec::new(),
                    ShardCompactionUtilityReport::default(),
                    ShardCompactionUtilityReport::default(),
                ),
            };
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .compaction_runs += 1;
            DataNodeTaskOutput::Compact(CompactionResponse {
                status,
                shard_id: request.shard_id,
                compacted_objects,
                previous_page_segment_id,
                compacted_page_segment_id,
                stale_page_segment_ids,
                before,
                after,
            })
        }
        TaskRequest::Gc(request) => {
            if cancellation.is_requested() {
                return task_canceled_output(task, "data node gc canceled before dirty cleanup");
            }
            let collected_objects = clear_dirty_shard(&inner.dirty, request.shard_id);
            let mut status = Status::ok();
            let mut cache_entries_removed = 0;
            let mut cache_disk_bytes_removed = 0;
            let mut oplog_records_removed = 0;
            let mut index_log_records_removed = 0;
            let mut page_segments_removed = 0;
            let mut page_segments_removed_physical_bytes = 0;
            let mut page_segments_retained_physical_bytes = 0;
            let mut page_segments_retained_live = 0;
            let mut page_segments_retained_live_physical_bytes = 0;
            let mut lifecycle_plan = None;
            if cancellation.is_requested() {
                return DataNodeTaskOutput::Gc(GcResponse {
                    status: Status::error(
                        "job_canceled",
                        "data node gc canceled before cache cleanup",
                    ),
                    shard_id: request.shard_id,
                    collected_objects,
                    cache_entries_removed,
                    cache_disk_bytes_removed,
                    oplog_records_removed,
                    index_log_records_removed,
                    page_segments_removed,
                    page_segments_removed_physical_bytes,
                    page_segments_retained_physical_bytes,
                    page_segments_retained_live,
                    page_segments_retained_live_physical_bytes,
                    lifecycle_plan,
                });
            }
            match inner.engine.cache().invalidate_shard(request.shard_id) {
                Ok(report) => {
                    cache_entries_removed = report.memory_entries_removed;
                    cache_disk_bytes_removed = report.disk_bytes_removed;
                }
                Err(err) => {
                    status = Status::error("cache_gc_failed", &err.to_string());
                }
            }
            if cancellation.is_requested() {
                return DataNodeTaskOutput::Gc(GcResponse {
                    status: Status::error(
                        "job_canceled",
                        "data node gc canceled before oplog cleanup",
                    ),
                    shard_id: request.shard_id,
                    collected_objects,
                    cache_entries_removed,
                    cache_disk_bytes_removed,
                    oplog_records_removed,
                    index_log_records_removed,
                    page_segments_removed,
                    page_segments_removed_physical_bytes,
                    page_segments_retained_physical_bytes,
                    page_segments_retained_live,
                    page_segments_retained_live_physical_bytes,
                    lifecycle_plan,
                });
            }
            if let Some(retain_from_sequence) = request.retain_oplog_from_sequence {
                if status.ok {
                    match inner
                        .engine
                        .oplog_store()
                        .gc_before_sequence(request.shard_id, retain_from_sequence)
                    {
                        Ok(report) => oplog_records_removed = report.records_removed,
                        Err(err) => {
                            status = Status::error("oplog_gc_failed", &err.to_string());
                        }
                    }
                }
            }
            if cancellation.is_requested() {
                return DataNodeTaskOutput::Gc(GcResponse {
                    status: Status::error(
                        "job_canceled",
                        "data node gc canceled before index-log cleanup",
                    ),
                    shard_id: request.shard_id,
                    collected_objects,
                    cache_entries_removed,
                    cache_disk_bytes_removed,
                    oplog_records_removed,
                    index_log_records_removed,
                    page_segments_removed,
                    page_segments_removed_physical_bytes,
                    page_segments_retained_physical_bytes,
                    page_segments_retained_live,
                    page_segments_retained_live_physical_bytes,
                    lifecycle_plan,
                });
            }
            if status.ok {
                if let Some(retain_from_sequence) = request.retain_index_log_from_sequence {
                    match inner
                        .engine
                        .index_log_store()
                        .gc_before_sequence(request.shard_id, retain_from_sequence)
                    {
                        Ok(report) => index_log_records_removed = report.records_removed,
                        Err(err) => {
                            status = Status::error("index_log_gc_failed", &err.to_string());
                        }
                    }
                }
            }
            if cancellation.is_requested() {
                return DataNodeTaskOutput::Gc(GcResponse {
                    status: Status::error(
                        "job_canceled",
                        "data node gc canceled before page-segment cleanup",
                    ),
                    shard_id: request.shard_id,
                    collected_objects,
                    cache_entries_removed,
                    cache_disk_bytes_removed,
                    oplog_records_removed,
                    index_log_records_removed,
                    page_segments_removed,
                    page_segments_removed_physical_bytes,
                    page_segments_retained_physical_bytes,
                    page_segments_retained_live,
                    page_segments_retained_live_physical_bytes,
                    lifecycle_plan,
                });
            }
            if status.ok {
                if let Some(retain_from_page_segment_id) = request.retain_page_segments_from_id {
                    let live_page_segment_ids =
                        inner.engine.live_page_segment_ids(request.shard_id);
                    match inner.engine.page_store().gc_segments_before_with_live_refs(
                        retain_from_page_segment_id,
                        live_page_segment_ids,
                    ) {
                        Ok(report) => {
                            page_segments_removed = report.removed_page_segment_ids.len();
                            page_segments_removed_physical_bytes = report.removed_physical_bytes;
                            page_segments_retained_physical_bytes = report.retained_physical_bytes;
                            page_segments_retained_live =
                                report.retained_live_page_segment_ids.len();
                            page_segments_retained_live_physical_bytes =
                                report.retained_live_physical_bytes;
                        }
                        Err(err) => {
                            status = Status::error("page_store_gc_failed", &err.to_string());
                        }
                    }
                }
            }
            lifecycle_plan = Some(
                inner
                    .engine
                    .storage_lifecycle_plan(StorageLifecycleRequest {
                        shard_id: request.shard_id,
                        selected_dump_slots: Vec::new(),
                        max_dump_slots_per_round: 0,
                        min_undumped_oplog_records: 0,
                        purge_delayed_destroy: false,
                        prune_slot_dump_manifests: false,
                        roll_forward_slot_dump_installs: false,
                        follower_replay_cursors: Vec::new(),
                        invalidate_cache: false,
                        warm_cache: false,
                    }),
            );
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .gc_runs += 1;
            DataNodeTaskOutput::Gc(GcResponse {
                status,
                shard_id: request.shard_id,
                collected_objects,
                cache_entries_removed,
                cache_disk_bytes_removed,
                oplog_records_removed,
                index_log_records_removed,
                page_segments_removed,
                page_segments_removed_physical_bytes,
                page_segments_retained_physical_bytes,
                page_segments_retained_live,
                page_segments_retained_live_physical_bytes,
                lifecycle_plan,
            })
        }
    }
}

struct TaskCancellation<'a> {
    inner: &'a DataNodeRuntimeInner,
    job_id: u64,
}

impl TaskCancellation<'_> {
    fn is_requested(&self) -> bool {
        is_cancel_requested(self.inner, self.job_id)
    }
}

fn task_timeout_output(kind: DataNodeTaskKind) -> DataNodeTaskOutput {
    let status = Status::error("deadline_exceeded", "data node task deadline exceeded");
    match kind {
        DataNodeTaskKind::Load => DataNodeTaskOutput::Load(LoadShardResponse { status }),
        DataNodeTaskKind::Reload => DataNodeTaskOutput::Reload(LoadShardResponse { status }),
        DataNodeTaskKind::Unload => DataNodeTaskOutput::Unload(UnloadShardResponse { status }),
        DataNodeTaskKind::Execute => DataNodeTaskOutput::Execute(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        }),
        DataNodeTaskKind::CheckedExecute => {
            DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            })
        }
        DataNodeTaskKind::Dump => DataNodeTaskOutput::Dump(DumpShardResponse {
            status,
            shard_id: 0,
            index_bytes: 0,
            dirty_objects_flushed: 0,
            slot_dump_manifest: None,
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id: 0,
            compacted_objects: 0,
            previous_page_segment_id: 0,
            compacted_page_segment_id: 0,
            stale_page_segment_ids: Vec::new(),
            before: ShardCompactionUtilityReport::default(),
            after: ShardCompactionUtilityReport::default(),
        }),
        DataNodeTaskKind::Gc => DataNodeTaskOutput::Gc(GcResponse {
            status,
            shard_id: 0,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            oplog_records_removed: 0,
            index_log_records_removed: 0,
            page_segments_removed: 0,
            page_segments_removed_physical_bytes: 0,
            page_segments_retained_physical_bytes: 0,
            page_segments_retained_live: 0,
            page_segments_retained_live_physical_bytes: 0,
            lifecycle_plan: None,
        }),
    }
}

fn task_canceled_output(task: &QueuedTask, message: &str) -> DataNodeTaskOutput {
    let status = Status::error("job_canceled", message);
    let shard_id = task.request.shard_id();
    match task.kind {
        DataNodeTaskKind::Load => DataNodeTaskOutput::Load(LoadShardResponse { status }),
        DataNodeTaskKind::Reload => DataNodeTaskOutput::Reload(LoadShardResponse { status }),
        DataNodeTaskKind::Unload => DataNodeTaskOutput::Unload(UnloadShardResponse { status }),
        DataNodeTaskKind::Execute => DataNodeTaskOutput::Execute(ExecuteResponse {
            status,
            response: CommandResponse::Empty,
        }),
        DataNodeTaskKind::CheckedExecute => {
            DataNodeTaskOutput::CheckedExecute(CheckedExecuteResponse {
                status: status.clone(),
                response: ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                },
            })
        }
        DataNodeTaskKind::Dump => DataNodeTaskOutput::Dump(DumpShardResponse {
            status,
            shard_id,
            index_bytes: 0,
            dirty_objects_flushed: 0,
            slot_dump_manifest: None,
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id,
            compacted_objects: 0,
            previous_page_segment_id: 0,
            compacted_page_segment_id: 0,
            stale_page_segment_ids: Vec::new(),
            before: ShardCompactionUtilityReport::default(),
            after: ShardCompactionUtilityReport::default(),
        }),
        DataNodeTaskKind::Gc => DataNodeTaskOutput::Gc(GcResponse {
            status,
            shard_id,
            collected_objects: 0,
            cache_entries_removed: 0,
            cache_disk_bytes_removed: 0,
            oplog_records_removed: 0,
            index_log_records_removed: 0,
            page_segments_removed: 0,
            page_segments_removed_physical_bytes: 0,
            page_segments_retained_physical_bytes: 0,
            page_segments_retained_live: 0,
            page_segments_retained_live_physical_bytes: 0,
            lifecycle_plan: None,
        }),
    }
}

fn is_cancel_requested(inner: &DataNodeRuntimeInner, job_id: u64) -> bool {
    inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .contains(&job_id)
}

fn take_canceled(inner: &DataNodeRuntimeInner, job_id: u64) -> bool {
    inner
        .canceled
        .lock()
        .expect("runtime cancellation lock poisoned")
        .remove(&job_id)
}

fn task_output_status(output: &DataNodeTaskOutput) -> Status {
    match output {
        DataNodeTaskOutput::Load(response) => response.status.clone(),
        DataNodeTaskOutput::Reload(response) => response.status.clone(),
        DataNodeTaskOutput::Unload(response) => response.status.clone(),
        DataNodeTaskOutput::Execute(response) => response.status.clone(),
        DataNodeTaskOutput::CheckedExecute(response) => response.status.clone(),
        DataNodeTaskOutput::Dump(response) => response.status.clone(),
        DataNodeTaskOutput::Compact(response) => response.status.clone(),
        DataNodeTaskOutput::Gc(response) => response.status.clone(),
    }
}

fn mark_dirty(dirty: &Mutex<DirtyTracker>, shard_id: ShardId, key: Option<&str>) {
    let Some(key) = key else {
        return;
    };
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    let object_key = (shard_id, key.to_string());
    let now = now_ms();
    if !dirty.by_key.contains_key(&object_key) {
        dirty.next_object_id += 1;
        let object_id = dirty.next_object_id;
        dirty.by_key.insert(
            object_key.clone(),
            DirtyObjectInfo {
                shard_id,
                key: key.to_string(),
                object_id,
                last_dirty_at_ms: now,
            },
        );
    }
    if let Some(entry) = dirty.by_key.get_mut(&object_key) {
        entry.last_dirty_at_ms = now;
    }
}

fn clear_dirty_shard(dirty: &Mutex<DirtyTracker>, shard_id: ShardId) -> usize {
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    let before = dirty.by_key.len();
    dirty
        .by_key
        .retain(|(dirty_shard_id, _), _| *dirty_shard_id != shard_id);
    before - dirty.by_key.len()
}

fn clear_dirty_shard_slots(
    dirty: &Mutex<DirtyTracker>,
    engine: &TemporalEngine,
    shard_id: ShardId,
    selected_slots: &[u32],
) -> usize {
    if selected_slots.is_empty() {
        return 0;
    }
    let selected_slots = selected_slots.iter().copied().collect::<BTreeSet<_>>();
    let mut dirty = dirty.lock().expect("dirty tracker lock poisoned");
    let before = dirty.by_key.len();
    dirty.by_key.retain(|(dirty_shard_id, key), _| {
        *dirty_shard_id != shard_id
            || !selected_slots.contains(&engine.routing_slot_for_key(shard_id, key))
    });
    before - dirty.by_key.len()
}

fn dirty_shards(dirty: &Mutex<DirtyTracker>) -> Vec<ShardId> {
    dirty
        .lock()
        .expect("dirty tracker lock poisoned")
        .by_key
        .keys()
        .map(|(shard_id, _)| *shard_id)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn mark_dirty_for_successful_commands(
    dirty: &Mutex<DirtyTracker>,
    shard_id: ShardId,
    commands: &[Command],
    responses: &[ExecuteResponse],
) {
    for (command, response) in commands.iter().zip(responses.iter()) {
        if response.status.ok && is_write_command(command) {
            mark_dirty(dirty, shard_id, command_key(command));
        }
    }
}

fn command_key(command: &Command) -> Option<&str> {
    match command {
        Command::CommonDelete { key }
        | Command::CommonExpire { key, .. }
        | Command::StringSet { key, .. }
        | Command::StringSetEx { key, .. }
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
        | Command::RiskIncrement { key, .. }
        | Command::RiskIncrementWithOptions { key, .. }
        | Command::RiskSet { key, .. }
        | Command::RiskSetAndGet { key, .. } => Some(key),
        _ => None,
    }
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
            | Command::FeatureAppendWithPolicy { .. }
            | Command::FeatureReplace { .. }
            | Command::FeatureDelete { .. }
            | Command::SequenceAdd { .. }
            | Command::IpsAdd { .. }
            | Command::IpsAddWithOptions { .. }
            | Command::RiskIncrement { .. }
            | Command::RiskIncrementWithOptions { .. }
            | Command::RiskSet { .. }
            | Command::RiskSetAndGet { .. }
    )
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
