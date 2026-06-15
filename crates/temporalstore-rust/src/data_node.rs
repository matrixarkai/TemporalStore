use std::collections::{BTreeSet, HashMap, VecDeque};
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
    StorageLifecycleRequest, StorageProductionReadinessReport, TemporalEngine,
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
    pub metaserver: DataNodeMetaHeartbeatReport,
    pub topology_validation: DataNodeTopologyValidationReport,
    pub queued_workers: Vec<ShardWorkerInfo>,
    pub dirty_shards: Vec<ShardId>,
    pub dirty_objects: Vec<DirtyObjectInfo>,
    pub degraded_reasons: Vec<String>,
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
            next_job_id: AtomicU64::new(1),
        });
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
        Self {
            inner: Arc::new(DataNodeRuntimeInner {
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
                next_job_id: AtomicU64::new(1),
            }),
        }
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
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        DataNodePreflightReport {
            status,
            stats,
            lifecycle: self.lifecycle_report(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_reports_cpp_style_shard_worker_ownership() {
        let runtime = DataNodeRuntime::new(
            TemporalEngine::default(),
            DataNodeRuntimeOptions {
                worker_threads: 4,
                max_queue_depth: 16,
                max_background_queue_depth: 8,
            },
        );

        let stats = runtime.stats();
        assert_eq!(stats.worker_threads, 4);
        assert_eq!(stats.max_queue_depth, 16);
        assert_eq!(stats.max_background_queue_depth, 8);
        assert_eq!(
            runtime.shard_worker_info(7),
            ShardWorkerInfo {
                shard_id: 7,
                worker_index: 3,
                worker_threads: 4,
            }
        );
    }

    #[test]
    fn runtime_lifecycle_report_counts_loaded_readonly_and_preflight() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 7,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 19,
                    readonly: true,
                    load_version: 42,
                    local_node_id: Some(3),
                })
                .status
                .ok
        );
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.loaded_shard_count, 1);
        assert_eq!(lifecycle.serving_count, 0);
        assert_eq!(lifecycle.readonly_count, 1);
        assert_eq!(lifecycle.queued_count, 0);
        assert_eq!(lifecycle.running_count, 0);
        assert_eq!(lifecycle.unloading_count, 0);
        assert_eq!(lifecycle.failed_count, 0);
        assert_eq!(lifecycle.max_load_version, 42);
        assert_eq!(lifecycle.shards.len(), 1);
        assert_eq!(lifecycle.shards[0].shard_id, 7);
        assert_eq!(lifecycle.shards[0].serving_state, "readonly");
        assert!(lifecycle.shards[0].readonly);
        assert_eq!(lifecycle.shards[0].table_name, "tbl");
        assert_eq!(lifecycle.transitions.len(), 0);

        let preflight = runtime.preflight_report();
        assert_eq!(preflight.lifecycle, lifecycle);
    }

    #[test]
    fn runtime_load_reload_unload_records_lifecycle_transitions() {
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            TemporalEngine::default(),
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );

        let load = runtime.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        });
        assert!(load.status.ok, "{load:?}");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.serving_count, 1);
        assert_eq!(lifecycle.failed_count, 0);
        assert_eq!(lifecycle.transitions.len(), 1);
        assert_eq!(lifecycle.transitions[0].state, "serving");
        assert_eq!(lifecycle.transitions[0].operation, "load");
        assert_eq!(lifecycle.transitions[0].load_version, 42);

        let stale = runtime.reload_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "stale".to_string(),
            shard_uri: "local://tbl/stale".to_string(),
            start_routing_slot: 20,
            end_routing_slot: 29,
            readonly: true,
            load_version: 41,
            local_node_id: Some(3),
        });
        assert_eq!(stale.status.code, "stale_load_version");
        let failed = runtime.lifecycle_report();
        assert_eq!(failed.failed_count, 1);
        assert_eq!(failed.shards[0].serving_state, "failed");
        assert_eq!(failed.transitions[0].state, "failed");
        assert_eq!(failed.transitions[0].operation, "reload");
        assert_eq!(
            failed.transitions[0].last_status.as_ref().unwrap().code,
            "stale_load_version"
        );

        let reload = runtime.reload_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl-new".to_string(),
            shard_uri: "local://tbl/7-new".to_string(),
            start_routing_slot: 20,
            end_routing_slot: 29,
            readonly: true,
            load_version: 43,
            local_node_id: Some(4),
        });
        assert!(reload.status.ok, "{reload:?}");
        let readonly = runtime.lifecycle_report();
        assert_eq!(readonly.readonly_count, 1);
        assert_eq!(readonly.failed_count, 0);
        assert_eq!(readonly.transitions[0].state, "readonly");
        assert_eq!(readonly.transitions[0].operation, "reload");
        assert_eq!(readonly.transitions[0].load_version, 43);

        let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
        assert!(unload.status.ok, "{unload:?}");
        let unloaded = runtime.lifecycle_report();
        assert_eq!(unloaded.loaded_shard_count, 0);
        assert_eq!(unloaded.transitions[0].state, "unloaded");
        assert_eq!(unloaded.transitions[0].operation, "unload");
    }

    #[test]
    fn runtime_direct_unload_rejects_busy_shard_without_unloading() {
        let engine = TemporalEngine::default();
        engine.load_shard(7);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 8,
                max_background_queue_depth: 4,
            },
        );

        let queued = runtime.submit_dump(
            DumpShardRequest {
                shard_id: 7,
                selected_routing_slots: Vec::new(),
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(queued.status.ok, "{queued:?}");

        let unload = runtime.unload_shard_with(crate::control::UnloadShardRequest { shard_id: 7 });
        assert_eq!(unload.status.code, "shard_busy");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.loaded_shard_count, 1);
        assert_eq!(lifecycle.failed_count, 1);
        assert_eq!(lifecycle.transitions[0].state, "failed");
        assert_eq!(lifecycle.transitions[0].operation, "unload");
        assert_eq!(
            lifecycle.transitions[0].last_status.as_ref().unwrap().code,
            "shard_busy"
        );
    }

    #[test]
    fn runtime_queued_unload_waits_for_prior_shard_work() {
        let engine = TemporalEngine::default();
        engine.load_shard(7);
        let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

        let write = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 7,
                command: Command::StringSet {
                    key: "before-unload".to_string(),
                    value: b"value".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let unload = runtime.submit_unload(
            crate::control::UnloadShardRequest { shard_id: 7 },
            RequestController { timeout_ms: 1000 },
        );
        assert!(write.status.ok, "{write:?}");
        assert!(unload.status.ok, "{unload:?}");

        let first = runtime
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned")
            .pop_ready()
            .expect("first shard task should be ready");
        assert_eq!(first.job_id, write.job_id);
        let output = execute_task(&runtime.inner, &first);
        let DataNodeTaskOutput::Execute(response) = output else {
            panic!("expected execute output");
        };
        assert!(response.status.ok, "{response:?}");
        runtime
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned")
            .finish_shard(7);

        let second = runtime
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned")
            .pop_ready()
            .expect("queued unload should be ready after prior work");
        assert_eq!(second.job_id, unload.job_id);
        let output = execute_task(&runtime.inner, &second);
        let DataNodeTaskOutput::Unload(response) = output else {
            panic!("expected unload output");
        };
        assert!(response.status.ok, "{response:?}");
        assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
    }

    #[test]
    fn runtime_enforces_authorized_lifecycle_token_when_installed() {
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            TemporalEngine::default(),
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        runtime.require_lifecycle_token(SchedulerLifecycleToken {
            task_id: 12,
            shard_id: 7,
            operation: "load".to_string(),
            load_version: 43,
            generation: 900,
        });

        let stale = runtime.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/stale".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        });
        assert_eq!(stale.status.code, "lifecycle_token_mismatch");
        let failed = runtime.lifecycle_report();
        assert_eq!(failed.failed_count, 1);
        assert_eq!(failed.transitions[0].scheduler_task_id, Some(12));
        assert_eq!(failed.transitions[0].scheduler_generation, Some(900));

        let load = runtime.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 19,
            readonly: false,
            load_version: 43,
            local_node_id: Some(3),
        });
        assert!(load.status.ok, "{load:?}");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.failed_count, 0);
        assert_eq!(lifecycle.transitions[0].state, "serving");
        assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(12));
        assert_eq!(lifecycle.transitions[0].scheduler_generation, Some(900));
    }

    #[test]
    fn runtime_async_lifecycle_jobs_report_progress_and_outputs() {
        let runtime = DataNodeRuntime::new(
            TemporalEngine::default(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 4,
            },
        );

        let submitted = runtime.submit_load(
            crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 19,
                readonly: false,
                load_version: 42,
                local_node_id: Some(3),
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(submitted.status.ok, "{submitted:?}");
        assert_eq!(submitted.kind, DataNodeTaskKind::Load);
        assert!(submitted.finished_at_ms.is_none());

        let finished = wait_for_job(&runtime, submitted.job_id);
        assert!(finished.status.ok, "{finished:?}");
        let Some(DataNodeTaskOutput::Load(output)) = finished.output else {
            panic!("expected load output");
        };
        assert!(output.status.ok, "{output:?}");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.serving_count, 1);
        assert_eq!(lifecycle.transitions[0].operation, "load");
        assert_eq!(lifecycle.transitions[0].state, "serving");

        let reloaded = runtime.submit_reload(
            crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl-new".to_string(),
                shard_uri: "local://tbl/7-new".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 19,
                readonly: true,
                load_version: 43,
                local_node_id: Some(3),
            },
            RequestController { timeout_ms: 1000 },
        );
        let reloaded = wait_for_job(&runtime, reloaded.job_id);
        let Some(DataNodeTaskOutput::Reload(output)) = reloaded.output else {
            panic!("expected reload output");
        };
        assert!(output.status.ok, "{output:?}");
        assert_eq!(runtime.lifecycle_report().readonly_count, 1);

        let unloaded = runtime.submit_unload(
            crate::control::UnloadShardRequest { shard_id: 7 },
            RequestController { timeout_ms: 1000 },
        );
        let unloaded = wait_for_job(&runtime, unloaded.job_id);
        let Some(DataNodeTaskOutput::Unload(output)) = unloaded.output else {
            panic!("expected unload output");
        };
        assert!(output.status.ok, "{output:?}");
        assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
    }

    #[test]
    fn runtime_rejects_foreground_writes_during_lifecycle_transition() {
        let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
        let load = runtime.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        });
        assert!(load.status.ok, "{load:?}");
        let seed = runtime.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(seed.status.ok, "{seed:?}");

        record_lifecycle_state_inner(&runtime.inner, 7, "reloading", "reload", 43, None);

        let write = runtime.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringSet {
                key: "k".to_string(),
                value: b"blocked".to_vec(),
            },
        });
        assert_eq!(write.status.code, "lifecycle_write_blocked");
        let checked = runtime.execute_checked(CheckedExecuteRequest {
            shard_id: 7,
            load_version: 42,
            command: Command::StringDelete {
                key: "k".to_string(),
            },
        });
        assert_eq!(checked.status.code, "lifecycle_write_blocked");
        let batch = runtime.batch_execute(BatchExecuteRequest {
            shard_id: 7,
            commands: vec![Command::HashSet {
                key: "h".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            }],
        });
        assert_eq!(batch.status.code, "lifecycle_write_blocked");

        let read = runtime.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "k".to_string(),
            },
        });
        assert!(read.status.ok, "{read:?}");
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
    }

    #[test]
    fn runtime_rejects_queued_foreground_write_during_lifecycle_transition() {
        let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);
        let load = runtime.load_shard_with(crate::control::LoadShardRequest {
            shard_id: 7,
            table_name: "tbl".to_string(),
            shard_uri: "local://tbl/7".to_string(),
            start_routing_slot: 10,
            end_routing_slot: 19,
            readonly: false,
            load_version: 42,
            local_node_id: Some(3),
        });
        assert!(load.status.ok, "{load:?}");
        record_lifecycle_state_inner(&runtime.inner, 7, "unloading", "unload", 42, None);
        let submitted = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 7,
                command: Command::StringSet {
                    key: "queued".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let task = runtime
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned")
            .pop_ready()
            .expect("queued write should be ready");
        assert_eq!(task.job_id, submitted.job_id);

        let output = execute_task(&runtime.inner, &task);
        let DataNodeTaskOutput::Execute(response) = output else {
            panic!("expected execute output");
        };
        assert_eq!(response.status.code, "lifecycle_write_blocked");
    }

    #[test]
    fn runtime_executes_async_tracks_dirty_and_dump_clears_it() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let finished = wait_for_job(&runtime, job.job_id);
        assert!(finished.status.ok);
        assert_eq!(runtime.dirty_objects().len(), 1);

        let dump = runtime.submit_dump(
            DumpShardRequest {
                shard_id: 1,
                selected_routing_slots: Vec::new(),
            },
            RequestController { timeout_ms: 1000 },
        );
        let finished = wait_for_job(&runtime, dump.job_id);
        let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
            panic!("expected dump output");
        };
        assert_eq!(output.dirty_objects_flushed, 1);
        assert!(runtime.dirty_objects().is_empty());
    }

    #[test]
    fn runtime_dump_can_flush_only_selected_dirty_slots() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let mut key_a = String::new();
        let mut key_b = String::new();
        let mut slot_a = 0;
        for index in 0..128 {
            let key = format!("slot-key-{index}");
            let slot = engine.routing_slot_for_key(1, &key);
            if key_a.is_empty() {
                key_a = key;
                slot_a = slot;
            } else if slot != slot_a {
                key_b = key;
                break;
            }
        }
        assert!(!key_b.is_empty(), "test needs two distinct routing slots");
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        for key in [&key_a, &key_b] {
            let job = runtime.submit_execute(
                ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                },
                RequestController { timeout_ms: 1000 },
            );
            assert!(wait_for_job(&runtime, job.job_id).status.ok);
        }
        assert_eq!(runtime.dirty_objects().len(), 2);

        let dump = runtime.submit_dump(
            DumpShardRequest {
                shard_id: 1,
                selected_routing_slots: vec![slot_a],
            },
            RequestController { timeout_ms: 1000 },
        );
        let finished = wait_for_job(&runtime, dump.job_id);
        let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
            panic!("expected dump output");
        };
        assert!(output.status.ok);
        assert_eq!(output.dirty_objects_flushed, 1);
        assert_eq!(
            output.slot_dump_manifest.as_ref().unwrap().slot_ids,
            vec![slot_a]
        );
        let remaining = runtime.dirty_objects();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].key, key_b);
    }

    #[test]
    fn runtime_schedules_dumps_for_dirty_shards() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.load_shard(2);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 2,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );

        for (shard_id, key) in [(1, "alpha"), (2, "beta")] {
            let job = runtime.submit_execute(
                ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: key.as_bytes().to_vec(),
                    },
                },
                RequestController { timeout_ms: 1000 },
            );
            assert!(wait_for_job(&runtime, job.job_id).status.ok);
        }

        assert_eq!(runtime.dirty_shards(), vec![1, 2]);
        let dumps = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
        assert_eq!(dumps.len(), 2);
        for dump in dumps {
            let finished = wait_for_job(&runtime, dump.job_id);
            let Some(DataNodeTaskOutput::Dump(output)) = finished.output else {
                panic!("expected dump output");
            };
            assert!(output.status.ok);
            assert_eq!(output.dirty_objects_flushed, 1);
        }
        assert!(runtime.dirty_objects().is_empty());
        assert!(runtime.dirty_shards().is_empty());
    }

    #[test]
    fn runtime_preflight_reports_dirty_backlog_and_queue_degradation() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 1,
                max_background_queue_depth: 1,
            },
        );
        mark_dirty(&runtime.inner.dirty, 1, Some("dirty-key"));
        let first = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "queued".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(first.status.ok);
        let rejected = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "queued".to_string(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert_eq!(rejected.status.code, "queue_full");

        let preflight = runtime.preflight_report();

        assert!(!preflight.status.ok);
        assert!(preflight
            .degraded_reasons
            .contains(&"foreground_queue_full".to_string()));
        assert!(preflight
            .degraded_reasons
            .contains(&"rejected_requests".to_string()));
        assert_eq!(preflight.stats.queue_depth, 1);
        assert_eq!(preflight.queued_workers.len(), 1);
        assert_eq!(preflight.dirty_shards, vec![1]);
        assert_eq!(preflight.dirty_objects.len(), 1);
    }

    #[test]
    fn runtime_builds_cpp_style_server_load_report() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(crate::control::LoadShardRequest {
                    shard_id: 7,
                    table_name: "tbl".to_string(),
                    shard_uri: "local://tbl/7".to_string(),
                    start_routing_slot: 10,
                    end_routing_slot: 19,
                    readonly: false,
                    load_version: 42,
                    local_node_id: Some(3),
                })
                .status
                .ok
        );
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 4,
                max_queue_depth: 2,
                max_background_queue_depth: 1,
            },
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                })
                .status
                .ok
        );
        mark_dirty(&runtime.inner.dirty, 7, Some("k"));
        let queued = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 7,
                command: Command::StringGet {
                    key: "k".to_string(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(queued.status.ok);

        let load = runtime.server_runtime_load();
        assert_eq!(load.queue_depth, 1);
        assert_eq!(load.queued_shard_count, 1);
        assert_eq!(load.dirty_object_count, 1);
        assert_eq!(load.last_meta_topology_version, 0);
        runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
            status: Status::error("resource_frozen", "server frozen"),
            forbid_auto_register: true,
            topology_version: 99,
            server_state: "frozen".to_string(),
        });
        let preflight = runtime.preflight_report();
        assert_eq!(preflight.metaserver.last_topology_version, 99);
        assert_eq!(preflight.metaserver.last_server_state, "frozen");
        assert_eq!(preflight.metaserver.consecutive_failures, 1);
        assert!(preflight
            .degraded_reasons
            .contains(&"metaserver_heartbeat_failed".to_string()));
        assert!(preflight
            .degraded_reasons
            .contains(&"metaserver_forbid_auto_register".to_string()));
        let load = runtime.server_runtime_load();
        assert_eq!(load.last_meta_topology_version, 99);
        assert_eq!(load.meta_heartbeat_consecutive_failures, 1);
        assert!(load.meta_forbid_auto_register);
        runtime.record_metaserver_heartbeat(&ServerHeartbeatResponse {
            status: Status::ok(),
            forbid_auto_register: false,
            topology_version: 100,
            server_state: "normal".to_string(),
        });
        let recovered = runtime.preflight_report();
        assert_eq!(recovered.metaserver.consecutive_failures, 0);
        assert_eq!(recovered.metaserver.last_topology_version, 100);
        assert_eq!(
            recovered.topology_validation.last_meta_topology_version,
            100
        );
        assert_eq!(recovered.topology_validation.loaded_shards, vec![7]);
        assert!(recovered.topology_validation.validation_limited);
        let topology = TableTopologyResponse {
            status: Status::ok(),
            table: Some(crate::meta::TableMetaInfo {
                table_id: 1,
                namespace: "ns".to_string(),
                table_name: "tbl".to_string(),
                state: crate::meta::MetaEntityState::Normal,
                topology_version: 100,
                first_shard_id: 7,
                shard_count: 1,
                replica_count: 1,
                use_cpp_partition_ids: false,
                partition_version: 0,
                serving_options: crate::meta::TableServingOptions::default(),
            }),
            partitions: vec![crate::meta::TablePartition {
                shard_id: 7,
                start_slot: 10,
                end_slot: 19,
                primary: Some("server-a".to_string()),
                replicas: vec!["server-a".to_string()],
                primary_endpoint: None,
                replica_endpoints: Vec::new(),
            }],
            unchanged: false,
        };
        let validated = runtime.validate_topology_against_metaserver("server-a", &[topology]);
        assert!(validated.validated_against_metaserver);
        assert!(!validated.validation_limited);
        assert_eq!(validated.authoritative_topology_version, 100);
        assert_eq!(validated.mismatch_count, 0);
        let mismatch_topology = TableTopologyResponse {
            status: Status::ok(),
            table: None,
            partitions: vec![crate::meta::TablePartition {
                shard_id: 7,
                start_slot: 0,
                end_slot: 9,
                primary: Some("server-b".to_string()),
                replicas: vec!["server-b".to_string()],
                primary_endpoint: None,
                replica_endpoints: Vec::new(),
            }],
            unchanged: false,
        };
        let mismatched =
            runtime.validate_topology_against_metaserver("server-a", &[mismatch_topology]);
        assert!(mismatched.mismatch_count >= 2);
        let shard_states = runtime.shard_serving_states();
        assert_eq!(shard_states.len(), 1);
        let shard = &shard_states[0];
        assert_eq!(shard.shard_id, 7);
        assert_eq!(shard.serving_state, "queued");
        assert_eq!(shard.worker_index, 0);
        assert_eq!(shard.worker_threads, 1);
        assert!(!shard.readonly);
        assert_eq!(shard.load_version, 42);
        assert_eq!(shard.table_name, "tbl");
        assert_eq!(shard.dirty_object_count, 1);
        assert_eq!(shard.oplog_sequence, 1);
    }

    #[test]
    fn runtime_dirty_dump_scheduler_periodically_flushes_dirty_shards() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        engine.load_shard(2);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 2,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let scheduler = runtime.start_dirty_dump_scheduler(
            Duration::from_millis(5),
            RequestController { timeout_ms: 1000 },
        );

        for (shard_id, key) in [(1, "periodic-a"), (2, "periodic-b")] {
            let job = runtime.submit_execute(
                ExecuteRequest {
                    shard_id,
                    command: Command::StringSet {
                        key: key.to_string(),
                        value: b"v".to_vec(),
                    },
                },
                RequestController { timeout_ms: 1000 },
            );
            assert!(wait_for_job(&runtime, job.job_id).status.ok);
        }

        wait_until(Duration::from_secs(1), || {
            runtime.dirty_objects().is_empty() && runtime.stats().dump_runs >= 2
        });
        scheduler.stop();
        assert!(runtime.dirty_objects().is_empty());
        assert_eq!(runtime.dirty_shards(), Vec::<ShardId>::new());
    }

    #[test]
    fn runtime_dirty_dump_scheduler_skips_already_queued_dump_for_shard() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        mark_dirty(&runtime.inner.dirty, 1, Some("queued"));
        let first = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });
        let second = runtime.schedule_dirty_shard_dumps(RequestController { timeout_ms: 1000 });

        assert_eq!(first.len(), 1);
        assert!(first[0].status.ok);
        assert!(second.is_empty());
        assert_eq!(runtime.stats().background_queue_depth, 1);
    }

    #[test]
    fn runtime_storage_lifecycle_scheduler_runs_periodically() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let job = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "lifecycle-scheduler".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(wait_for_job(&runtime, job.job_id).status.ok);
        let scheduler = runtime.start_storage_lifecycle_scheduler(
            Duration::from_millis(5),
            StorageLifecycleRequest {
                shard_id: 1,
                selected_dump_slots: Vec::new(),
                max_dump_slots_per_round: 0,
                min_undumped_oplog_records: 0,
                purge_delayed_destroy: false,
                prune_slot_dump_manifests: false,
                roll_forward_slot_dump_installs: false,
                follower_replay_cursors: Vec::new(),
                invalidate_cache: false,
                warm_cache: true,
            },
        );
        wait_until(Duration::from_secs(1), || {
            runtime.stats().storage_lifecycle_runs >= 1
        });
        scheduler.stop();
        assert!(runtime.stats().storage_lifecycle_runs >= 1);
    }

    #[test]
    fn runtime_expiry_sweep_scheduler_removes_expired_records() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        assert!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringSetEx {
                        key: "ttl".to_string(),
                        value: b"gone".to_vec(),
                        ttl_ms: 1,
                    },
                })
                .status
                .ok
        );
        let scheduler = runtime.start_expiry_sweep_scheduler(Duration::from_millis(5));
        wait_until(Duration::from_secs(1), || {
            runtime.stats().expired_records_removed >= 1
        });
        scheduler.stop();
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "ttl".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes { value: None }
        );
        assert!(runtime.stats().expiry_sweeps >= 1);
    }

    #[test]
    fn runtime_rejects_when_queue_is_full() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 1,
                max_background_queue_depth: 1,
            },
        );
        let _first = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "a".to_string(),
                    value: b"1".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let rejected = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "b".to_string(),
                    value: b"2".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        if !rejected.status.ok {
            assert_eq!(rejected.status.code, "queue_full");
        }
    }

    #[test]
    fn runtime_cancel_reports_not_found_and_already_finished_jobs() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(engine, DataNodeRuntimeOptions::default());

        assert_eq!(runtime.cancel_job(42).status.code, "job_not_found");

        let submitted = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "k".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let finished = wait_for_job(&runtime, submitted.job_id);
        assert!(finished.status.ok);
        assert_eq!(
            runtime.cancel_job(submitted.job_id).status.code,
            "job_already_finished"
        );
    }

    #[test]
    fn runtime_compaction_rewrites_live_pages_and_reports_stale_segments() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for (key, value) in [
            ("a", b"old".to_vec()),
            ("a", b"one".to_vec()),
            ("b", b"two".to_vec()),
        ] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value,
                },
            });
            assert!(response.status.ok);
        }
        assert_eq!(engine.live_page_segment_ids(1), vec![0]);

        let runtime = DataNodeRuntime::new(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let submitted = runtime.submit_compaction(
            CompactionRequest { shard_id: 1 },
            RequestController { timeout_ms: 1000 },
        );
        assert!(submitted.status.ok, "{submitted:?}");
        let finished = wait_for_job(&runtime, submitted.job_id);
        let Some(DataNodeTaskOutput::Compact(output)) = finished.output else {
            panic!("expected compaction output");
        };
        assert!(output.status.ok);
        assert_eq!(output.compacted_objects, 2);
        assert_eq!(output.previous_page_segment_id, 0);
        assert_eq!(output.compacted_page_segment_id, 1);
        assert_eq!(output.stale_page_segment_ids, vec![0]);
        assert_eq!(output.before.total_page_count, 3);
        assert_eq!(output.before.live_page_refs, 2);
        assert_eq!(output.before.stale_page_estimate, 1);
        assert_eq!(output.before.live_ref_density_basis_points, 6_666);
        assert_eq!(output.after.total_page_count, 2);
        assert_eq!(output.after.live_page_refs, 2);
        assert_eq!(output.after.stale_page_estimate, 0);
        assert_eq!(output.after.live_ref_density_basis_points, 10_000);
        assert_eq!(engine.live_page_segment_ids(1), vec![1]);
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "a".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"one".to_vec())
            }
        );
    }

    #[test]
    fn runtime_gc_reclaims_log_tails_and_reports_counts() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for key in ["a", "b", "c"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            });
            assert!(response.status.ok);
        }
        assert_eq!(
            engine
                .execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::StringGet {
                        key: "a".to_string()
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(b"a".to_vec())
            }
        );
        engine.page_store().install_segment(1, b"old").unwrap();
        engine.page_store().install_segment(2, b"new").unwrap();

        let runtime = DataNodeRuntime::new(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let submitted = runtime.submit_gc(
            GcRequest {
                shard_id: 1,
                retain_oplog_from_sequence: Some(3),
                retain_index_log_from_sequence: Some(2),
                retain_page_segments_from_id: Some(2),
            },
            RequestController { timeout_ms: 1000 },
        );
        let finished = wait_for_job(&runtime, submitted.job_id);
        let Some(DataNodeTaskOutput::Gc(output)) = finished.output else {
            panic!("expected gc output");
        };
        assert!(output.status.ok);
        assert_eq!(output.cache_entries_removed, 2);
        assert!(output.cache_disk_bytes_removed > 0);
        assert_eq!(output.oplog_records_removed, 2);
        assert_eq!(output.index_log_records_removed, 1);
        assert_eq!(output.page_segments_removed, 1);
        assert!(output.page_segments_removed_physical_bytes > 0);
        assert!(output.page_segments_retained_physical_bytes > 0);
        assert_eq!(output.page_segments_retained_live, 1);
        assert!(output.page_segments_retained_live_physical_bytes > 0);
        assert_eq!(engine.oplog_store().stats(1).last_sequence, 3);
        assert_eq!(engine.index_log_store().stats(1).last_sequence, 3);
        assert_eq!(engine.page_store().segment_ids().unwrap(), vec![0, 2]);
    }

    #[test]
    fn runtime_cancels_queued_job_before_worker_executes_it() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

        let submitted = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "queued".to_string(),
                    value: b"v".to_vec(),
                },
            },
            RequestController { timeout_ms: 1000 },
        );
        let canceled = runtime.cancel_job(submitted.job_id);
        assert_eq!(canceled.status.code, "job_canceled");
        assert_eq!(runtime.stats().canceled_total, 1);
        assert_eq!(runtime.stats().queue_depth, 0);
    }

    #[test]
    fn runtime_cancels_queued_lifecycle_job_before_execution() {
        let runtime = DataNodeRuntime::new_without_workers_for_test(TemporalEngine::default(), 8);

        let submitted = runtime.submit_load(
            crate::control::LoadShardRequest {
                shard_id: 7,
                table_name: "tbl".to_string(),
                shard_uri: "local://tbl/7".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 19,
                readonly: false,
                load_version: 42,
                local_node_id: Some(3),
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(submitted.status.ok, "{submitted:?}");

        let canceled = runtime.cancel_job(submitted.job_id);
        assert_eq!(canceled.status.code, "job_canceled");
        assert_eq!(canceled.kind, DataNodeTaskKind::Load);
        assert_eq!(runtime.stats().canceled_total, 1);
        assert_eq!(runtime.stats().queue_depth, 0);
        assert_eq!(runtime.lifecycle_report().loaded_shard_count, 0);
    }

    #[test]
    fn runtime_marks_inflight_cancellation_requested_before_worker_finishes() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);

        let submitted = runtime.submit_dump(
            DumpShardRequest {
                shard_id: 1,
                selected_routing_slots: Vec::new(),
            },
            RequestController { timeout_ms: 1000 },
        );
        let task = runtime
            .inner
            .queue
            .lock()
            .expect("runtime queue lock poisoned")
            .pop_ready()
            .expect("task should be marked running");
        assert_eq!(task.job_id, submitted.job_id);

        let cancel_requested = runtime.cancel_job(submitted.job_id);
        assert_eq!(cancel_requested.status.code, "job_cancel_requested");
        assert_eq!(
            runtime.job_status(submitted.job_id).unwrap().status.code,
            "job_cancel_requested"
        );
        assert_eq!(runtime.stats().canceled_total, 0);

        let output = execute_task(&runtime.inner, &task);
        let DataNodeTaskOutput::Dump(response) = output else {
            panic!("expected dump output");
        };
        assert_eq!(response.status.code, "job_canceled");
        assert!(take_canceled(&runtime.inner, submitted.job_id));
    }

    #[test]
    fn runtime_honors_inflight_cancellation_before_dump_side_effects() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::StringSet {
                key: "dirty".to_string(),
                value: b"v".to_vec(),
            },
        });
        assert!(response.status.ok);
        let runtime = DataNodeRuntime::new_without_workers_for_test(engine, 8);
        mark_dirty(&runtime.inner.dirty, 1, Some("dirty"));
        let task = QueuedTask {
            job_id: 99,
            kind: DataNodeTaskKind::Dump,
            deadline: Instant::now() + Duration::from_secs(60),
            submitted_at_ms: now_ms(),
            request: TaskRequest::Dump(DumpShardRequest {
                shard_id: 1,
                selected_routing_slots: Vec::new(),
            }),
        };
        runtime
            .inner
            .canceled
            .lock()
            .expect("runtime cancellation lock poisoned")
            .insert(task.job_id);

        let output = execute_task(&runtime.inner, &task);
        let DataNodeTaskOutput::Dump(response) = output else {
            panic!("expected dump output");
        };
        assert_eq!(response.status.code, "job_canceled");
        assert_eq!(response.shard_id, 1);
        assert_eq!(runtime.dirty_objects().len(), 1);
    }

    #[test]
    fn runtime_honors_inflight_cancellation_before_gc_side_effects() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        for key in ["a", "b"] {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: key.as_bytes().to_vec(),
                },
            });
            assert!(response.status.ok);
        }
        let runtime = DataNodeRuntime::new_without_workers_for_test(engine.clone(), 8);
        mark_dirty(&runtime.inner.dirty, 1, Some("a"));
        let task = QueuedTask {
            job_id: 100,
            kind: DataNodeTaskKind::Gc,
            deadline: Instant::now() + Duration::from_secs(60),
            submitted_at_ms: now_ms(),
            request: TaskRequest::Gc(GcRequest {
                shard_id: 1,
                retain_oplog_from_sequence: Some(2),
                retain_index_log_from_sequence: Some(2),
                retain_page_segments_from_id: None,
            }),
        };
        runtime
            .inner
            .canceled
            .lock()
            .expect("runtime cancellation lock poisoned")
            .insert(task.job_id);

        let output = execute_task(&runtime.inner, &task);
        let DataNodeTaskOutput::Gc(response) = output else {
            panic!("expected gc output");
        };
        assert_eq!(response.status.code, "job_canceled");
        assert_eq!(response.shard_id, 1);
        assert_eq!(runtime.dirty_objects().len(), 1);
        assert_eq!(engine.oplog_store().stats(1).last_sequence, 2);
        assert_eq!(engine.index_log_store().stats(1).last_sequence, 2);
        assert_eq!(
            engine
                .oplog_store()
                .scan(1, 0, u64::MAX, u64::MAX)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            engine
                .index_log_store()
                .scan(1, 0, u64::MAX, u64::MAX)
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn runtime_queues_are_shard_affine_and_parallel_across_shards() {
        let mut queues = RuntimeQueues::default();
        queues.push(queued_string_set(1, 1, "a"));
        queues.push(queued_string_set(2, 1, "b"));
        queues.push(queued_string_set(3, 2, "c"));

        let first = queues.pop_ready().expect("shard 1 should be ready");
        assert_eq!(first.job_id, 1);
        assert_eq!(queues.queued_total, 2);
        assert!(queues.running_shards.contains(&1));

        let second = queues
            .pop_ready()
            .expect("shard 2 should run while shard 1 is busy");
        assert_eq!(second.job_id, 3);
        assert_eq!(second.request.shard_id(), 2);
        assert!(queues.pop_ready().is_none());

        queues.finish_shard(1);
        let third = queues
            .pop_ready()
            .expect("next shard 1 task should run after lane release");
        assert_eq!(third.job_id, 2);
        queues.finish_shard(2);
        queues.finish_shard(1);
        assert_eq!(queues.queued_total, 0);
        assert!(queues.by_shard.is_empty());
        assert!(queues.running_shards.is_empty());
    }

    #[test]
    fn runtime_scheduler_prioritizes_foreground_over_background() {
        let mut queues = RuntimeQueues::default();
        queues.push(queued_dump(1, 1));
        queues.push(queued_string_set(2, 1, "foreground"));
        queues.push(queued_dump(3, 2));

        let first = queues.pop_ready().expect("foreground work should be ready");
        assert_eq!(first.job_id, 2);
        assert_eq!(first.request.priority(), TaskPriority::Foreground);
        queues.finish_shard(1);

        let second = queues.pop_ready().expect("background shard should run");
        assert_eq!(second.request.priority(), TaskPriority::Background);
        assert_eq!(queues.background_queued_total, 1);
    }

    #[test]
    fn runtime_rejects_background_work_when_background_queue_is_full() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new_without_workers_with_options(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 0,
                max_queue_depth: 8,
                max_background_queue_depth: 1,
            },
        );

        let accepted = runtime.submit_dump(
            DumpShardRequest {
                shard_id: 1,
                selected_routing_slots: Vec::new(),
            },
            RequestController { timeout_ms: 1000 },
        );
        assert!(accepted.status.ok);
        let rejected = runtime.submit_gc(
            GcRequest {
                shard_id: 1,
                retain_oplog_from_sequence: None,
                retain_index_log_from_sequence: None,
                retain_page_segments_from_id: None,
            },
            RequestController { timeout_ms: 1000 },
        );
        assert_eq!(rejected.status.code, "background_queue_full");
        assert_eq!(runtime.stats().rejected_background_total, 1);
        assert_eq!(runtime.stats().background_queue_depth, 1);
    }

    fn queued_string_set(job_id: u64, shard_id: ShardId, key: &str) -> QueuedTask {
        QueuedTask {
            job_id,
            kind: DataNodeTaskKind::Execute,
            deadline: Instant::now() + Duration::from_secs(60),
            submitted_at_ms: now_ms(),
            request: TaskRequest::Execute(ExecuteRequest {
                shard_id,
                command: Command::StringSet {
                    key: key.to_string(),
                    value: b"v".to_vec(),
                },
            }),
        }
    }

    fn queued_dump(job_id: u64, shard_id: ShardId) -> QueuedTask {
        QueuedTask {
            job_id,
            kind: DataNodeTaskKind::Dump,
            deadline: Instant::now() + Duration::from_secs(60),
            submitted_at_ms: now_ms(),
            request: TaskRequest::Dump(DumpShardRequest {
                shard_id,
                selected_routing_slots: Vec::new(),
            }),
        }
    }

    fn wait_for_job(runtime: &DataNodeRuntime, job_id: u64) -> DataNodeTaskStatus {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(status) = runtime.job_status(job_id) {
                if status.finished_at_ms.is_some() {
                    return status;
                }
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("job {job_id} did not finish");
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            predicate(),
            "condition did not become true within {timeout:?}"
        );
    }
}
