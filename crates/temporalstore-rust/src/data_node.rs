use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{CheckedExecuteRequest, CheckedExecuteResponse};
use crate::engine::{
    ShardCompactionUtilityReport, SlotDumpManifest, StorageLifecyclePlan, StorageLifecycleReport,
    StorageLifecycleRequest, TemporalEngine,
};
use crate::meta::{ServerRuntimeLoad, ServerShardServingState};
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse, ShardId, Status};

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
    Execute,
    CheckedExecute,
    Dump,
    Compact,
    Gc,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataNodeTaskOutput {
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
    pub queued_workers: Vec<ShardWorkerInfo>,
    pub dirty_shards: Vec<ShardId>,
    pub dirty_objects: Vec<DirtyObjectInfo>,
    pub degraded_reasons: Vec<String>,
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
            TaskRequest::Execute(request) => request.shard_id,
            TaskRequest::CheckedExecute(request) => request.shard_id,
            TaskRequest::Dump(request) => request.shard_id,
            TaskRequest::Compact(request) => request.shard_id,
            TaskRequest::Gc(request) => request.shard_id,
        }
    }

    fn priority(&self) -> TaskPriority {
        match self {
            TaskRequest::Execute(_) | TaskRequest::CheckedExecute(_) => TaskPriority::Foreground,
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
                next_job_id: AtomicU64::new(1),
            }),
        }
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
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        DataNodePreflightReport {
            status,
            stats,
            queued_workers: self.queued_shard_worker_infos(),
            dirty_shards: self.dirty_shards(),
            dirty_objects: self.dirty_objects(),
            degraded_reasons,
        }
    }

    pub fn server_runtime_load(&self) -> ServerRuntimeLoad {
        let preflight = self.preflight_report();
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

        self.inner
            .engine
            .loaded_shard_stats()
            .into_iter()
            .map(|stats| {
                let serving_state = if running_shards.contains(&stats.shard_id) {
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
        TaskRequest::Execute(request) => {
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
