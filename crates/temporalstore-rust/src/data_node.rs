use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::control::{CheckedExecuteRequest, CheckedExecuteResponse};
use crate::engine::TemporalEngine;
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse, ShardId, Status};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataNodeRuntimeOptions {
    pub worker_threads: usize,
    pub max_queue_depth: usize,
}

impl Default for DataNodeRuntimeOptions {
    fn default() -> Self {
        Self {
            worker_threads: 4,
            max_queue_depth: 1024,
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
    pub submitted_total: u64,
    pub completed_total: u64,
    pub rejected_total: u64,
    pub timed_out_total: u64,
    pub canceled_total: u64,
    pub queue_depth: usize,
    pub queued_shard_count: usize,
    pub running_shard_count: usize,
    pub dirty_object_count: usize,
    pub dirty_shard_count: usize,
    pub dump_runs: u64,
    pub compaction_runs: u64,
    pub gc_runs: u64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DumpShardResponse {
    pub status: Status,
    pub shard_id: ShardId,
    pub index_bytes: usize,
    pub dirty_objects_flushed: usize,
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
}

#[derive(Debug, Clone)]
pub struct DataNodeRuntime {
    inner: Arc<DataNodeRuntimeInner>,
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
    timed_out_total: u64,
    canceled_total: u64,
    dump_runs: u64,
    compaction_runs: u64,
    gc_runs: u64,
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
    by_shard: HashMap<ShardId, VecDeque<QueuedTask>>,
    ready_shards: VecDeque<ShardId>,
    running_shards: BTreeSet<ShardId>,
    queued_total: usize,
}

#[derive(Debug)]
enum TaskRequest {
    Execute(ExecuteRequest),
    CheckedExecute(CheckedExecuteRequest),
    Dump(DumpShardRequest),
    Compact(CompactionRequest),
    Gc(GcRequest),
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
}

impl RuntimeQueues {
    fn push(&mut self, task: QueuedTask) {
        let shard_id = task.request.shard_id();
        let queue = self.by_shard.entry(shard_id).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(task);
        self.queued_total += 1;
        if was_empty && !self.running_shards.contains(&shard_id) {
            self.ready_shards.push_back(shard_id);
        }
    }

    fn pop_ready(&mut self) -> Option<QueuedTask> {
        while let Some(shard_id) = self.ready_shards.pop_front() {
            if self.running_shards.contains(&shard_id) {
                continue;
            }
            let Some(queue) = self.by_shard.get_mut(&shard_id) else {
                continue;
            };
            let Some(task) = queue.pop_front() else {
                continue;
            };
            self.queued_total = self.queued_total.saturating_sub(1);
            self.running_shards.insert(shard_id);
            return Some(task);
        }
        None
    }

    fn finish_shard(&mut self, shard_id: ShardId) {
        self.running_shards.remove(&shard_id);
        let has_more = self
            .by_shard
            .get(&shard_id)
            .map(|queue| !queue.is_empty())
            .unwrap_or(false);
        if has_more {
            self.ready_shards.push_back(shard_id);
        } else {
            self.by_shard.remove(&shard_id);
        }
    }

    fn remove_job(&mut self, job_id: u64) -> bool {
        for (shard_id, queue) in self.by_shard.iter_mut() {
            let before = queue.len();
            queue.retain(|task| task.job_id != job_id);
            if queue.len() != before {
                self.queued_total = self.queued_total.saturating_sub(before - queue.len());
                if queue.is_empty() && !self.running_shards.contains(shard_id) {
                    self.ready_shards
                        .retain(|ready_shard_id| ready_shard_id != shard_id);
                }
                return true;
            }
        }
        false
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
        Self {
            inner: Arc::new(DataNodeRuntimeInner {
                engine,
                options: DataNodeRuntimeOptions {
                    worker_threads: 0,
                    max_queue_depth: max_queue_depth.max(1),
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
            return DataNodeTaskStatus {
                status: Status::error(
                    "job_cancel_requested",
                    "data node job cancellation requested",
                ),
                ..existing
            };
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
            submitted_total: stats.submitted_total,
            completed_total: stats.completed_total,
            rejected_total: stats.rejected_total,
            timed_out_total: stats.timed_out_total,
            canceled_total: stats.canceled_total,
            queue_depth,
            queued_shard_count,
            running_shard_count,
            dirty_object_count: dirty.by_key.len(),
            dirty_shard_count,
            dump_runs: stats.dump_runs,
            compaction_runs: stats.compaction_runs,
            gc_runs: stats.gc_runs,
        }
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
            task_canceled_output(task.kind)
        } else {
            execute_task(&inner, &task)
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
            let index_bytes = inner
                .engine
                .export_index_bytes(request.shard_id)
                .map(|bytes| bytes.len())
                .unwrap_or_default();
            let dirty_objects_flushed = clear_dirty_shard(&inner.dirty, request.shard_id);
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
            })
        }
        TaskRequest::Compact(request) => {
            let compacted_objects = dirty_count(&inner.dirty, request.shard_id);
            inner
                .stats
                .lock()
                .expect("runtime stats lock poisoned")
                .compaction_runs += 1;
            DataNodeTaskOutput::Compact(CompactionResponse {
                status: Status::ok(),
                shard_id: request.shard_id,
                compacted_objects,
            })
        }
        TaskRequest::Gc(request) => {
            let collected_objects = clear_dirty_shard(&inner.dirty, request.shard_id);
            let mut status = Status::ok();
            let mut cache_entries_removed = 0;
            let mut cache_disk_bytes_removed = 0;
            let mut oplog_records_removed = 0;
            let mut index_log_records_removed = 0;
            let mut page_segments_removed = 0;
            match inner.engine.cache().invalidate_shard(request.shard_id) {
                Ok(report) => {
                    cache_entries_removed = report.memory_entries_removed;
                    cache_disk_bytes_removed = report.disk_bytes_removed;
                }
                Err(err) => {
                    status = Status::error("cache_gc_failed", &err.to_string());
                }
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
            if status.ok {
                if let Some(retain_from_page_segment_id) = request.retain_page_segments_from_id {
                    match inner
                        .engine
                        .page_store()
                        .gc_segments_before(retain_from_page_segment_id)
                    {
                        Ok(report) => {
                            page_segments_removed = report.removed_page_segment_ids.len();
                        }
                        Err(err) => {
                            status = Status::error("page_store_gc_failed", &err.to_string());
                        }
                    }
                }
            }
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
            })
        }
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
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id: 0,
            compacted_objects: 0,
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
        }),
    }
}

fn task_canceled_output(kind: DataNodeTaskKind) -> DataNodeTaskOutput {
    let status = Status::error("job_canceled", "data node task canceled before execution");
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
        }),
        DataNodeTaskKind::Compact => DataNodeTaskOutput::Compact(CompactionResponse {
            status,
            shard_id: 0,
            compacted_objects: 0,
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
        }),
    }
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

fn dirty_count(dirty: &Mutex<DirtyTracker>, shard_id: ShardId) -> usize {
    dirty
        .lock()
        .expect("dirty tracker lock poisoned")
        .by_key
        .keys()
        .filter(|(dirty_shard_id, _)| *dirty_shard_id == shard_id)
        .count()
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
        | Command::RiskIncrement { key, .. } => Some(key),
        Command::RiskIncrementWithOptions { key, .. } => Some(key),
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
    fn runtime_executes_async_tracks_dirty_and_dump_clears_it() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
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
            DumpShardRequest { shard_id: 1 },
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
    fn runtime_rejects_when_queue_is_full() {
        let engine = TemporalEngine::default();
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 1,
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

    fn wait_for_job(runtime: &DataNodeRuntime, job_id: u64) -> DataNodeTaskStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
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
}
