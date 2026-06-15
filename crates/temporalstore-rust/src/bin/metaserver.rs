use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json_with_options, serve, HttpRequest,
    HttpRequestOptions,
};
use temporalstore_rust::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, DeleteTableRequest,
    FreezeStaleServersRequest, GetShardResponse, GetTableTopologyRequest, LoadFinishRequest,
    MetaSnapshot, MetaSnapshotFileRequest, MetaSnapshotFileResponse, MetaSnapshotResponse,
    ProxyHeartbeatRequest, PublishShardSnapshotRequest, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, SafeModePolicy, ServerHeartbeatRequest,
    SingleNodeMeta, StateChangeRequest, TopologyVersionRequest, UpdateTableRequest,
};
use temporalstore_rust::raft::{
    ProductionMetaRaftRuntime, ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind,
    ProductionRaftNode, RaftClusterStatus, RaftConfig, RaftNodeId,
};
use temporalstore_rust::rebalance::{
    DeterministicTaskScheduler, RebalanceStep, SchedulerRunReport, SchedulerTask,
    SchedulerTaskKind, SchedulerTaskResult, TaskSchedulerOptions, TaskSchedulerSnapshot,
};
use temporalstore_rust::{
    production_readiness_report, types::Status, DataNodeLifecycleReport,
    DataNodeShardLifecycleState, LoadShardRequest, LoadShardResponse, SchedulerLifecycleToken,
    UnloadShardRequest, UnloadShardResponse,
};

fn main() {
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let backend = MetaBackend::from_env().expect("failed to initialize metaserver backend");
    let scheduler =
        MetaTaskScheduler::from_env().expect("failed to initialize metaserver scheduler");
    let stale_after_ms = env_u64("TS_META_STALE_AFTER_MS", 30_000);
    let detector_interval_ms = env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000);
    let _failure_detector = match &backend {
        MetaBackend::Single(meta) => Some(MetaBackground::Single(
            meta.start_failure_detector_loop(stale_after_ms, detector_interval_ms),
        )),
        MetaBackend::Raft(runtime) => Some(MetaBackground::Raft(runtime.start_timer_loop())),
    };
    println!("temporalstore metaserver listening on {addr}");
    serve(&addr, move |request| handle(&backend, &scheduler, request)).expect("metaserver failed");
}

#[derive(Clone)]
enum MetaBackend {
    Single(SingleNodeMeta),
    Raft(ProductionMetaRaftRuntime),
}

enum MetaBackground {
    Single(std::thread::JoinHandle<()>),
    Raft(temporalstore_rust::raft::ProductionRaftTimerHandle),
}

#[derive(Clone, Default)]
struct MetaTaskScheduler {
    inner: Arc<Mutex<DeterministicTaskScheduler>>,
    executions: Arc<Mutex<Vec<MetaSchedulerExecutionRecord>>>,
    snapshot_path: Option<PathBuf>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerSubmitRequest {
    priority: i32,
    now_ms: u64,
    kind: SchedulerTaskKind,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRunRequest {
    now_ms: u64,
    result: SchedulerTaskResult,
    #[serde(default)]
    options: Option<TaskSchedulerOptions>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRestoreRequest {
    snapshot: TaskSchedulerSnapshot,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerDurableSnapshot {
    scheduler: TaskSchedulerSnapshot,
    #[serde(default)]
    executions: Vec<MetaSchedulerExecutionRecord>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerExecuteRequest {
    now_ms: u64,
    node_addr: String,
    #[serde(default)]
    dry_run: bool,
    #[serde(default)]
    load_request: Option<LoadShardRequest>,
    #[serde(default)]
    options: Option<TaskSchedulerOptions>,
    #[serde(default)]
    http: HttpRequestOptionsView,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerTaskResponse {
    status: Status,
    task: Option<SchedulerTask>,
    #[serde(default)]
    lifecycle_token: Option<temporalstore_rust::SchedulerLifecycleToken>,
    queue_len: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerRunResponse {
    status: Status,
    report: Option<SchedulerRunReport>,
    queue_len: usize,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerSnapshotResponse {
    status: Status,
    snapshot: Option<TaskSchedulerSnapshot>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecuteResponse {
    status: Status,
    task: Option<SchedulerTask>,
    #[serde(default)]
    lifecycle_token: Option<SchedulerLifecycleToken>,
    node_addr: String,
    dry_run: bool,
    calls: Vec<MetaSchedulerNodeCall>,
    scheduler_report: Option<SchedulerRunReport>,
    #[serde(default)]
    node_lifecycle: Option<DataNodeLifecycleReport>,
    #[serde(default)]
    lifecycle_state: Option<DataNodeShardLifecycleState>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecutionsResponse {
    status: Status,
    executions: Vec<MetaSchedulerExecutionRecord>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerExecutionRecord {
    task_id: u64,
    node_addr: String,
    status: Status,
    scheduler_result: SchedulerTaskResult,
    retry_times: u64,
    #[serde(default)]
    next_run_time_ms: Option<u64>,
    calls: Vec<MetaSchedulerNodeCall>,
    #[serde(default)]
    lifecycle_token: Option<SchedulerLifecycleToken>,
    #[serde(default)]
    lifecycle_state: Option<DataNodeShardLifecycleState>,
    queue_len: usize,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct MetaSchedulerNodeCall {
    path: String,
    skipped: bool,
    status: Status,
}

#[derive(Debug, Clone, Copy, serde::Deserialize, serde::Serialize, PartialEq, Eq)]
struct HttpRequestOptionsView {
    #[serde(default = "default_connect_timeout_ms")]
    connect_timeout_ms: u64,
    #[serde(default = "default_io_timeout_ms")]
    io_timeout_ms: u64,
    #[serde(default)]
    max_retries: usize,
}

impl Default for HttpRequestOptionsView {
    fn default() -> Self {
        Self {
            connect_timeout_ms: default_connect_timeout_ms(),
            io_timeout_ms: default_io_timeout_ms(),
            max_retries: 1,
        }
    }
}

impl From<HttpRequestOptionsView> for HttpRequestOptions {
    fn from(value: HttpRequestOptionsView) -> Self {
        Self {
            connect_timeout_ms: value.connect_timeout_ms,
            io_timeout_ms: value.io_timeout_ms,
            max_retries: value.max_retries,
        }
    }
}

impl MetaTaskScheduler {
    fn from_env() -> io::Result<Self> {
        std::env::var("TS_META_SCHEDULER_SNAPSHOT")
            .ok()
            .map(|path| Self::with_snapshot_path(PathBuf::from(path)))
            .transpose()
            .map(|scheduler| scheduler.unwrap_or_default())
    }

    fn with_snapshot_path(path: PathBuf) -> io::Result<Self> {
        let (scheduler, executions) = if path.exists() {
            let bytes = fs::read(&path)?;
            decode_meta_scheduler_file(&bytes)?
        } else {
            (DeterministicTaskScheduler::default(), Vec::new())
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(scheduler)),
            executions: Arc::new(Mutex::new(executions)),
            snapshot_path: Some(path),
        })
    }

    fn snapshot(&self) -> MetaSchedulerSnapshotResponse {
        let scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        MetaSchedulerSnapshotResponse {
            status: Status::ok(),
            snapshot: Some(scheduler.export_snapshot()),
            queue_len: scheduler.queue_len(),
        }
    }

    fn submit(&self, request: MetaSchedulerSubmitRequest) -> MetaSchedulerTaskResponse {
        let (task, queue_len, persist_status) = {
            let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
            let task = scheduler.submit(request.priority, request.now_ms, request.kind);
            let queue_len = scheduler.queue_len();
            let persist_status = self.persist_locked(&scheduler);
            (task, queue_len, persist_status)
        };
        MetaSchedulerTaskResponse {
            status: persist_status,
            lifecycle_token: task.lifecycle_token(),
            task: Some(task),
            queue_len,
        }
    }

    fn run_next(&self, request: MetaSchedulerRunRequest) -> MetaSchedulerRunResponse {
        let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        match scheduler.run_next(
            request.now_ms,
            request.result,
            request.options.unwrap_or_default(),
        ) {
            Ok(report) => MetaSchedulerRunResponse {
                status: if report.is_some() {
                    self.persist_locked(&scheduler)
                } else {
                    Status::ok()
                },
                report,
                queue_len: scheduler.queue_len(),
            },
            Err(err) => MetaSchedulerRunResponse {
                status: Status::error("scheduler_error", err.to_string()),
                report: None,
                queue_len: scheduler.queue_len(),
            },
        }
    }

    fn restore(&self, request: MetaSchedulerRestoreRequest) -> MetaSchedulerSnapshotResponse {
        match DeterministicTaskScheduler::restore_snapshot(request.snapshot) {
            Ok(restored) => {
                let mut scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
                *scheduler = restored;
                let status = self.persist_locked(&scheduler);
                MetaSchedulerSnapshotResponse {
                    status,
                    snapshot: Some(scheduler.export_snapshot()),
                    queue_len: scheduler.queue_len(),
                }
            }
            Err(err) => MetaSchedulerSnapshotResponse {
                status: Status::error("scheduler_snapshot_error", err.to_string()),
                snapshot: None,
                queue_len: self
                    .inner
                    .lock()
                    .expect("meta scheduler lock poisoned")
                    .queue_len(),
            },
        }
    }

    fn execute_next(&self, request: MetaSchedulerExecuteRequest) -> MetaSchedulerExecuteResponse {
        let Some(task) = self.peek_next(request.now_ms) else {
            return MetaSchedulerExecuteResponse {
                status: Status::error("scheduler_empty", "no runnable scheduler task"),
                task: None,
                lifecycle_token: None,
                node_addr: request.node_addr,
                dry_run: request.dry_run,
                calls: Vec::new(),
                scheduler_report: None,
                node_lifecycle: None,
                lifecycle_state: None,
                queue_len: self.queue_len(),
            };
        };

        let execution = execute_scheduler_task_on_node(&task, &request);
        if request.dry_run {
            return MetaSchedulerExecuteResponse {
                status: execution.status,
                task: Some(task),
                lifecycle_token: execution.lifecycle_token,
                node_addr: request.node_addr,
                dry_run: true,
                calls: execution.calls,
                scheduler_report: None,
                node_lifecycle: None,
                lifecycle_state: None,
                queue_len: self.queue_len(),
            };
        }

        let result = classify_scheduler_execution_result(&execution.status);
        let mut calls = execution.calls;
        let (node_lifecycle, lifecycle_state) = fetch_node_lifecycle(
            &request.node_addr,
            request.http.into(),
            execution.lifecycle_token.as_ref(),
            &mut calls,
        );
        let run = self.run_next(MetaSchedulerRunRequest {
            now_ms: request.now_ms,
            result,
            options: request.options,
        });
        let status = if execution.status.ok {
            run.status.clone()
        } else {
            execution.status.clone()
        };
        let response = MetaSchedulerExecuteResponse {
            status,
            task: Some(task.clone()),
            lifecycle_token: execution.lifecycle_token.clone(),
            node_addr: request.node_addr,
            dry_run: false,
            calls: calls.clone(),
            scheduler_report: run.report.clone(),
            node_lifecycle,
            lifecycle_state: lifecycle_state.clone(),
            queue_len: run.queue_len,
        };
        self.record_execution(MetaSchedulerExecutionRecord {
            task_id: task.id,
            node_addr: response.node_addr.clone(),
            status: response.status.clone(),
            scheduler_result: result,
            retry_times: run
                .report
                .as_ref()
                .map(|report| report.retry_times)
                .unwrap_or(0),
            next_run_time_ms: run
                .report
                .as_ref()
                .and_then(|report| report.next_run_time_ms),
            calls,
            lifecycle_token: response.lifecycle_token.clone(),
            lifecycle_state,
            queue_len: response.queue_len,
        });
        response
    }

    fn peek_next(&self, now_ms: u64) -> Option<SchedulerTask> {
        self.inner
            .lock()
            .expect("meta scheduler lock poisoned")
            .snapshot()
            .into_iter()
            .find(|task| task.next_run_time_ms <= now_ms)
    }

    fn queue_len(&self) -> usize {
        self.inner
            .lock()
            .expect("meta scheduler lock poisoned")
            .queue_len()
    }

    fn executions(&self) -> MetaSchedulerExecutionsResponse {
        MetaSchedulerExecutionsResponse {
            status: Status::ok(),
            executions: self
                .executions
                .lock()
                .expect("meta scheduler executions lock poisoned")
                .clone(),
        }
    }

    fn validate_finish_load(&self, request: &LoadFinishRequest) -> Result<(), Status> {
        match (request.scheduler_task_id, request.scheduler_generation) {
            (None, None) => return Ok(()),
            (Some(_), Some(_)) => {}
            _ => {
                return Err(Status::error(
                    "invalid_scheduler_finish_load",
                    "finish_load must include both scheduler_task_id and scheduler_generation",
                ));
            }
        }
        let task_id = request.scheduler_task_id.unwrap();
        let generation = request.scheduler_generation.unwrap();
        let executions = self
            .executions
            .lock()
            .expect("meta scheduler executions lock poisoned");
        let Some(record) = executions.iter().rev().find(|record| {
            record.task_id == task_id
                && record.lifecycle_token.as_ref().is_some_and(|token| {
                    token.task_id == task_id
                        && token.generation == generation
                        && token.shard_id == request.shard_id
                        && token.operation == "load"
                })
        }) else {
            return Err(Status::error(
                "scheduler_finish_load_not_found",
                "no matching scheduler load execution found for finish_load",
            ));
        };
        if record.node_addr != request.server_addr {
            return Err(Status::error(
                "scheduler_finish_load_node_mismatch",
                "finish_load server does not match scheduler execution node",
            ));
        }
        if !record.status.ok {
            return Err(Status::error(
                "scheduler_finish_load_not_ready",
                "scheduler execution did not complete successfully",
            ));
        }
        let Some(token) = &record.lifecycle_token else {
            return Err(Status::error(
                "scheduler_finish_load_not_found",
                "scheduler execution has no lifecycle token",
            ));
        };
        if token.load_version != request.load_version {
            return Err(Status::error(
                "scheduler_finish_load_version_mismatch",
                "finish_load load_version does not match scheduler token",
            ));
        }
        if let Some(state) = &record.lifecycle_state {
            if state.load_version != request.load_version || state.state == "failed" {
                return Err(Status::error(
                    "scheduler_finish_load_state_mismatch",
                    "nodeserver lifecycle state does not confirm the requested load",
                ));
            }
        }
        Ok(())
    }

    fn record_execution(&self, record: MetaSchedulerExecutionRecord) {
        {
            let mut executions = self
                .executions
                .lock()
                .expect("meta scheduler executions lock poisoned");
            executions.push(record);
            const MAX_EXECUTION_RECORDS: usize = 128;
            if executions.len() > MAX_EXECUTION_RECORDS {
                let overflow = executions.len() - MAX_EXECUTION_RECORDS;
                executions.drain(0..overflow);
            }
        }
        let _ = self.persist_current();
    }

    fn persist_current(&self) -> Status {
        let Some(path) = &self.snapshot_path else {
            return Status::ok();
        };
        let scheduler = self.inner.lock().expect("meta scheduler lock poisoned");
        let executions = self
            .executions
            .lock()
            .expect("meta scheduler executions lock poisoned");
        match save_scheduler_snapshot(path, &scheduler.export_snapshot(), &executions) {
            Ok(()) => Status::ok(),
            Err(err) => Status::error("scheduler_persist_failed", err.to_string()),
        }
    }

    fn persist_locked(&self, scheduler: &DeterministicTaskScheduler) -> Status {
        match &self.snapshot_path {
            Some(path) => {
                let executions = self
                    .executions
                    .lock()
                    .expect("meta scheduler executions lock poisoned");
                match save_scheduler_snapshot(path, &scheduler.export_snapshot(), &executions) {
                    Ok(()) => Status::ok(),
                    Err(err) => Status::error("scheduler_persist_failed", err.to_string()),
                }
            }
            None => Status::ok(),
        }
    }
}

#[derive(Debug)]
struct SchedulerNodeExecution {
    status: Status,
    lifecycle_token: Option<SchedulerLifecycleToken>,
    calls: Vec<MetaSchedulerNodeCall>,
}

fn execute_scheduler_task_on_node(
    task: &SchedulerTask,
    request: &MetaSchedulerExecuteRequest,
) -> SchedulerNodeExecution {
    let SchedulerTaskKind::RebalanceStep(step) = &task.kind else {
        return SchedulerNodeExecution {
            status: Status::error(
                "unsupported_scheduler_task",
                "remote execution only supports rebalance steps",
            ),
            lifecycle_token: None,
            calls: Vec::new(),
        };
    };

    let token = task.lifecycle_token();
    let mut calls = Vec::new();
    if let Some(token) = &token {
        let status = post_status_or_error(
            &request.node_addr,
            "/ServerService/RequireLifecycleToken",
            token,
            request.http.into(),
            request.dry_run,
        );
        let ok = status.ok;
        calls.push(MetaSchedulerNodeCall {
            path: "/ServerService/RequireLifecycleToken".to_string(),
            skipped: request.dry_run,
            status,
        });
        if !ok {
            return SchedulerNodeExecution {
                status: calls.last().unwrap().status.clone(),
                lifecycle_token: token.clone().into(),
                calls,
            };
        }
    }

    let status = match step {
        RebalanceStep::LoadTarget {
            shard_id,
            node_id,
            load_version,
            ..
        } => {
            let Some(mut load) = request.load_request.clone() else {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "missing_load_request",
                        "LoadTarget execution requires load_request",
                    ),
                    lifecycle_token: token,
                    calls,
                };
            };
            if load.shard_id != *shard_id || load.load_version != *load_version {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "load_request_mismatch",
                        "load_request shard_id/load_version does not match scheduler task",
                    ),
                    lifecycle_token: token,
                    calls,
                };
            }
            load.local_node_id.get_or_insert(*node_id);
            let status = post_load_or_error(
                &request.node_addr,
                "/ServerService/Load",
                &load,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Load".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::ReloadTarget {
            shard_id,
            node_id,
            load_version,
            ..
        } => {
            let Some(mut load) = request.load_request.clone() else {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "missing_load_request",
                        "ReloadTarget execution requires load_request",
                    ),
                    lifecycle_token: token,
                    calls,
                };
            };
            if load.shard_id != *shard_id || load.load_version != *load_version {
                return SchedulerNodeExecution {
                    status: Status::error(
                        "load_request_mismatch",
                        "load_request shard_id/load_version does not match scheduler task",
                    ),
                    lifecycle_token: token,
                    calls,
                };
            }
            load.local_node_id.get_or_insert(*node_id);
            let status = post_load_or_error(
                &request.node_addr,
                "/ServerService/Reload",
                &load,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Reload".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::UnloadSource { shard_id, .. } => {
            let unload = UnloadShardRequest {
                shard_id: *shard_id,
            };
            let status = post_unload_or_error(
                &request.node_addr,
                "/ServerService/Unload",
                &unload,
                request.http.into(),
                request.dry_run,
            );
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/Unload".to_string(),
                skipped: request.dry_run,
                status: status.clone(),
            });
            status
        }
        RebalanceStep::FreezeSource { .. } => Status::ok(),
        RebalanceStep::UpdateMembership { .. } => Status::error(
            "unsupported_rebalance_step",
            "membership rebalance steps use the membership executor path",
        ),
    };

    SchedulerNodeExecution {
        status,
        lifecycle_token: token,
        calls,
    }
}

fn classify_scheduler_execution_result(status: &Status) -> SchedulerTaskResult {
    if status.ok {
        return SchedulerTaskResult::Ok;
    }
    if is_permanent_scheduler_execution_status(status.code.as_str()) {
        SchedulerTaskResult::Aborted
    } else {
        SchedulerTaskResult::RetryLater
    }
}

fn is_permanent_scheduler_execution_status(code: &str) -> bool {
    matches!(
        code,
        "unsupported_scheduler_task"
            | "unsupported_rebalance_step"
            | "missing_load_request"
            | "load_request_mismatch"
    )
}

fn post_status_or_error<T: serde::Serialize>(
    addr: &str,
    path: &str,
    request: &T,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, Status>(addr, path, request, options)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn post_load_or_error(
    addr: &str,
    path: &str,
    request: &LoadShardRequest,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, LoadShardResponse>(addr, path, request, options)
        .map(|response| response.status)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn post_unload_or_error(
    addr: &str,
    path: &str,
    request: &UnloadShardRequest,
    options: HttpRequestOptions,
    dry_run: bool,
) -> Status {
    if dry_run {
        return Status::ok();
    }
    post_json_with_options::<_, UnloadShardResponse>(addr, path, request, options)
        .map(|response| response.status)
        .unwrap_or_else(|err| Status::error("node_request_failed", err.to_string()))
}

fn fetch_node_lifecycle(
    addr: &str,
    options: HttpRequestOptions,
    token: Option<&SchedulerLifecycleToken>,
    calls: &mut Vec<MetaSchedulerNodeCall>,
) -> (
    Option<DataNodeLifecycleReport>,
    Option<DataNodeShardLifecycleState>,
) {
    match get_json_with_options::<DataNodeLifecycleReport>(
        addr,
        "/ServerService/GetLifecycle",
        options,
    ) {
        Ok(report) => {
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/GetLifecycle".to_string(),
                skipped: false,
                status: Status::ok(),
            });
            let state = token.and_then(|token| {
                report
                    .transitions
                    .iter()
                    .find(|state| {
                        state.shard_id == token.shard_id
                            && state.operation == token.operation
                            && state.scheduler_task_id == Some(token.task_id)
                            && state.scheduler_generation == Some(token.generation)
                    })
                    .cloned()
            });
            (Some(report), state)
        }
        Err(err) => {
            calls.push(MetaSchedulerNodeCall {
                path: "/ServerService/GetLifecycle".to_string(),
                skipped: false,
                status: Status::error("node_lifecycle_fetch_failed", err.to_string()),
            });
            (None, None)
        }
    }
}

fn default_connect_timeout_ms() -> u64 {
    200
}

fn default_io_timeout_ms() -> u64 {
    500
}

fn decode_meta_scheduler_file(
    bytes: &[u8],
) -> io::Result<(
    DeterministicTaskScheduler,
    Vec<MetaSchedulerExecutionRecord>,
)> {
    if let Ok(snapshot) = serde_json::from_slice::<MetaSchedulerDurableSnapshot>(bytes) {
        return Ok((
            DeterministicTaskScheduler::restore_snapshot(snapshot.scheduler)
                .map_err(io::Error::other)?,
            snapshot.executions,
        ));
    }
    Ok((
        DeterministicTaskScheduler::decode_snapshot(bytes).map_err(io::Error::other)?,
        Vec::new(),
    ))
}

fn save_scheduler_snapshot(
    path: &PathBuf,
    snapshot: &TaskSchedulerSnapshot,
    executions: &[MetaSchedulerExecutionRecord],
) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
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
        serde_json::to_writer_pretty(
            &mut file,
            &MetaSchedulerDurableSnapshot {
                scheduler: snapshot.clone(),
                executions: executions.to_vec(),
            },
        )
        .map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_data()?;
    }
    fs::rename(tmp_path, path)?;
    Ok(())
}

impl MetaBackend {
    fn from_env() -> std::io::Result<Self> {
        if env_bool("TS_META_RAFT", false) || std::env::var("TS_META_RAFT_NODES").is_ok() {
            return Ok(Self::Raft(
                ProductionMetaRaftRuntime::start(runtime_options_from_env())
                    .expect("failed to initialize metaserver raft runtime"),
            ));
        }
        let meta = std::env::var("TS_META_MUTATION_LOG")
            .ok()
            .map(SingleNodeMeta::with_mutation_log)
            .transpose()?
            .unwrap_or_default();
        Ok(Self::Single(meta))
    }

    fn raft_status(&self) -> Option<RaftClusterStatus> {
        match self {
            Self::Single(_) => None,
            Self::Raft(runtime) => Some(runtime.status()),
        }
    }

    fn raft_ready(&self) -> Status {
        match self {
            Self::Single(_) => Status::error("raft_disabled", "meta raft is disabled"),
            Self::Raft(runtime) => runtime
                .validate_ready()
                .map(|_| Status::ok())
                .unwrap_or_else(|err| Status::error("raft_not_ready", err.to_string())),
        }
    }

    fn export_snapshot(&self) -> MetaSnapshotResponse {
        match self {
            Self::Single(meta) => MetaSnapshotResponse {
                status: Status::ok(),
                snapshot: Some(meta.export_snapshot()),
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotResponse {
                    status: Status::ok(),
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotResponse {
                    status: Status::error("raft_snapshot_export_failed", err.to_string()),
                    snapshot: None,
                },
            },
        }
    }

    fn install_snapshot(&self, snapshot: MetaSnapshot) -> AckResponse {
        match self {
            Self::Single(meta) => meta.install_snapshot(snapshot),
            Self::Raft(runtime) => AckResponse {
                status: runtime
                    .cluster()
                    .install_meta_snapshot_on_live_nodes(snapshot)
                    .map(|_| Status::ok())
                    .unwrap_or_else(|err| {
                        Status::error("raft_snapshot_install_failed", err.to_string())
                    }),
            },
        }
    }

    fn save_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match meta.save_snapshot(&request.path) {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: Status::ok(),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match runtime.cluster().export_meta_snapshot() {
                Ok(snapshot) => MetaSnapshotFileResponse {
                    status: save_meta_snapshot_file(&request.path, &snapshot)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("snapshot_save_failed", err)),
                    path: request.path,
                    snapshot: Some(snapshot),
                },
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_save_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }

    fn load_snapshot(&self, request: MetaSnapshotFileRequest) -> MetaSnapshotFileResponse {
        match self {
            Self::Single(meta) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = meta.install_snapshot(snapshot.clone()).status;
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
            Self::Raft(runtime) => match SingleNodeMeta::load_snapshot_from_file(&request.path) {
                Ok(snapshot) => {
                    let status = runtime
                        .cluster()
                        .install_meta_snapshot_on_live_nodes(snapshot.clone())
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| {
                            Status::error("raft_snapshot_install_failed", err.to_string())
                        });
                    MetaSnapshotFileResponse {
                        status,
                        path: request.path,
                        snapshot: Some(snapshot),
                    }
                }
                Err(err) => MetaSnapshotFileResponse {
                    status: Status::error("snapshot_load_failed", err.to_string()),
                    path: request.path,
                    snapshot: None,
                },
            },
        }
    }
}

fn save_meta_snapshot_file(path: &str, snapshot: &MetaSnapshot) -> Result<(), String> {
    let path = PathBuf::from(path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|err| err.to_string())?;
    }
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
            .open(&tmp_path)
            .map_err(|err| err.to_string())?;
        serde_json::to_writer_pretty(&mut file, snapshot).map_err(|err| err.to_string())?;
        file.write_all(b"\n").map_err(|err| err.to_string())?;
        file.sync_data().map_err(|err| err.to_string())?;
    }
    fs::rename(tmp_path, path).map_err(|err| err.to_string())
}

macro_rules! backend_call {
    ($backend:expr, $method:ident $(, $arg:expr)*) => {
        match $backend {
            MetaBackend::Single(meta) => meta.$method($($arg),*),
            MetaBackend::Raft(runtime) => runtime.cluster().$method($($arg),*),
        }
    };
}

fn handle(
    meta: &MetaBackend,
    scheduler: &MetaTaskScheduler,
    request: HttpRequest,
) -> (u16, Vec<u8>) {
    if let Some(response) = handle_master_service_route(meta, &request) {
        return response;
    }
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/metrics") | ("GET", "/MasterService/Metrics") => (
            200,
            metaserver_prometheus_metrics(meta, scheduler).into_bytes(),
        ),
        ("GET", "/readiness") | ("GET", "/cpp_parity") => {
            json_response(200, &production_readiness_report())
        }
        ("GET", "/meta/info") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/meta/stats") => json_response(200, &backend_call!(meta, stats)),
        ("GET", "/meta/preflight") => json_response(200, &backend_call!(meta, preflight_report)),
        ("GET", "/meta/topology_version") | ("GET", "/meta/topology") => json_response(
            200,
            &backend_call!(
                meta,
                topology_version_report,
                TopologyVersionRequest::default()
            ),
        ),
        ("POST", "/meta/topology_version") | ("POST", "/meta/topology") => {
            parse_or(&request.body, |req: TopologyVersionRequest| {
                backend_call!(meta, topology_version_report, req)
            })
        }
        ("GET", "/meta/raft/status") => match meta.raft_status() {
            Some(status) => json_response(200, &status),
            None => json_response(
                200,
                &Status::error("raft_disabled", "meta raft is disabled"),
            ),
        },
        ("GET", "/meta/raft/ready") => json_response(200, &meta.raft_ready()),
        ("GET", "/meta/snapshot") => json_response(200, &meta.export_snapshot()),
        ("POST", "/meta/snapshot") | ("POST", "/meta/snapshot/restore") => {
            parse_or(&request.body, |snapshot: MetaSnapshot| {
                meta.install_snapshot(snapshot)
            })
        }
        ("POST", "/meta/snapshot/save") => {
            parse_or(&request.body, |req: MetaSnapshotFileRequest| {
                meta.save_snapshot(req)
            })
        }
        ("POST", "/meta/snapshot/load") => {
            parse_or(&request.body, |req: MetaSnapshotFileRequest| {
                meta.load_snapshot(req)
            })
        }
        ("GET", "/meta/scheduler") | ("GET", "/meta/scheduler/snapshot") => {
            json_response(200, &scheduler.snapshot())
        }
        ("GET", "/meta/scheduler/executions") => json_response(200, &scheduler.executions()),
        ("POST", "/meta/scheduler/submit") => {
            parse_or(&request.body, |req: MetaSchedulerSubmitRequest| {
                scheduler.submit(req)
            })
        }
        ("POST", "/meta/scheduler/run_next") => {
            parse_or(&request.body, |req: MetaSchedulerRunRequest| {
                scheduler.run_next(req)
            })
        }
        ("POST", "/meta/scheduler/execute_next") => {
            parse_or(&request.body, |req: MetaSchedulerExecuteRequest| {
                scheduler.execute_next(req)
            })
        }
        ("POST", "/meta/scheduler/restore") => {
            parse_or(&request.body, |req: MetaSchedulerRestoreRequest| {
                scheduler.restore(req)
            })
        }
        ("POST", "/register_shard") => parse_or(&request.body, |req: RegisterShardRequest| {
            backend_call!(meta, register, req)
        }),
        ("GET", path) if path.starts_with("/shards/") => {
            let shard_id = path
                .trim_start_matches("/shards/")
                .parse()
                .unwrap_or_default();
            json_response(200, &backend_call!(meta, get, shard_id))
        }
        ("POST", "/shards/snapshot") | ("POST", "/publish_shard_snapshot") => {
            parse_or(&request.body, |req: PublishShardSnapshotRequest| {
                backend_call!(meta, publish_shard_snapshot, req)
            })
        }
        ("POST", "/servers/register") => parse_or(&request.body, |req: RegisterServerRequest| {
            backend_call!(meta, register_server, req)
        }),
        ("POST", "/servers/heartbeat") | ("POST", "/server_heartbeat") => {
            parse_or(&request.body, |req: ServerHeartbeatRequest| {
                backend_call!(meta, server_heartbeat, req)
            })
        }
        ("GET", "/servers") => json_response(200, &backend_call!(meta, list_servers)),
        ("POST", "/servers/freeze_stale") => {
            parse_or(&request.body, |req: FreezeStaleServersRequest| {
                backend_call!(
                    meta,
                    freeze_stale_resources_with_policy,
                    req.stale_after_ms,
                    SafeModePolicy {
                        server_freeze_cooldown_ms: req.server_freeze_cooldown_ms,
                        proxy_freeze_cooldown_ms: req.proxy_freeze_cooldown_ms,
                    }
                )
            })
        }
        ("GET", "/meta/safe_mode") | ("GET", "/safe_mode") => {
            json_response(200, &backend_call!(meta, safe_mode_report))
        }
        ("POST", "/partitions/finish_load") | ("POST", "/finish_load") => {
            parse_or(&request.body, |req: LoadFinishRequest| {
                match scheduler.validate_finish_load(&req) {
                    Ok(()) => backend_call!(meta, finish_load, req),
                    Err(status) => AckResponse { status },
                }
            })
        }
        ("POST", "/servers/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, freeze_server, req)
        }),
        ("POST", "/servers/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, drop_server, req)
        }),
        ("POST", "/proxies/register") => parse_or(&request.body, |req: RegisterProxyRequest| {
            backend_call!(meta, register_proxy, req)
        }),
        ("POST", "/proxies/heartbeat") | ("POST", "/proxy_heartbeat") => {
            parse_or(&request.body, |req: ProxyHeartbeatRequest| {
                backend_call!(meta, proxy_heartbeat, req)
            })
        }
        ("GET", "/proxies") => json_response(200, &backend_call!(meta, list_proxies)),
        ("POST", "/proxies/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, freeze_proxy, req)
        }),
        ("POST", "/proxies/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            backend_call!(meta, drop_proxy, req)
        }),
        ("POST", "/namespaces") => parse_or(&request.body, |req: AddNamespaceRequest| {
            backend_call!(meta, add_namespace, req)
        }),
        ("GET", "/namespaces") => json_response(200, &backend_call!(meta, list_namespaces)),
        ("POST", "/tables") => parse_or(&request.body, |req: AddTableRequest| {
            backend_call!(meta, add_table, req)
        }),
        ("POST", "/tables/delete") | ("DELETE", "/tables") => {
            parse_or(&request.body, |req: DeleteTableRequest| {
                backend_call!(meta, delete_table, req)
            })
        }
        ("POST", "/tables/update") | ("PATCH", "/tables") => {
            parse_or(&request.body, |req: UpdateTableRequest| {
                backend_call!(meta, update_table, req)
            })
        }
        ("POST", "/tables/freeze") => parse_or(&request.body, |req: DeleteTableRequest| {
            backend_call!(meta, freeze_table, req)
        }),
        ("POST", "/tables/unfreeze") => parse_or(&request.body, |req: DeleteTableRequest| {
            backend_call!(meta, unfreeze_table, req)
        }),
        ("GET", "/tables") => json_response(200, &backend_call!(meta, list_tables)),
        ("POST", "/tables/topology") | ("POST", "/table_topology") => {
            parse_or(&request.body, |req: GetTableTopologyRequest| {
                backend_call!(meta, get_table_topology, req)
            })
        }
        _ => json_response(
            404,
            &GetShardResponse {
                status: Status::error("not_found", "unknown metaserver route"),
                location: None,
            },
        ),
    }
}

fn metaserver_prometheus_metrics(meta: &MetaBackend, scheduler: &MetaTaskScheduler) -> String {
    let stats = backend_call!(meta, stats);
    let servers = backend_call!(meta, list_servers).servers;
    let proxies = backend_call!(meta, list_proxies).proxies;
    let namespaces = backend_call!(meta, list_namespaces).namespaces;
    let tables = backend_call!(meta, list_tables).tables;
    let scheduler_snapshot = scheduler.snapshot();
    let scheduler_executions = scheduler.executions();
    let mut out = String::new();

    out.push_str("# HELP temporalstore_meta_requests_total Metaserver request counters by kind.\n");
    out.push_str("# TYPE temporalstore_meta_requests_total counter\n");
    for (kind, value) in [
        ("register_shard", stats.register_shard_total),
        ("get_shard", stats.get_shard_total),
        ("server_register", stats.server_register_total),
        ("server_heartbeat", stats.server_heartbeat_total),
        ("proxy_register", stats.proxy_register_total),
        ("proxy_heartbeat", stats.proxy_heartbeat_total),
        ("namespace_create", stats.namespace_create_total),
        ("table_create", stats.table_create_total),
        ("topology_query", stats.topology_query_total),
        ("load_finish", stats.load_finish_total),
    ] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_requests_total",
            &[("kind", kind)],
            value,
        );
    }

    out.push_str("# HELP temporalstore_meta_inventory Current metaserver inventory counts.\n");
    out.push_str("# TYPE temporalstore_meta_inventory gauge\n");
    for (kind, value) in [
        ("namespace", namespaces.len() as u64),
        ("table", tables.len() as u64),
        ("server", servers.len() as u64),
        ("proxy", proxies.len() as u64),
        ("shard", stats.shard_count as u64),
    ] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_inventory",
            &[("kind", kind)],
            value,
        );
    }

    out.push_str(
        "# HELP temporalstore_meta_resource_state Current metaserver resource counts by state.\n",
    );
    out.push_str("# TYPE temporalstore_meta_resource_state gauge\n");
    for state in ["normal", "frozen", "dropped"] {
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "server"), ("state", state)],
            servers
                .iter()
                .filter(|server| server.state.as_str() == state)
                .count() as u64,
        );
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "proxy"), ("state", state)],
            proxies
                .iter()
                .filter(|proxy| proxy.state.as_str() == state)
                .count() as u64,
        );
        push_meta_metric(
            &mut out,
            "temporalstore_meta_resource_state",
            &[("resource", "table"), ("state", state)],
            tables
                .iter()
                .filter(|table| table.state.as_str() == state)
                .count() as u64,
        );
    }

    out.push_str(
        "# HELP temporalstore_meta_topology_version Current metaserver topology version.\n",
    );
    out.push_str("# TYPE temporalstore_meta_topology_version gauge\n");
    push_meta_metric(
        &mut out,
        "temporalstore_meta_topology_version",
        &[],
        stats.topology_version,
    );

    out.push_str("# HELP temporalstore_meta_scheduler_queue_depth Current metaserver scheduler queue depth.\n");
    out.push_str("# TYPE temporalstore_meta_scheduler_queue_depth gauge\n");
    push_meta_metric(
        &mut out,
        "temporalstore_meta_scheduler_queue_depth",
        &[],
        scheduler_snapshot.queue_len as u64,
    );

    out.push_str("# HELP temporalstore_meta_scheduler_executions_total Metaserver scheduler execution counters by result.\n");
    out.push_str("# TYPE temporalstore_meta_scheduler_executions_total counter\n");
    for result in ["ok", "retry_later", "aborted", "failed"] {
        let count = scheduler_executions
            .executions
            .iter()
            .filter(|record| scheduler_execution_result_label(record) == result)
            .count() as u64;
        push_meta_metric(
            &mut out,
            "temporalstore_meta_scheduler_executions_total",
            &[("result", result)],
            count,
        );
    }

    if let MetaBackend::Raft(runtime) = meta {
        out.push_str(&runtime.cluster().prometheus_metrics());
    }
    out
}

fn scheduler_execution_result_label(record: &MetaSchedulerExecutionRecord) -> &'static str {
    if !record.status.ok {
        return "failed";
    }
    match record.scheduler_result {
        SchedulerTaskResult::Ok => "ok",
        SchedulerTaskResult::RetryLater => "retry_later",
        SchedulerTaskResult::Aborted => "aborted",
    }
}

fn push_meta_metric(out: &mut String, name: &str, labels: &[(&str, &str)], value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (index, (key, value)) in labels.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(key);
            out.push_str("=\"");
            out.push_str(&escape_meta_metric_label(value));
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_meta_metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[derive(Debug, serde::Deserialize)]
struct MasterTableOptionsRequest {
    #[serde(default)]
    partition_num: u64,
    #[serde(default)]
    pin_primary: Option<bool>,
    #[serde(default)]
    replica_read_policy: Option<String>,
    #[serde(default)]
    preferred_location: Option<String>,
    #[serde(default)]
    drop_percent: Option<u8>,
    #[serde(default)]
    max_read_retries: Option<u32>,
    #[serde(default)]
    max_write_retries: Option<u32>,
    #[serde(default)]
    retry_backoff_ms: Option<u64>,
    #[serde(default)]
    continuous_failed_time_ms: Option<u64>,
    #[serde(default)]
    io_timeout_ms: Option<u64>,
    #[serde(default)]
    connect_timeout_ms: Option<u64>,
}

impl MasterTableOptionsRequest {
    fn serving_options(&self) -> temporalstore_rust::meta::TableServingOptions {
        let mut options = temporalstore_rust::meta::TableServingOptions::default();
        if let Some(pin_primary) = self.pin_primary {
            options.pin_primary = pin_primary;
        }
        if let Some(replica_read_policy) = &self.replica_read_policy {
            options.replica_read_policy = replica_read_policy.clone();
        }
        if let Some(preferred_location) = &self.preferred_location {
            options.preferred_location = preferred_location.clone();
        }
        if let Some(drop_percent) = self.drop_percent {
            options.drop_percent = drop_percent;
        }
        if let Some(max_read_retries) = self.max_read_retries {
            options.max_read_retries = max_read_retries;
        }
        if let Some(max_write_retries) = self.max_write_retries {
            options.max_write_retries = max_write_retries;
        }
        if let Some(retry_backoff_ms) = self.retry_backoff_ms {
            options.retry_backoff_ms = retry_backoff_ms;
        }
        if let Some(continuous_failed_time_ms) = self.continuous_failed_time_ms {
            options.continuous_failed_time_ms = continuous_failed_time_ms;
        }
        if let Some(io_timeout_ms) = self.io_timeout_ms {
            options.io_timeout_ms = io_timeout_ms;
        }
        if let Some(connect_timeout_ms) = self.connect_timeout_ms {
            options.connect_timeout_ms = connect_timeout_ms;
        }
        options
    }

    fn serving_options_patch(&self) -> temporalstore_rust::meta::TableServingOptionsPatch {
        temporalstore_rust::meta::TableServingOptionsPatch {
            pin_primary: self.pin_primary,
            replica_read_policy: self.replica_read_policy.clone(),
            preferred_location: self.preferred_location.clone(),
            drop_percent: self.drop_percent,
            max_read_retries: self.max_read_retries,
            max_write_retries: self.max_write_retries,
            retry_backoff_ms: self.retry_backoff_ms,
            continuous_failed_time_ms: self.continuous_failed_time_ms,
            io_timeout_ms: self.io_timeout_ms,
            connect_timeout_ms: self.connect_timeout_ms,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct MasterCreateTableRequest {
    namespace: String,
    table_name: String,
    #[serde(default)]
    first_shard_id: u64,
    #[serde(default)]
    shard_count: u64,
    #[serde(default = "default_master_replica_count")]
    replica_count: u64,
    #[serde(default)]
    table_options: Option<MasterTableOptionsRequest>,
    #[serde(default)]
    use_cpp_partition_ids: bool,
    #[serde(default)]
    partition_version: u32,
}

#[derive(Debug, serde::Deserialize)]
struct MasterUpdateTableRequest {
    namespace: String,
    table_name: String,
    #[serde(default)]
    shard_count: Option<u64>,
    #[serde(default)]
    replica_count: Option<u64>,
    #[serde(default)]
    first_shard_id: Option<u64>,
    #[serde(default)]
    table_options: Option<MasterTableOptionsRequest>,
    #[serde(default)]
    use_cpp_partition_ids: Option<bool>,
    #[serde(default)]
    partition_version: Option<u32>,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterTableRequest {
    namespace: String,
    table_name: String,
    #[serde(default)]
    open_version: u64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MasterOpenTableResponse {
    status: Status,
    open_version: u64,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterGetTableTopoRequest {
    namespace: String,
    table_name: String,
    #[serde(default)]
    old_topo_version: u64,
    #[serde(default)]
    old_topology_version: u64,
    #[serde(default)]
    idc: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    compress: bool,
}

#[allow(dead_code)]
#[derive(Debug, serde::Deserialize)]
struct MasterRegisterServerRequest {
    #[serde(default)]
    server_addr: String,
    #[serde(default)]
    host: String,
    #[serde(default)]
    port: u32,
    #[serde(default)]
    table_name: String,
    #[serde(default)]
    node_id: u64,
    #[serde(default)]
    location: String,
    #[serde(default)]
    binary_version: String,
}

fn default_master_replica_count() -> u64 {
    1
}

fn handle_master_service_route(
    meta: &MetaBackend,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/MasterService/CreateTable") => {
            parse_or(&request.body, |req: MasterCreateTableRequest| {
                let shard_count = if req.shard_count > 0 {
                    req.shard_count
                } else {
                    req.table_options
                        .as_ref()
                        .map(|options| options.partition_num)
                        .unwrap_or_default()
                        .max(1)
                };
                backend_call!(
                    meta,
                    add_table,
                    AddTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        first_shard_id: req.first_shard_id,
                        shard_count,
                        replica_count: req.replica_count.max(1),
                        use_cpp_partition_ids: req.use_cpp_partition_ids,
                        partition_version: req.partition_version,
                        serving_options: req
                            .table_options
                            .as_ref()
                            .map(MasterTableOptionsRequest::serving_options)
                            .unwrap_or_default(),
                    }
                )
            })
        }
        ("POST", "/MasterService/DeleteTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                backend_call!(
                    meta,
                    delete_table,
                    DeleteTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                    }
                )
            })
        }
        ("POST", "/MasterService/UpdateTable") | ("POST", "/MasterService/AlterTable") => {
            parse_or(&request.body, |req: MasterUpdateTableRequest| {
                let shard_count = req.shard_count.or_else(|| {
                    req.table_options.as_ref().and_then(|options| {
                        (options.partition_num > 0).then_some(options.partition_num)
                    })
                });
                backend_call!(
                    meta,
                    update_table,
                    UpdateTableRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        shard_count,
                        replica_count: req.replica_count,
                        first_shard_id: req.first_shard_id,
                        use_cpp_partition_ids: req.use_cpp_partition_ids,
                        partition_version: req.partition_version,
                        serving_options: req
                            .table_options
                            .as_ref()
                            .map(MasterTableOptionsRequest::serving_options_patch),
                    }
                )
            })
        }
        ("POST", "/MasterService/OpenTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                let topology = backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        old_topology_version: 0,
                    }
                );
                MasterOpenTableResponse {
                    open_version: topology
                        .table
                        .as_ref()
                        .map(|table| table.topology_version)
                        .unwrap_or_default(),
                    status: topology.status,
                }
            })
        }
        ("POST", "/MasterService/CloseTable") => {
            parse_or(&request.body, |req: MasterTableRequest| {
                let topology = backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        old_topology_version: 0,
                    }
                );
                AckResponse {
                    status: topology.status,
                }
            })
        }
        ("POST", "/MasterService/GetTableTopo") => {
            parse_or(&request.body, |req: MasterGetTableTopoRequest| {
                backend_call!(
                    meta,
                    get_table_topology,
                    GetTableTopologyRequest {
                        namespace: req.namespace,
                        table_name: req.table_name,
                        old_topology_version: req.old_topology_version.max(req.old_topo_version),
                    }
                )
            })
        }
        ("POST", "/MasterService/RegisterServer") => {
            parse_or(&request.body, |req: MasterRegisterServerRequest| {
                let server_addr = master_server_addr(&req);
                backend_call!(
                    meta,
                    register_server,
                    RegisterServerRequest {
                        server_addr,
                        node_id: req.node_id,
                        location: req.location,
                        binary_version: req.binary_version,
                    }
                )
            })
        }
        ("GET", "/MasterService/GetInfo") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/MasterService/Preflight") => {
            json_response(200, &backend_call!(meta, preflight_report))
        }
        ("POST", "/MasterService/UnRegisterServer") => {
            parse_or(&request.body, |req: MasterRegisterServerRequest| {
                backend_call!(
                    meta,
                    drop_server,
                    StateChangeRequest {
                        endpoint: master_server_addr(&req),
                        freeze_cooldown_ms: 0,
                    }
                )
            })
        }
        _ => return None,
    };
    Some(response)
}

fn master_server_addr(request: &MasterRegisterServerRequest) -> String {
    if !request.server_addr.is_empty() {
        request.server_addr.clone()
    } else if request.port > 0 {
        format!("{}:{}", request.host, request.port)
    } else {
        request.host.clone()
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

fn runtime_options_from_env() -> ProductionMetaRaftRuntimeOptions {
    ProductionMetaRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        local_node_id: env_u64("TS_META_RAFT_NODE_ID", 1),
        nodes: parse_meta_raft_nodes(),
        config: RaftConfig::default(),
        heartbeat_interval_ms: env_u64("TS_META_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_META_RAFT_ELECTION_TICK_MS", 50),
        failure_detector_interval_ms: env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000),
        stale_server_after_ms: env_u64("TS_META_STALE_AFTER_MS", 30_000),
    }
}

fn parse_meta_raft_nodes() -> Vec<ProductionRaftNode> {
    std::env::var("TS_META_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .enumerate()
                .filter_map(|(index, part)| parse_meta_raft_node(index, part.trim()))
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| {
            vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:17101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:17102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:17103".to_string(),
                },
            ]
        })
}

fn parse_meta_raft_node(index: usize, value: &str) -> Option<ProductionRaftNode> {
    if let Some((id, addr)) = value.split_once('=') {
        return Some(ProductionRaftNode {
            node_id: id.trim().parse().ok()?,
            addr: addr.trim().to_string(),
        });
    }
    let node_id = value.parse::<RaftNodeId>().ok()?;
    Some(ProductionRaftNode {
        node_id,
        addr: format!("127.0.0.1:{}", 17101 + index),
    })
}

fn parse_or<T, R>(body: &[u8], f: impl FnOnce(T) -> R) -> (u16, Vec<u8>)
where
    T: serde::de::DeserializeOwned,
    R: serde::Serialize,
{
    match parse_json::<T>(body) {
        Ok(req) => json_response(200, &f(req)),
        Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalstore_rust::http::HttpRequest;
    use temporalstore_rust::meta::{MetaEntityState, ShardSnapshotRef, TableTopologyResponse};
    use temporalstore_rust::rebalance::RebalanceStep;
    use temporalstore_rust::ProductionReadinessReport;

    #[test]
    fn metaserver_exposes_cpp_parity_readiness_report() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();

        for path in ["/readiness", "/cpp_parity"] {
            let (code, body) = handle(
                &backend,
                &scheduler,
                HttpRequest {
                    method: "GET".to_string(),
                    path: path.to_string(),
                    body: Vec::new(),
                },
            );
            assert_eq!(code, 200);
            let report: ProductionReadinessReport = serde_json::from_slice(&body).unwrap();
            assert!(!report.production_ready);
            assert!(!report.cpp_parity_ready);
            assert!(report.missing_count() > 0);
        }
    }

    #[test]
    fn metaserver_metrics_expose_inventory_state_and_scheduler() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "metrics-server-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "metrics-proxy-a".to_string(),
            namespace: "metrics-ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 7,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            endpoint: "metrics-proxy-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_namespace(AddNamespaceRequest {
            namespace: "metrics-ns".to_string(),
        });
        meta.add_table(AddTableRequest {
            namespace: "metrics-ns".to_string(),
            table_name: "metrics-table".to_string(),
            first_shard_id: 1,
            shard_count: 1,
            replica_count: 1,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: temporalstore_rust::meta::TableServingOptions::default(),
        });
        let backend = MetaBackend::Single(meta);
        let scheduler = MetaTaskScheduler::default();
        let submitted = scheduler.submit(MetaSchedulerSubmitRequest {
            priority: 0,
            now_ms: 1,
            kind: SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 1,
                replica_id: 1,
                node_id: 1,
            }),
        });
        assert!(submitted.status.ok, "{submitted:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/metrics".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let metrics = String::from_utf8(body).unwrap();
        assert!(metrics.contains("# TYPE temporalstore_meta_requests_total counter"));
        assert!(metrics.contains("temporalstore_meta_inventory{kind=\"namespace\"} 1"));
        assert!(metrics.contains("temporalstore_meta_inventory{kind=\"table\"} 1"));
        assert!(metrics
            .contains("temporalstore_meta_resource_state{resource=\"server\",state=\"normal\"} 1"));
        assert!(metrics
            .contains("temporalstore_meta_resource_state{resource=\"proxy\",state=\"frozen\"} 1"));
        assert!(metrics.contains("temporalstore_meta_scheduler_queue_depth 1"));
        assert!(metrics.contains("temporalstore_meta_topology_version"));
    }

    #[test]
    fn metaserver_snapshot_routes_export_save_load_and_restore_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-route-snapshot.json");
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "server-route-a".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register(RegisterShardRequest {
            shard_id: 91,
            server_addr: "server-route-a".to_string(),
        });
        meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: 91,
            snapshot: ShardSnapshotRef {
                uri: "s3://cluster/shards/91/snapshots/1-7/manifest.json".to_string(),
                checksum: "sha256:route".to_string(),
                byte_size: 91,
                last_log_index: 7,
                created_at_ms: 10,
            },
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "proxy-route-a".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 3,
            binary_version: "v1".to_string(),
        });
        meta.freeze_proxy(StateChangeRequest {
            endpoint: "proxy-route-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        meta.add_table(AddTableRequest {
            namespace: "ns".to_string(),
            table_name: "tbl".to_string(),
            first_shard_id: 91,
            shard_count: 1,
            replica_count: 1,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: temporalstore_rust::meta::TableServingOptions::default(),
        });
        let backend = MetaBackend::Single(meta.clone());
        let scheduler = MetaTaskScheduler::default();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/snapshot".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let exported: MetaSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(exported.status.ok);
        assert_eq!(exported.snapshot.as_ref().unwrap().stats.shard_count, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/save".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let saved: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(saved.status.ok);
        assert!(snapshot_path.exists());

        meta.drop_proxy(StateChangeRequest {
            endpoint: "proxy-route-a".to_string(),
            freeze_cooldown_ms: 0,
        });
        assert_eq!(
            meta.list_proxies().proxies[0].state,
            MetaEntityState::Dropped
        );

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/load".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let loaded: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok);
        assert_eq!(
            meta.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );

        let restored = SingleNodeMeta::default();
        let restore_backend = MetaBackend::Single(restored.clone());
        let restore_scheduler = MetaTaskScheduler::default();
        let (code, body) = handle(
            &restore_backend,
            &restore_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/restore".to_string(),
                body: serde_json::to_vec(&exported.snapshot.unwrap()).unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok);
        assert_eq!(
            restored.get(91).location.unwrap().server_addr,
            "server-route-a"
        );
        assert_eq!(
            restored.list_proxies().proxies[0].state,
            MetaEntityState::Frozen
        );
    }

    #[test]
    fn metaserver_safe_mode_route_reports_frozen_cooldown_resources() {
        let meta = SingleNodeMeta::default();
        meta.register_server(RegisterServerRequest {
            server_addr: "safe-server".to_string(),
            node_id: 1,
            location: "zone-a".to_string(),
            binary_version: "v1".to_string(),
        });
        meta.register_proxy(RegisterProxyRequest {
            proxy_addr: "safe-proxy".to_string(),
            namespace: "ns".to_string(),
            location: "zone-a".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        });
        let backend = MetaBackend::Single(meta);
        let scheduler = MetaTaskScheduler::default();
        std::thread::sleep(std::time::Duration::from_millis(2));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/freeze_stale".to_string(),
                body: serde_json::to_vec(&FreezeStaleServersRequest {
                    stale_after_ms: 0,
                    server_freeze_cooldown_ms: 60_000,
                    proxy_freeze_cooldown_ms: 60_000,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let stale: temporalstore_rust::meta::StaleResourceReport =
            serde_json::from_slice(&body).unwrap();
        assert!(stale.status.ok, "{stale:?}");
        assert_eq!(stale.frozen_servers, vec!["safe-server".to_string()]);
        assert_eq!(stale.frozen_proxies, vec!["safe-proxy".to_string()]);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/safe_mode".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let safe_mode: temporalstore_rust::meta::SafeModeReport =
            serde_json::from_slice(&body).unwrap();
        assert!(safe_mode.status.ok, "{safe_mode:?}");
        assert_eq!(safe_mode.blocked_servers, vec!["safe-server".to_string()]);
        assert_eq!(safe_mode.blocked_proxies, vec!["safe-proxy".to_string()]);
    }

    #[test]
    fn master_service_aliases_cover_cpp_master_surface() {
        let meta = SingleNodeMeta::default();
        let backend = MetaBackend::Single(meta.clone());
        let scheduler = MetaTaskScheduler::default();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/namespaces".to_string(),
                body: serde_json::to_vec(&AddNamespaceRequest {
                    namespace: "ns".to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok, "{ack:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/RegisterServer".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "host": "127.0.0.1",
                    "port": 19090,
                    "node_id": 99,
                    "location": "zone-a",
                    "binary_version": "v-master"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok, "{ack:?}");
        assert_eq!(
            meta.list_servers().servers[0].server_addr,
            "127.0.0.1:19090"
        );

        for path in ["/meta/preflight", "/MasterService/Preflight"] {
            let (code, body) = handle(
                &backend,
                &scheduler,
                HttpRequest {
                    method: "GET".to_string(),
                    path: path.to_string(),
                    body: Vec::new(),
                },
            );
            assert_eq!(code, 200, "{path}");
            let preflight: temporalstore_rust::meta::MetaPreflightReport =
                serde_json::from_slice(&body).unwrap();
            assert!(preflight.status.ok, "{path}: {preflight:?}");
            assert_eq!(preflight.normal_servers, 1);
        }

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/MasterService/GetInfo".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let info: temporalstore_rust::meta::MetaInfo = serde_json::from_slice(&body).unwrap();
        assert!(info.status.ok, "{info:?}");
        assert_eq!(info.stats.server_count, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/CreateTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "first_shard_id": 700,
                    "replica_count": 1,
                    "table_options": {
                        "partition_num": 2,
                        "pin_primary": false,
                        "replica_read_policy": "first_replica",
                        "preferred_location": "zone-a",
                        "drop_percent": 13,
                        "max_read_retries": 3,
                        "max_write_retries": 2,
                        "retry_backoff_ms": 9,
                        "continuous_failed_time_ms": 1234,
                        "io_timeout_ms": 345,
                        "connect_timeout_ms": 456
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok, "{ack:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/OpenTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let open: MasterOpenTableResponse = serde_json::from_slice(&body).unwrap();
        assert!(open.status.ok, "{open:?}");
        assert!(open.open_version > 0);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/GetTableTopo".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "old_topo_version": 0
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let topo: TableTopologyResponse = serde_json::from_slice(&body).unwrap();
        assert!(topo.status.ok, "{topo:?}");
        assert_eq!(topo.partitions.len(), 2);
        let serving_options = &topo.table.as_ref().unwrap().serving_options;
        assert!(!serving_options.pin_primary);
        assert_eq!(serving_options.replica_read_policy, "first_replica");
        assert_eq!(serving_options.preferred_location, "zone-a");
        assert_eq!(serving_options.drop_percent, 13);
        assert_eq!(serving_options.max_read_retries, 3);
        assert_eq!(serving_options.max_write_retries, 2);
        assert_eq!(serving_options.retry_backoff_ms, 9);
        assert_eq!(serving_options.continuous_failed_time_ms, 1234);
        assert_eq!(serving_options.io_timeout_ms, 345);
        assert_eq!(serving_options.connect_timeout_ms, 456);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/GetTableTopo".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "old_topo_version": open.open_version
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let unchanged: TableTopologyResponse = serde_json::from_slice(&body).unwrap();
        assert!(unchanged.status.ok, "{unchanged:?}");
        assert!(unchanged.unchanged);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/UpdateTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "replica_count": 2,
                    "table_options": {
                        "partition_num": 3,
                        "drop_percent": 21,
                        "replica_read_policy": "round_robin_replica"
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let update: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(update.status.ok, "{update:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/GetTableTopo".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "old_topo_version": open.open_version
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let changed_topo: TableTopologyResponse = serde_json::from_slice(&body).unwrap();
        assert!(changed_topo.status.ok, "{changed_topo:?}");
        assert!(!changed_topo.unchanged);
        assert_eq!(changed_topo.table.as_ref().unwrap().replica_count, 2);
        assert_eq!(
            changed_topo
                .table
                .as_ref()
                .unwrap()
                .serving_options
                .replica_read_policy,
            "round_robin_replica"
        );
        assert_eq!(
            changed_topo
                .table
                .as_ref()
                .unwrap()
                .serving_options
                .drop_percent,
            21
        );
        assert_eq!(changed_topo.partitions.len(), 3);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/CloseTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl",
                    "open_version": open.open_version
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let close: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(close.status.ok, "{close:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/UnRegisterServer".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "host": "127.0.0.1",
                    "port": 19090
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let unregister: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(unregister.status.ok, "{unregister:?}");
        assert_eq!(
            meta.list_servers().servers[0].state,
            MetaEntityState::Dropped
        );

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/DeleteTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let delete: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(delete.status.ok, "{delete:?}");
        assert_eq!(meta.list_tables().tables[0].state, MetaEntityState::Dropped);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/MasterService/OpenTable".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "namespace": "ns",
                    "table_name": "tbl"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let open_after_delete: MasterOpenTableResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(open_after_delete.status.code, "table_not_found");
    }

    #[test]
    fn metaserver_raft_snapshot_routes_export_save_load_and_restore_state() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_path = dir.path().join("meta-raft-route-snapshot.json");
        let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            engine: ProductionRaftEngineKind::OpenRaft,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:18101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:18102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:18103".to_string(),
                },
            ],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let backend = MetaBackend::Raft(runtime);
        let scheduler = MetaTaskScheduler::default();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/register".to_string(),
                body: serde_json::to_vec(&RegisterServerRequest {
                    server_addr: "raft-server-a".to_string(),
                    node_id: 11,
                    location: "zone-a".to_string(),
                    binary_version: "v1".to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let ack: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(ack.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/snapshot".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let exported: MetaSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(exported.status.ok);
        assert_eq!(exported.snapshot.as_ref().unwrap().stats.server_count, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/save".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let saved: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(saved.status.ok);
        assert!(snapshot_path.exists());

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/drop".to_string(),
                body: serde_json::to_vec(&StateChangeRequest {
                    endpoint: "raft-server-a".to_string(),
                    freeze_cooldown_ms: 0,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let dropped: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(dropped.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/load".to_string(),
                body: serde_json::to_vec(&MetaSnapshotFileRequest {
                    path: snapshot_path.display().to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let loaded: MetaSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/servers".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let servers: temporalstore_rust::meta::ListServersResponse =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(servers.servers[0].state, MetaEntityState::Normal);

        let restored_runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
            engine: ProductionRaftEngineKind::OpenRaft,
            local_node_id: 1,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:18201".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:18202".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:18203".to_string(),
                },
            ],
            config: RaftConfig::default(),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            failure_detector_interval_ms: 1_000,
            stale_server_after_ms: 30_000,
        })
        .unwrap();
        let restore_backend = MetaBackend::Raft(restored_runtime);
        let (code, body) = handle(
            &restore_backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/snapshot/restore".to_string(),
                body: serde_json::to_vec(&exported.snapshot.unwrap()).unwrap(),
            },
        );
        assert_eq!(code, 200);
        let restored: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(restored.status.ok);
    }

    #[test]
    fn metaserver_scheduler_routes_submit_run_snapshot_and_restore_tasks() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let low_priority = SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
            shard_id: 7,
            replica_id: 11,
            node_id: 2,
        });
        let high_priority = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 7,
            replica_id: 12,
            node_id: 3,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 10,
                    "now_ms": 100,
                    "kind": low_priority,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let low: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(low.status.ok);
        assert_eq!(low.queue_len, 1);
        let low_token = low.lifecycle_token.unwrap();
        assert_eq!(low_token.shard_id, 7);
        assert_eq!(low_token.operation, "freeze");
        assert_eq!(low_token.generation, 100);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 100,
                    "kind": high_priority,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let high: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(high.status.ok);
        assert_eq!(high.queue_len, 2);
        let high_token = high.lifecycle_token.unwrap();
        assert_eq!(high_token.shard_id, 7);
        assert_eq!(high_token.operation, "unload");
        assert_eq!(high_token.generation, 100);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 100,
                    "result": "RetryLater",
                    "options": {
                        "base_postpone_ms": 5,
                        "max_postpone_ms": 50,
                        "max_retry_times": 3,
                        "max_inflight": 1
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let retry: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(retry.status.ok);
        let retry_report = retry.report.unwrap();
        assert_eq!(retry_report.task_id, high.task.unwrap().id);
        assert_eq!(retry_report.next_run_time_ms, Some(105));
        assert_eq!(retry.queue_len, 2);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let snapshot: MetaSchedulerSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(snapshot.status.ok);
        assert_eq!(snapshot.queue_len, 2);

        let restored_scheduler = MetaTaskScheduler::default();
        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/restore".to_string(),
                body: serde_json::to_vec(&MetaSchedulerRestoreRequest {
                    snapshot: snapshot.snapshot.unwrap(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let restored: MetaSchedulerSnapshotResponse = serde_json::from_slice(&body).unwrap();
        assert!(restored.status.ok);
        assert_eq!(restored.queue_len, 2);

        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 104,
                    "result": "Ok"
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let run: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(run.status.ok);
        assert_eq!(run.report.unwrap().task_id, low.task.unwrap().id);
        assert_eq!(run.queue_len, 1);
    }

    #[test]
    fn metaserver_scheduler_execute_next_dry_run_preserves_queue() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 42,
            replica_id: 9,
            node_id: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 700,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 700,
                    "node_addr": "127.0.0.1:1",
                    "dry_run": true
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok);
        assert!(executed.dry_run);
        assert!(executed.scheduler_report.is_none());
        assert_eq!(executed.queue_len, 1);
        assert_eq!(executed.calls.len(), 2);
        assert!(executed.calls.iter().all(|call| call.skipped));
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Unload");
        assert_eq!(scheduler.snapshot().queue_len, 1);
        assert_eq!(submitted.queue_len, 1);
    }

    #[test]
    fn metaserver_scheduler_execute_next_installs_token_then_loads_node() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_recording_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
            load_version: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 900,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();

        let load_request = LoadShardRequest {
            shard_id: 44,
            load_version: 5,
            local_node_id: None,
            shard_uri: "memory://metaserver-executor".to_string(),
            start_routing_slot: 0,
            end_routing_slot: 1023,
            readonly: false,
            table_name: "executor_table".to_string(),
        };
        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 900,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok, "{executed:?}");
        assert_eq!(executed.queue_len, 0);
        assert_eq!(
            executed.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Load");
        assert_eq!(executed.calls[2].path, "/ServerService/GetLifecycle");
        let lifecycle_state = executed.lifecycle_state.as_ref().unwrap();
        assert_eq!(lifecycle_state.shard_id, 44);
        assert_eq!(lifecycle_state.operation, "load");
        assert_eq!(lifecycle_state.scheduler_task_id, Some(submitted_task.id));
        assert_eq!(lifecycle_state.scheduler_generation, Some(900));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(executions.executions[0].task_id, submitted_task.id);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(
            executions.executions[0]
                .lifecycle_state
                .as_ref()
                .unwrap()
                .operation,
            "load"
        );

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/servers/register".to_string(),
                body: serde_json::to_vec(&RegisterServerRequest {
                    server_addr: node_addr.clone(),
                    node_id: 9,
                    location: "zone-a".to_string(),
                    binary_version: "v1".to_string(),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let registered: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(registered.status.ok);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/partitions/finish_load".to_string(),
                body: serde_json::to_vec(&LoadFinishRequest {
                    server_addr: node_addr.clone(),
                    shard_id: 44,
                    load_version: 5,
                    status: Status::ok(),
                    scheduler_task_id: Some(submitted_task.id),
                    scheduler_generation: Some(900),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let finish: AckResponse = serde_json::from_slice(&body).unwrap();
        assert!(finish.status.ok, "{finish:?}");

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/partitions/finish_load".to_string(),
                body: serde_json::to_vec(&LoadFinishRequest {
                    server_addr: node_addr.clone(),
                    shard_id: 44,
                    load_version: 5,
                    status: Status::ok(),
                    scheduler_task_id: Some(submitted_task.id),
                    scheduler_generation: Some(899),
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let stale_finish: AckResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(stale_finish.status.code, "scheduler_finish_load_not_found");

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[0].1["operation"], "load");
        assert_eq!(records[0].1["task_id"], submitted_task.id);
        assert_eq!(records[1].0, "/ServerService/Load");
        assert_eq!(records[1].1["shard_id"], 44);
        assert_eq!(records[1].1["load_version"], 5);
        assert_eq!(records[1].1["local_node_id"], 9);
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_execute_next_installs_token_then_reloads_node() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_recording_reload_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::ReloadTarget {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
            load_version: 6,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 901,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();
        assert_eq!(
            submitted.lifecycle_token.as_ref().unwrap().operation,
            "reload"
        );

        let load_request = LoadShardRequest {
            shard_id: 44,
            load_version: 6,
            local_node_id: None,
            shard_uri: "memory://metaserver-reload-executor".to_string(),
            start_routing_slot: 0,
            end_routing_slot: 1023,
            readonly: true,
            table_name: "executor_table".to_string(),
        };
        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 901,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(executed.status.ok, "{executed:?}");
        assert_eq!(executed.queue_len, 0);
        assert_eq!(
            executed.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(
            executed.calls[0].path,
            "/ServerService/RequireLifecycleToken"
        );
        assert_eq!(executed.calls[1].path, "/ServerService/Reload");
        assert_eq!(executed.calls[2].path, "/ServerService/GetLifecycle");
        let lifecycle_state = executed.lifecycle_state.as_ref().unwrap();
        assert_eq!(lifecycle_state.shard_id, 44);
        assert_eq!(lifecycle_state.operation, "reload");
        assert_eq!(lifecycle_state.state, "readonly");
        assert_eq!(lifecycle_state.load_version, 6);
        assert_eq!(lifecycle_state.scheduler_task_id, Some(submitted_task.id));
        assert_eq!(lifecycle_state.scheduler_generation, Some(901));

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::Ok
        );
        assert_eq!(
            executions.executions[0]
                .lifecycle_state
                .as_ref()
                .unwrap()
                .operation,
            "reload"
        );

        let records = records.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[0].1["operation"], "reload");
        assert_eq!(records[0].1["task_id"], submitted_task.id);
        assert_eq!(records[1].0, "/ServerService/Reload");
        assert_eq!(records[1].1["shard_id"], 44);
        assert_eq!(records[1].1["load_version"], 6);
        assert_eq!(records[1].1["local_node_id"], 9);
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_retries_busy_unload_without_dropping_task() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_busy_unload_nodeserver();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::UnloadSource {
            shard_id: 44,
            replica_id: 8,
            node_id: 9,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": 900,
                    "kind": task_kind,
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        let submitted_task = submitted.task.unwrap();

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": 900,
                    "node_addr": node_addr,
                    "options": {
                        "base_postpone_ms": 50,
                        "max_postpone_ms": 50,
                        "max_retry_times": 3,
                        "max_inflight": 1
                    },
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let executed: MetaSchedulerExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(executed.status.code, "shard_busy");
        assert_eq!(executed.queue_len, 1);
        let report = executed.scheduler_report.unwrap();
        assert_eq!(report.task_id, submitted_task.id);
        assert_eq!(report.result, SchedulerTaskResult::RetryLater);
        assert_eq!(report.retry_times, 1);
        assert_eq!(report.next_run_time_ms, Some(950));
        assert!(!report.aborted);
        assert_eq!(scheduler.snapshot().queue_len, 1);

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "GET".to_string(),
                path: "/meta/scheduler/executions".to_string(),
                body: Vec::new(),
            },
        );
        assert_eq!(code, 200);
        let executions: MetaSchedulerExecutionsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(
            executions.executions[0].scheduler_result,
            SchedulerTaskResult::RetryLater
        );
        assert_eq!(executed.calls.len(), 3);
        assert_eq!(executed.calls[1].path, "/ServerService/Unload");
        assert_eq!(executed.calls[1].status.code, "shard_busy");
        assert_eq!(
            executed
                .lifecycle_state
                .as_ref()
                .unwrap()
                .last_status
                .as_ref()
                .unwrap()
                .code,
            "shard_busy"
        );

        let records = records.lock().unwrap();
        assert_eq!(records[0].0, "/ServerService/RequireLifecycleToken");
        assert_eq!(records[1].0, "/ServerService/Unload");
        assert_eq!(records[2].0, "/ServerService/GetLifecycle");
    }

    #[test]
    fn metaserver_scheduler_drives_load_reload_unload_lifecycle_workflow() {
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::default();
        let (node_addr, records) = spawn_stateful_lifecycle_nodeserver();

        let load_task = submit_scheduler_task(
            &backend,
            &scheduler,
            100,
            RebalanceStep::LoadTarget {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
                load_version: 5,
            },
        );
        let load = execute_scheduler_task(
            &backend,
            &scheduler,
            100,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 44,
                load_version: 5,
                local_node_id: None,
                shard_uri: "memory://workflow/load".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 1023,
                readonly: false,
                table_name: "workflow_table".to_string(),
            }),
        );
        assert!(load.status.ok, "{load:?}");
        assert_eq!(
            load.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let load_state = load.lifecycle_state.as_ref().unwrap();
        assert_eq!(load_state.operation, "load");
        assert_eq!(load_state.state, "serving");
        assert_eq!(load_state.load_version, 5);
        assert_eq!(load_state.scheduler_task_id, Some(load_task.id));
        assert_eq!(load.queue_len, 0);

        let reload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            200,
            RebalanceStep::ReloadTarget {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
                load_version: 6,
            },
        );
        let reload = execute_scheduler_task(
            &backend,
            &scheduler,
            200,
            &node_addr,
            Some(LoadShardRequest {
                shard_id: 44,
                load_version: 6,
                local_node_id: None,
                shard_uri: "memory://workflow/reload".to_string(),
                start_routing_slot: 0,
                end_routing_slot: 1023,
                readonly: true,
                table_name: "workflow_table".to_string(),
            }),
        );
        assert!(reload.status.ok, "{reload:?}");
        assert_eq!(
            reload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let reload_state = reload.lifecycle_state.as_ref().unwrap();
        assert_eq!(reload_state.operation, "reload");
        assert_eq!(reload_state.state, "readonly");
        assert_eq!(reload_state.load_version, 6);
        assert_eq!(reload_state.scheduler_task_id, Some(reload_task.id));
        assert_eq!(reload.queue_len, 0);

        let unload_task = submit_scheduler_task(
            &backend,
            &scheduler,
            300,
            RebalanceStep::UnloadSource {
                shard_id: 44,
                replica_id: 8,
                node_id: 9,
            },
        );
        let unload = execute_scheduler_task(&backend, &scheduler, 300, &node_addr, None);
        assert!(unload.status.ok, "{unload:?}");
        assert_eq!(
            unload.scheduler_report.unwrap().result,
            SchedulerTaskResult::Ok
        );
        let unload_state = unload.lifecycle_state.as_ref().unwrap();
        assert_eq!(unload_state.operation, "unload");
        assert_eq!(unload_state.state, "unloaded");
        assert_eq!(unload_state.scheduler_task_id, Some(unload_task.id));
        assert_eq!(unload.queue_len, 0);

        let executions = scheduler.executions();
        assert_eq!(executions.executions.len(), 3);
        let operations = executions
            .executions
            .iter()
            .map(|record| record.lifecycle_state.as_ref().unwrap().operation.as_str())
            .collect::<Vec<_>>();
        assert_eq!(operations, vec!["load", "reload", "unload"]);

        let records = records.lock().unwrap();
        let paths = records
            .iter()
            .map(|record| record.0.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Load",
                "/ServerService/GetLifecycle",
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Reload",
                "/ServerService/GetLifecycle",
                "/ServerService/RequireLifecycleToken",
                "/ServerService/Unload",
                "/ServerService/GetLifecycle",
            ]
        );
    }

    #[test]
    fn metaserver_scheduler_persists_snapshot_file_after_mutations() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler.json");
        let backend = MetaBackend::Single(SingleNodeMeta::default());
        let scheduler = MetaTaskScheduler::with_snapshot_path(path.clone()).unwrap();
        let task_kind = SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
            shard_id: 12,
            replica_id: 21,
            node_id: 5,
        });

        let (code, body) = handle(
            &backend,
            &scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&MetaSchedulerSubmitRequest {
                    priority: 4,
                    now_ms: 200,
                    kind: task_kind,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok);
        assert!(path.exists());

        let restored_scheduler = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        let (code, body) = handle(
            &backend,
            &restored_scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/run_next".to_string(),
                body: serde_json::to_vec(&MetaSchedulerRunRequest {
                    now_ms: 200,
                    result: SchedulerTaskResult::Ok,
                    options: None,
                })
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let run: MetaSchedulerRunResponse = serde_json::from_slice(&body).unwrap();
        assert!(run.status.ok);
        assert_eq!(run.report.unwrap().task_id, submitted.task.unwrap().id);
        assert_eq!(run.queue_len, 0);
    }

    #[test]
    fn metaserver_scheduler_restores_execution_tokens_from_snapshot_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("scheduler-executions.json");
        let scheduler = MetaTaskScheduler::with_snapshot_path(path.clone()).unwrap();
        scheduler.record_execution(MetaSchedulerExecutionRecord {
            task_id: 12,
            node_addr: "node-a".to_string(),
            status: Status::ok(),
            scheduler_result: SchedulerTaskResult::Ok,
            retry_times: 0,
            next_run_time_ms: None,
            calls: Vec::new(),
            lifecycle_token: Some(SchedulerLifecycleToken {
                task_id: 12,
                shard_id: 44,
                operation: "load".to_string(),
                load_version: 5,
                generation: 900,
            }),
            lifecycle_state: Some(DataNodeShardLifecycleState {
                shard_id: 44,
                state: "serving".to_string(),
                operation: "load".to_string(),
                load_version: 5,
                updated_at_ms: 901,
                last_status: Some(Status::ok()),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(900),
            }),
            queue_len: 0,
        });
        assert!(path.exists());

        let restored = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        let executions = restored.executions();
        assert!(executions.status.ok);
        assert_eq!(executions.executions.len(), 1);
        assert_eq!(executions.executions[0].task_id, 12);
        assert!(restored
            .validate_finish_load(&LoadFinishRequest {
                server_addr: "node-a".to_string(),
                shard_id: 44,
                load_version: 5,
                status: Status::ok(),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(900),
            })
            .is_ok());

        let stale = restored
            .validate_finish_load(&LoadFinishRequest {
                server_addr: "node-a".to_string(),
                shard_id: 44,
                load_version: 5,
                status: Status::ok(),
                scheduler_task_id: Some(12),
                scheduler_generation: Some(899),
            })
            .unwrap_err();
        assert_eq!(stale.code, "scheduler_finish_load_not_found");
    }

    #[test]
    fn metaserver_scheduler_loads_legacy_task_only_snapshot_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("legacy-scheduler.json");
        let mut scheduler = DeterministicTaskScheduler::default();
        let task = scheduler.submit(
            4,
            200,
            SchedulerTaskKind::RebalanceStep(RebalanceStep::FreezeSource {
                shard_id: 12,
                replica_id: 21,
                node_id: 5,
            }),
        );
        fs::write(
            &path,
            serde_json::to_vec_pretty(&scheduler.export_snapshot()).unwrap(),
        )
        .unwrap();

        let restored = MetaTaskScheduler::with_snapshot_path(path).unwrap();
        assert_eq!(restored.executions().executions.len(), 0);
        assert_eq!(restored.queue_len(), 1);
        assert_eq!(restored.peek_next(200).unwrap().id, task.id);
    }

    fn spawn_recording_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Load" => json_response(
                        200,
                        &LoadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/Unload" => json_response(
                        200,
                        &UnloadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 1,
                            readonly_count: 0,
                            queued_count: 0,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 0,
                            max_load_version: 5,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "serving".to_string(),
                                operation: "load".to_string(),
                                load_version: 5,
                                updated_at_ms: 901,
                                last_status: Some(Status::ok()),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(900),
                            }],
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_recording_reload_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>)
    {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Reload" => json_response(
                        200,
                        &LoadShardResponse {
                            status: Status::ok(),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 0,
                            readonly_count: 1,
                            queued_count: 0,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 0,
                            max_load_version: 6,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "readonly".to_string(),
                                operation: "reload".to_string(),
                                load_version: 6,
                                updated_at_ms: 902,
                                last_status: Some(Status::ok()),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(901),
                            }],
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_busy_unload_nodeserver() -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let server_records = Arc::clone(&records);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => json_response(200, &Status::ok()),
                    "/ServerService/Unload" => json_response(
                        200,
                        &UnloadShardResponse {
                            status: Status::error(
                                "shard_busy",
                                "cannot unload while shard work is queued or running",
                            ),
                        },
                    ),
                    "/ServerService/GetLifecycle" => json_response(
                        200,
                        &DataNodeLifecycleReport {
                            loaded_shard_count: 1,
                            serving_count: 0,
                            readonly_count: 0,
                            queued_count: 1,
                            running_count: 0,
                            unloading_count: 0,
                            failed_count: 1,
                            max_load_version: 0,
                            shards: Vec::new(),
                            transitions: vec![DataNodeShardLifecycleState {
                                shard_id: 44,
                                state: "failed".to_string(),
                                operation: "unload".to_string(),
                                load_version: 0,
                                updated_at_ms: 901,
                                last_status: Some(Status::error(
                                    "shard_busy",
                                    "cannot unload while shard work is queued or running",
                                )),
                                scheduler_task_id: Some(0),
                                scheduler_generation: Some(900),
                            }],
                        },
                    ),
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    fn spawn_stateful_lifecycle_nodeserver(
    ) -> (String, Arc<Mutex<Vec<(String, serde_json::Value)>>>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let records = Arc::new(Mutex::new(Vec::new()));
        let lifecycle = Arc::new(Mutex::new(StatefulLifecycleNode::default()));
        let server_records = Arc::clone(&records);
        let server_lifecycle = Arc::clone(&lifecycle);
        let server_addr = addr.clone();
        std::thread::spawn(move || {
            serve(&server_addr, move |request| {
                let body = if request.body.is_empty() {
                    serde_json::Value::Null
                } else {
                    serde_json::from_slice(&request.body).unwrap()
                };
                server_records
                    .lock()
                    .unwrap()
                    .push((request.path.clone(), body));
                match request.path.as_str() {
                    "/ServerService/RequireLifecycleToken" => {
                        let token: SchedulerLifecycleToken =
                            serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle.lock().unwrap().token = Some(token);
                        json_response(200, &Status::ok())
                    }
                    "/ServerService/Load" => {
                        let load: LoadShardRequest = serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_load("load", "serving", &load);
                        json_response(
                            200,
                            &LoadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/Reload" => {
                        let load: LoadShardRequest = serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_load("reload", "readonly", &load);
                        json_response(
                            200,
                            &LoadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/Unload" => {
                        let unload: UnloadShardRequest =
                            serde_json::from_slice(&request.body).unwrap();
                        server_lifecycle
                            .lock()
                            .unwrap()
                            .transition_from_unload(&unload);
                        json_response(
                            200,
                            &UnloadShardResponse {
                                status: Status::ok(),
                            },
                        )
                    }
                    "/ServerService/GetLifecycle" => {
                        let report = server_lifecycle.lock().unwrap().report();
                        json_response(200, &report)
                    }
                    _ => json_response(404, &Status::error("not_found", "unknown path")),
                }
            })
            .unwrap();
        });
        std::thread::sleep(std::time::Duration::from_millis(25));
        (addr, records)
    }

    #[derive(Debug, Default)]
    struct StatefulLifecycleNode {
        token: Option<SchedulerLifecycleToken>,
        transition: Option<DataNodeShardLifecycleState>,
        loaded: bool,
        readonly: bool,
        load_version: u64,
    }

    impl StatefulLifecycleNode {
        fn transition_from_load(&mut self, operation: &str, state: &str, load: &LoadShardRequest) {
            self.loaded = true;
            self.readonly = load.readonly;
            self.load_version = load.load_version;
            self.transition =
                Some(self.transition_state(load.shard_id, state, operation, load.load_version));
        }

        fn transition_from_unload(&mut self, unload: &UnloadShardRequest) {
            self.loaded = false;
            self.readonly = false;
            self.transition = Some(self.transition_state(
                unload.shard_id,
                "unloaded",
                "unload",
                self.load_version,
            ));
        }

        fn transition_state(
            &self,
            shard_id: u64,
            state: &str,
            operation: &str,
            load_version: u64,
        ) -> DataNodeShardLifecycleState {
            DataNodeShardLifecycleState {
                shard_id,
                state: state.to_string(),
                operation: operation.to_string(),
                load_version,
                updated_at_ms: 1_000 + load_version,
                last_status: Some(Status::ok()),
                scheduler_task_id: self.token.as_ref().map(|token| token.task_id),
                scheduler_generation: self.token.as_ref().map(|token| token.generation),
            }
        }

        fn report(&self) -> DataNodeLifecycleReport {
            DataNodeLifecycleReport {
                loaded_shard_count: usize::from(self.loaded),
                serving_count: usize::from(self.loaded && !self.readonly),
                readonly_count: usize::from(self.loaded && self.readonly),
                queued_count: 0,
                running_count: 0,
                unloading_count: 0,
                failed_count: 0,
                max_load_version: self.load_version,
                shards: Vec::new(),
                transitions: self.transition.clone().into_iter().collect(),
            }
        }
    }

    fn submit_scheduler_task(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        step: RebalanceStep,
    ) -> SchedulerTask {
        let (code, body) = handle(
            backend,
            scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/submit".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "priority": 1,
                    "now_ms": now_ms,
                    "kind": SchedulerTaskKind::RebalanceStep(step),
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        let submitted: MetaSchedulerTaskResponse = serde_json::from_slice(&body).unwrap();
        assert!(submitted.status.ok, "{submitted:?}");
        submitted.task.unwrap()
    }

    fn execute_scheduler_task(
        backend: &MetaBackend,
        scheduler: &MetaTaskScheduler,
        now_ms: u64,
        node_addr: &str,
        load_request: Option<LoadShardRequest>,
    ) -> MetaSchedulerExecuteResponse {
        let (code, body) = handle(
            backend,
            scheduler,
            HttpRequest {
                method: "POST".to_string(),
                path: "/meta/scheduler/execute_next".to_string(),
                body: serde_json::to_vec(&serde_json::json!({
                    "now_ms": now_ms,
                    "node_addr": node_addr,
                    "load_request": load_request,
                    "http": {
                        "connect_timeout_ms": 1000,
                        "io_timeout_ms": 1000,
                        "max_retries": 10
                    }
                }))
                .unwrap(),
            },
        );
        assert_eq!(code, 200);
        serde_json::from_slice(&body).unwrap()
    }
}
