use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::{
    AckResponse, AddNamespaceRequest, AddTableRequest, DeleteTableRequest,
    FreezeStaleServersRequest, GetShardResponse, GetTableTopologyRequest, LoadFinishRequest,
    MetaSnapshot, MetaSnapshotFileRequest, MetaSnapshotFileResponse, MetaSnapshotResponse,
    ProxyHeartbeatRequest, PublishShardSnapshotRequest, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, SafeModePolicy, ServerHeartbeatRequest,
    SingleNodeMeta, StateChangeRequest, UpdateTableRequest,
};
use temporalstore_rust::raft::{
    ProductionMetaRaftRuntime, ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind,
    ProductionRaftNode, RaftClusterStatus, RaftConfig, RaftNodeId,
};
use temporalstore_rust::rebalance::{
    DeterministicTaskScheduler, SchedulerRunReport, SchedulerTask, SchedulerTaskKind,
    SchedulerTaskResult, TaskSchedulerOptions, TaskSchedulerSnapshot,
};
use temporalstore_rust::{production_readiness_report, types::Status};

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

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct MetaSchedulerTaskResponse {
    status: Status,
    task: Option<SchedulerTask>,
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

impl MetaTaskScheduler {
    fn from_env() -> io::Result<Self> {
        std::env::var("TS_META_SCHEDULER_SNAPSHOT")
            .ok()
            .map(|path| Self::with_snapshot_path(PathBuf::from(path)))
            .transpose()
            .map(|scheduler| scheduler.unwrap_or_default())
    }

    fn with_snapshot_path(path: PathBuf) -> io::Result<Self> {
        let scheduler = if path.exists() {
            let bytes = fs::read(&path)?;
            DeterministicTaskScheduler::decode_snapshot(&bytes).map_err(io::Error::other)?
        } else {
            DeterministicTaskScheduler::default()
        };
        Ok(Self {
            inner: Arc::new(Mutex::new(scheduler)),
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

    fn persist_locked(&self, scheduler: &DeterministicTaskScheduler) -> Status {
        match &self.snapshot_path {
            Some(path) => match save_scheduler_snapshot(path, &scheduler.export_snapshot()) {
                Ok(()) => Status::ok(),
                Err(err) => Status::error("scheduler_persist_failed", err.to_string()),
            },
            None => Status::ok(),
        }
    }
}

fn save_scheduler_snapshot(path: &PathBuf, snapshot: &TaskSchedulerSnapshot) -> io::Result<()> {
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
        serde_json::to_writer_pretty(&mut file, snapshot).map_err(io::Error::other)?;
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
        ("GET", "/readiness") | ("GET", "/cpp_parity") => {
            json_response(200, &production_readiness_report())
        }
        ("GET", "/meta/info") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/meta/stats") => json_response(200, &backend_call!(meta, stats)),
        ("GET", "/meta/preflight") => json_response(200, &backend_call!(meta, preflight_report)),
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
                backend_call!(meta, finish_load, req)
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
}
