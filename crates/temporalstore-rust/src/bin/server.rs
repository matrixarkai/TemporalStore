use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json, serve, HttpRequest,
    HttpRequestOptions,
};
use temporalstore_rust::meta::{
    AckResponse, GetShardResponse, LoadFinishRequest, PartitionLoad, RegisterServerRequest,
    RegisterShardRequest, RegisterShardResponse, ServerHeartbeatRequest, ServerHeartbeatResponse,
    ShardLoad,
};
use temporalstore_rust::raft::{DataRaftReadMode, DataRaftReadPolicy};
use temporalstore_rust::types::{BatchExecuteRequest, ExecuteRequest, ExecuteResponse, Status};
use temporalstore_rust::{
    handle_raft_http, CheckedBatchExecuteRequest, CheckedExecuteRequest, Command, CommandResponse,
    CompactionRequest, DataNodeRuntime, DataNodeRuntimeOptions, DistributedRaftCommandResponse,
    DistributedRaftProposeRequest, DistributedRaftReadRequest, DumpShardRequest, GcRequest,
    HttpReplicaStreamSource, LoadShardRequest, MembershipUpdateRequest, ProductionRaftEngineKind,
    ProductionRaftNode, ProductionRaftRuntime, ProductionRaftRuntimeOptions,
    ProductionRaftSecurity, RaftConfig, RaftFailoverReport, RaftNodeId, RaftRpcRuntimeOptions,
    RaftTransport, ReplicaReplayLoop, ReplicaReplayOptions, ReplicaReplayRequest,
    ReplicaReplayResponse, RequestController, ScanStreamRequest, SetConfigRequest,
    StreamReadRequest, UnloadShardRequest,
};

fn main() {
    let addr = std::env::var("TS_SERVER_BIND_ADDR")
        .or_else(|_| std::env::var("TS_SERVER_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17002".to_string());
    let advertised_addr = std::env::var("TS_SERVER_ADVERTISE_ADDR")
        .unwrap_or_else(|_| std::env::var("TS_SERVER_ADDR").unwrap_or_else(|_| addr.clone()));
    let meta_addr = std::env::var("TS_META_ADDR").unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let shard_id = std::env::var("TS_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let cache_dir =
        std::env::var("TS_CACHE_DIR").unwrap_or_else(|_| "target/temporalstore-cache".to_string());
    let page_store_dir = std::env::var("TS_PAGE_STORE_DIR")
        .unwrap_or_else(|_| "target/temporalstore-pages".to_string());
    let index_dir = std::env::var("TS_INDEX_DIR")
        .unwrap_or_else(|_| "target/temporalstore-indexes".to_string());
    let replica_replay_cursor_dir = std::env::var("TS_REPLICA_REPLAY_CURSOR_DIR")
        .unwrap_or_else(|_| format!("{index_dir}/replica-replay-cursors"));
    let cache_memory_bytes = std::env::var("TS_CACHE_MEMORY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16 * 1024 * 1024);
    let node_id = std::env::var("TS_SERVER_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_default();
    let engine =
        TemporalEngine::with_local_dirs(cache_memory_bytes, cache_dir, page_store_dir, index_dir);
    let startup_load = startup_load_shard_request(shard_id, node_id);
    let load_response = engine.load_shard_with(startup_load);
    if !load_response.status.ok {
        eprintln!(
            "startup shard load failed for shard {shard_id}: {}",
            load_response.status.message
        );
    }
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: env_usize("TS_SERVER_WORKER_THREADS", 4),
            max_queue_depth: env_usize("TS_SERVER_MAX_QUEUE_DEPTH", 1024),
            max_background_queue_depth: env_usize("TS_SERVER_MAX_BACKGROUND_QUEUE_DEPTH", 128),
        },
    );

    let location = std::env::var("TS_SERVER_LOCATION").unwrap_or_default();
    let binary_version = env!("CARGO_PKG_VERSION").to_string();
    let heartbeat_interval_ms = std::env::var("TS_SERVER_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000);
    let replica_replay_primary_addr =
        std::env::var("TS_REPLICA_REPLAY_PRIMARY_ADDR").unwrap_or_default();
    let replica_replay_interval_ms = env_u64("TS_REPLICA_REPLAY_INTERVAL_MS", 0);
    let replica_replay_max_stream_bytes =
        env_u64("TS_REPLICA_REPLAY_MAX_STREAM_BYTES", 16 * 1024 * 1024);
    let replica_replay_max_backoff_ms = env_u64(
        "TS_REPLICA_REPLAY_MAX_BACKOFF_MS",
        replica_replay_interval_ms.saturating_mul(16).max(30_000),
    );
    let raft_state = start_server_raft_from_env(shard_id, node_id, &advertised_addr);

    let server_registration = RegisterServerRequest {
        server_addr: advertised_addr.clone(),
        node_id,
        location,
        binary_version: binary_version.clone(),
    };
    match post_json::<_, AckResponse>(&meta_addr, "/servers/register", &server_registration) {
        Ok(response) if response.status.ok => {
            println!("registered server {advertised_addr} with metaserver {meta_addr}");
        }
        Ok(response) => {
            eprintln!(
                "metaserver rejected server registration: {}",
                response.status.message
            );
        }
        Err(err) => {
            eprintln!("failed to register server {advertised_addr}: {err}");
        }
    }

    let registration = RegisterShardRequest {
        shard_id,
        server_addr: advertised_addr.clone(),
    };
    match post_json::<_, RegisterShardResponse>(&meta_addr, "/register_shard", &registration) {
        Ok(response) if response.status.ok => {
            println!("registered shard {shard_id} with metaserver {meta_addr}");
        }
        Ok(response) => {
            eprintln!(
                "metaserver rejected registration: {}",
                response.status.message
            );
        }
        Err(err) => {
            eprintln!("failed to register shard {shard_id}: {err}");
        }
    }

    start_heartbeat_loop(
        engine.clone(),
        meta_addr.clone(),
        advertised_addr.clone(),
        binary_version.clone(),
        heartbeat_interval_ms,
    );
    let replica_replay_loop = start_replica_replay_loop(
        engine.clone(),
        replica_replay_cursor_dir.clone(),
        meta_addr.clone(),
        advertised_addr.clone(),
        shard_id,
        replica_replay_primary_addr,
        replica_replay_interval_ms,
        replica_replay_max_stream_bytes,
        replica_replay_max_backoff_ms,
    );

    println!("temporalstore server listening on {addr}");
    serve(&addr, move |request| {
        if let Some(raft_state) = &raft_state {
            if let Some(response) = handle_server_raft_route(raft_state, &request) {
                return response;
            }
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/metrics") => {
                let mut metrics = engine.prometheus_metrics();
                append_runtime_metrics(&mut metrics, &runtime);
                append_replica_replay_metrics(&mut metrics, &replica_replay_loop.status());
                (200, metrics.into_bytes())
            }
            ("GET", "/server/info") => json_response(200, &engine.loaded_shard_stats()),
            ("GET", "/server/runtime_stats") => json_response(200, &runtime.stats()),
            ("GET", "/server/dirty_objects") => json_response(200, &runtime.dirty_objects()),
            ("GET", "/server/replica_replay_status") => {
                json_response(200, &replica_replay_loop.status())
            }
            ("POST", "/heartbeat") => json_response(
                200,
                &send_heartbeat(&engine, &meta_addr, &advertised_addr, &binary_version),
            ),
            ("GET", path) if path.starts_with("/jobs/") => {
                let job_id = path
                    .trim_start_matches("/jobs/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &runtime.job_status(job_id))
            }
            ("POST", "/load") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.load_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/unload") => match parse_json::<UnloadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.unload_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/execute") => match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => {
                    if let Some(raft_state) = &raft_state {
                        json_response(200, &execute_via_server_raft(raft_state, req))
                    } else {
                        json_response(200, &engine.execute(req))
                    }
                }
                Err(err) => json_response(
                    400,
                    &ExecuteResponse {
                        status: Status::error("bad_request", err.to_string()),
                        response: temporalstore_rust::CommandResponse::Empty,
                    },
                ),
            },
            ("POST", "/execute_checked") => {
                match parse_json::<CheckedExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &engine.execute_checked(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/async_execute") => match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_execute(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/async_execute_checked") => {
                match parse_json::<CheckedExecuteRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &runtime.submit_checked_execute(req, RequestController::default()),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/batch_execute") => match parse_json::<BatchExecuteRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.batch_execute(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/batch_execute_checked") => {
                match parse_json::<CheckedBatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &engine.batch_execute_checked(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/dump") => match parse_json::<DumpShardRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &runtime.submit_dump(req, RequestController::default()))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/compact") => match parse_json::<CompactionRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_compaction(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/gc") => match parse_json::<GcRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &runtime.submit_gc(req, RequestController::default()))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/set_config") => match parse_json::<SetConfigRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.set_config(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("GET", path) if path.starts_with("/get_config/") => {
                let shard_id = path
                    .trim_start_matches("/get_config/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.get_config(shard_id))
            }
            ("GET", path) if path.starts_with("/get_info/") => {
                let shard_id = path
                    .trim_start_matches("/get_info/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.get_info(shard_id))
            }
            ("GET", path) if path.starts_with("/get_stats/") => {
                let shard_id = path
                    .trim_start_matches("/get_stats/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.get_stats(shard_id))
            }
            ("POST", "/update_membership") => {
                match parse_json::<MembershipUpdateRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &update_membership_with_finish_callback(
                            &engine,
                            &meta_addr,
                            &advertised_addr,
                            req,
                        ),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/read_stream") => match parse_json::<StreamReadRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.read_stream(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/scan_stream") => match parse_json::<ScanStreamRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.scan_stream(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/replica/replay") => {
                match parse_json::<ReplicaReplayRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &run_replica_replay(&engine, &replica_replay_cursor_dir, req),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            _ => json_response(404, &Status::error("not_found", "unknown server route")),
        }
    })
    .expect("server failed");
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicaReplayLoopStatus {
    enabled: bool,
    shard_id: u64,
    configured_primary_addr: String,
    last_primary_addr: Option<String>,
    attempts_total: u64,
    success_total: u64,
    failure_total: u64,
    skipped_total: u64,
    consecutive_failures: u64,
    next_delay_ms: u64,
    last_attempt_at_ms: u64,
    last_success_at_ms: u64,
    last_error: Option<String>,
    last_report: Option<temporalstore_rust::ReplicaReplayReport>,
}

#[derive(Debug, Clone)]
struct ReplicaReplayLoopHandle {
    status: Arc<Mutex<ReplicaReplayLoopStatus>>,
}

impl ReplicaReplayLoopHandle {
    fn new(status: ReplicaReplayLoopStatus) -> Self {
        Self {
            status: Arc::new(Mutex::new(status)),
        }
    }

    fn status(&self) -> ReplicaReplayLoopStatus {
        self.status.lock().unwrap().clone()
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
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

#[derive(Debug, Deserialize)]
struct RaftAdminLivenessRequest {
    node_id: RaftNodeId,
    alive: bool,
}

#[derive(Debug, Deserialize)]
struct RaftAdminElectRequest {
    node_id: RaftNodeId,
}

#[derive(Debug, Deserialize)]
struct RaftAdminPeerBlockRequest {
    peer_id: RaftNodeId,
    blocked: bool,
}

#[derive(Debug, Deserialize)]
struct RaftAdminCatchUpRequest {
    node_id: RaftNodeId,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminLivenessResponse {
    status: Status,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminFailoverResponse {
    status: Status,
    report: Option<RaftFailoverReport>,
}

#[derive(Debug, Clone)]
struct ServerRaftState {
    runtime: ProductionRaftRuntime,
    local_node_id: RaftNodeId,
    read_policy: DataRaftReadPolicy,
    local_admin_enabled: bool,
    blocked_peers: Arc<Mutex<BTreeSet<RaftNodeId>>>,
}

fn start_server_raft_from_env(
    shard_id: u64,
    node_id: u64,
    advertised_addr: &str,
) -> Option<ServerRaftState> {
    if !env_bool("TS_SERVER_RAFT", false) {
        return None;
    }
    let local_node_id = env_u64("TS_RAFT_NODE_ID", if node_id == 0 { 1 } else { node_id });
    let raft_shard_id = env_u64("TS_RAFT_SHARD_ID", shard_id);
    let nodes = parse_raft_nodes(advertised_addr, local_node_id);
    let wal_dir = std::env::var("TS_RAFT_WAL_DIR")
        .unwrap_or_else(|_| format!("target/temporalstore-server-raft/node-{local_node_id}"));
    let auth_token =
        std::env::var("TS_RAFT_AUTH_TOKEN").unwrap_or_else(|_| "local-raft-token".to_string());
    let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        shard_id: raft_shard_id,
        local_node_id,
        nodes,
        wal_dir,
        config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions {
            max_retries: env_usize("TS_RAFT_RPC_RETRIES", 2),
            deadline_ms: env_u64("TS_RAFT_RPC_DEADLINE_MS", 1_000),
            ..RaftRpcRuntimeOptions::default()
        },
        security: ProductionRaftSecurity::plaintext_for_local_chaos(auth_token),
        heartbeat_interval_ms: env_u64("TS_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_RAFT_ELECTION_TICK_MS", 50),
        max_catchup_entries_per_heartbeat: env_u64(
            "TS_RAFT_MAX_CATCHUP_ENTRIES_PER_HEARTBEAT",
            256,
        ),
        allow_plaintext_for_local_chaos: env_bool("TS_RAFT_ALLOW_PLAINTEXT", true),
    })
    .expect("failed to start server raft runtime");
    let _timer = runtime.start_timer_loop();
    Some(ServerRaftState {
        runtime,
        local_node_id,
        read_policy: data_raft_read_policy_from_env(),
        local_admin_enabled: env_bool("TS_RAFT_ENABLE_LOCAL_ADMIN", false),
        blocked_peers: Arc::new(Mutex::new(BTreeSet::new())),
    })
}

fn data_raft_read_policy_from_env() -> DataRaftReadPolicy {
    let mode = std::env::var("TS_DATA_RAFT_READ_MODE")
        .or_else(|_| std::env::var("TS_SERVER_RAFT_READ_MODE"))
        .unwrap_or_else(|_| "leader".to_string())
        .parse::<DataRaftReadMode>()
        .unwrap_or_else(|err| panic!("invalid TS_DATA_RAFT_READ_MODE: {err}"));
    DataRaftReadPolicy {
        mode,
        bounded_stale_max_index_lag: env_u64("TS_DATA_RAFT_BOUNDED_STALE_MAX_INDEX_LAG", 0),
        read_index_timeout_ms: env_u64("TS_DATA_RAFT_READ_INDEX_TIMEOUT_MS", 1_000),
    }
}

fn parse_raft_nodes(advertised_addr: &str, local_node_id: RaftNodeId) -> Vec<ProductionRaftNode> {
    std::env::var("TS_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| {
                    let (id, addr) = part.split_once('=')?;
                    Some(ProductionRaftNode {
                        node_id: id.trim().parse().ok()?,
                        addr: addr.trim().to_string(),
                    })
                })
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| {
            BTreeMap::from([(local_node_id, advertised_addr.to_string())])
                .into_iter()
                .map(|(node_id, addr)| ProductionRaftNode { node_id, addr })
                .collect()
        })
}

fn handle_server_raft_route(
    state: &ServerRaftState,
    request: &HttpRequest,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/raft/status") => json_response(200, &state.runtime.status()),
        ("POST", "/raft/admin/liveness") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminLivenessRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .set_alive(req.node_id, req.alive)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/elect") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminElectRequest>(&request.body) {
                Ok(req) => {
                    let cluster = state.runtime.cluster();
                    let status = cluster
                        .catch_up(req.node_id)
                        .and_then(|_| cluster.elect_leader(req.node_id))
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/failover") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            let response = match state.runtime.cluster().failover_primary() {
                Ok(report) => RaftAdminFailoverResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => RaftAdminFailoverResponse {
                    status: Status::error("raft_error", err.to_string()),
                    report: None,
                },
            };
            json_response(200, &response)
        }
        ("POST", "/raft/admin/catch_up") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminCatchUpRequest>(&request.body) {
                Ok(req) => {
                    let cluster = state.runtime.cluster();
                    let status = cluster
                        .build_install_snapshot_request(req.node_id)
                        .and_then(|snapshot| {
                            let response = state.runtime.transport().install_snapshot(snapshot)?;
                            if response.success {
                                cluster.catch_up(req.node_id)
                            } else {
                                Err(temporalstore_rust::RaftError::Transport(format!(
                                    "snapshot install rejected by node {}: {:?}",
                                    req.node_id, response.reject_reason
                                )))
                            }
                        })
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/local_catch_up") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminCatchUpRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .catch_up(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/block_peer") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminPeerBlockRequest>(&request.body) {
                Ok(req) => {
                    let mut blocked = state
                        .blocked_peers
                        .lock()
                        .expect("blocked peer lock poisoned");
                    if req.blocked {
                        blocked.insert(req.peer_id);
                    } else {
                        blocked.remove(&req.peer_id);
                    }
                    json_response(
                        200,
                        &RaftAdminLivenessResponse {
                            status: Status::ok(),
                        },
                    )
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/propose") => {
            match parse_json::<DistributedRaftProposeRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &command_response(state.runtime.propose(req.command)))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/read") => match parse_json::<DistributedRaftReadRequest>(&request.body) {
            Ok(req) => json_response(
                200,
                &command_response(state.runtime.read_local(req.node_id, req.command)),
            ),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        _ if request.path.starts_with("/raft/") => {
            if let Some(peer_id) = incoming_raft_peer_id(request) {
                if state
                    .blocked_peers
                    .lock()
                    .expect("blocked peer lock poisoned")
                    .contains(&peer_id)
                {
                    return Some(json_response(
                        503,
                        &Status::error("raft_peer_blocked", "local chaos peer block active"),
                    ));
                }
            }
            handle_raft_http(
                &state.runtime.cluster(),
                HttpRequest {
                    method: request.method.clone(),
                    path: request.path.clone(),
                    body: request.body.clone(),
                },
            )
        }
        _ => return None,
    };
    Some(response)
}

fn execute_via_server_raft(state: &ServerRaftState, request: ExecuteRequest) -> ExecuteResponse {
    let result = if is_raft_read_command(&request.command) {
        read_via_server_raft(state, request.command)
    } else {
        state.runtime.propose(request.command)
    };
    match result {
        Ok(response) => ExecuteResponse {
            status: Status::ok(),
            response,
        },
        Err(err) => ExecuteResponse {
            status: Status::error("raft_error", err.to_string()),
            response: CommandResponse::Empty,
        },
    }
}

fn read_via_server_raft(
    state: &ServerRaftState,
    command: Command,
) -> Result<CommandResponse, temporalstore_rust::RaftError> {
    let target_node_id = match state.read_policy.mode {
        DataRaftReadMode::Leader | DataRaftReadMode::Linearizable => {
            state.runtime.status().leader_id
        }
        DataRaftReadMode::BoundedStale | DataRaftReadMode::UnsafeAnyReplica => state.local_node_id,
    };
    let cluster = state.runtime.cluster();
    cluster.check_data_raft_read_policy(target_node_id, state.read_policy)?;
    cluster.read_from_replica(target_node_id, command)
}

fn command_response(
    result: Result<CommandResponse, temporalstore_rust::RaftError>,
) -> DistributedRaftCommandResponse {
    match result {
        Ok(response) => DistributedRaftCommandResponse {
            status: Status::ok(),
            response,
        },
        Err(err) => DistributedRaftCommandResponse {
            status: Status::error("raft_error", err.to_string()),
            response: CommandResponse::Empty,
        },
    }
}

fn incoming_raft_peer_id(request: &HttpRequest) -> Option<RaftNodeId> {
    match request.path.as_str() {
        "/raft/append_entries" | "/raft/install_snapshot" | "/raft/install_snapshot_chunk" => {
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()?
                .get("leader_id")?
                .as_u64()
        }
        "/raft/request_vote" => serde_json::from_slice::<serde_json::Value>(&request.body)
            .ok()?
            .get("candidate_id")?
            .as_u64(),
        _ => None,
    }
}

fn is_raft_read_command(command: &Command) -> bool {
    matches!(
        command,
        Command::CommonTtl { .. }
            | Command::CommonExists { .. }
            | Command::StringGet { .. }
            | Command::HashGet { .. }
            | Command::HashMultiGet { .. }
            | Command::HashGetAll { .. }
            | Command::HashLen { .. }
            | Command::SetMembers { .. }
            | Command::FeatureQuery { .. }
            | Command::FeatureQueryFiltered { .. }
            | Command::FeatureAggQuery { .. }
            | Command::SequenceQuery { .. }
            | Command::SequenceBatchQuery { .. }
            | Command::IpsQueryLast { .. }
            | Command::IpsQueryRange { .. }
            | Command::IpsBatchQueryLast { .. }
            | Command::IpsCount { .. }
            | Command::IpsQueryRangeWithOptions { .. }
            | Command::IpsSnapshot { .. }
            | Command::IpsStat { .. }
            | Command::IpsFilter { .. }
            | Command::RiskCount { .. }
            | Command::RiskQuery { .. }
            | Command::RiskDetail { .. }
            | Command::RiskSetAndGet { .. }
            | Command::RiskFamilyQuery { .. }
            | Command::RiskManager { .. }
    )
}

fn startup_load_shard_request(shard_id: u64, node_id: u64) -> LoadShardRequest {
    LoadShardRequest {
        shard_id,
        load_version: env_u64("TS_SHARD_LOAD_VERSION", 0),
        local_node_id: if node_id == 0 { None } else { Some(node_id) },
        shard_uri: std::env::var("TS_SHARD_URI")
            .unwrap_or_else(|_| format!("local://shard/{shard_id}")),
        start_routing_slot: env_u32("TS_SHARD_START_ROUTING_SLOT", 0),
        end_routing_slot: env_u32("TS_SHARD_END_ROUTING_SLOT", u32::MAX),
        readonly: env_bool("TS_SHARD_READONLY", env_bool("TS_SERVER_READONLY", false)),
        table_name: std::env::var("TS_TABLE_NAME").unwrap_or_default(),
    }
}

fn run_replica_replay(
    engine: &TemporalEngine,
    cursor_dir: &str,
    request: ReplicaReplayRequest,
) -> ReplicaReplayResponse {
    if request.primary_addr.is_empty() {
        return ReplicaReplayResponse {
            status: Status::error("bad_request", "primary_addr is required"),
            report: None,
        };
    }
    let cursor_path = request.cursor_path.clone().unwrap_or_else(|| {
        format!(
            "{}/shard-{}.cursor.json",
            cursor_dir.trim_end_matches('/'),
            request.shard_id
        )
    });
    let mut options = ReplicaReplayOptions::new(request.shard_id, cursor_path);
    if let Some(max_stream_bytes) = request.max_stream_bytes {
        options.max_stream_bytes = max_stream_bytes.max(1);
    }
    let source = HttpReplicaStreamSource::with_options(
        request.primary_addr,
        HttpRequestOptions {
            connect_timeout_ms: 1_000,
            io_timeout_ms: 5_000,
            max_retries: 3,
        },
    );
    match ReplicaReplayLoop::new(options).run(&source, engine) {
        Ok(report) => ReplicaReplayResponse {
            status: Status::ok(),
            report: Some(report),
        },
        Err(err) => ReplicaReplayResponse {
            status: Status::error("replica_replay_failed", err.to_string()),
            report: None,
        },
    }
}

fn update_membership_with_finish_callback(
    engine: &TemporalEngine,
    meta_addr: &str,
    server_addr: &str,
    request: MembershipUpdateRequest,
) -> Status {
    let shard_id = request.shard_id;
    let status = engine.update_membership(request);
    if status.ok {
        if let Some(info) = engine.get_info(shard_id).info {
            let _ = post_json::<_, AckResponse>(
                meta_addr,
                "/partitions/finish_load",
                &LoadFinishRequest {
                    server_addr: server_addr.to_string(),
                    shard_id,
                    load_version: info.load_version,
                    status: status.clone(),
                },
            );
        }
    }
    status
}

fn start_replica_replay_loop(
    engine: TemporalEngine,
    cursor_dir: String,
    meta_addr: String,
    local_addr: String,
    shard_id: u64,
    primary_addr: String,
    interval_ms: u64,
    max_stream_bytes: u64,
    max_backoff_ms: u64,
) -> ReplicaReplayLoopHandle {
    let handle = ReplicaReplayLoopHandle::new(ReplicaReplayLoopStatus {
        enabled: interval_ms > 0,
        shard_id,
        configured_primary_addr: primary_addr.clone(),
        last_primary_addr: None,
        attempts_total: 0,
        success_total: 0,
        failure_total: 0,
        skipped_total: 0,
        consecutive_failures: 0,
        next_delay_ms: interval_ms,
        last_attempt_at_ms: 0,
        last_success_at_ms: 0,
        last_error: None,
        last_report: None,
    });
    if interval_ms == 0 {
        return handle;
    }
    let status = Arc::clone(&handle.status);
    let max_backoff_ms = max_backoff_ms.max(interval_ms);
    std::thread::spawn(move || loop {
        let mut delay_ms = interval_ms;
        if let Some(primary_addr) =
            resolve_replica_replay_primary_addr(&meta_addr, &local_addr, shard_id, &primary_addr)
        {
            {
                let mut status = status.lock().unwrap();
                status.last_primary_addr = Some(primary_addr.clone());
                status.attempts_total += 1;
                status.last_attempt_at_ms = now_ms();
                status.next_delay_ms = interval_ms;
            }
            let response = run_replica_replay(
                &engine,
                &cursor_dir,
                ReplicaReplayRequest {
                    shard_id,
                    primary_addr,
                    cursor_path: None,
                    max_stream_bytes: Some(max_stream_bytes),
                },
            );
            let mut status = status.lock().unwrap();
            if response.status.ok {
                status.success_total += 1;
                status.consecutive_failures = 0;
                status.last_success_at_ms = now_ms();
                status.last_error = None;
                status.last_report = response.report;
                status.next_delay_ms = interval_ms;
            } else {
                status.failure_total += 1;
                status.consecutive_failures += 1;
                status.last_error = Some(response.status.message);
                let shifts = status.consecutive_failures.saturating_sub(1).min(10) as u32;
                delay_ms = interval_ms
                    .saturating_mul(1u64 << shifts)
                    .min(max_backoff_ms);
                status.next_delay_ms = delay_ms;
            }
        } else {
            let mut status = status.lock().unwrap();
            status.skipped_total += 1;
            status.last_primary_addr = None;
            status.last_error = Some("primary route unavailable or local".to_string());
            status.next_delay_ms = interval_ms;
        }
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
    });
    handle
}

fn resolve_replica_replay_primary_addr(
    meta_addr: &str,
    local_addr: &str,
    shard_id: u64,
    configured_primary_addr: &str,
) -> Option<String> {
    let candidate = if configured_primary_addr.is_empty() {
        let response = get_json_with_options::<GetShardResponse>(
            meta_addr,
            &format!("/shards/{shard_id}"),
            HttpRequestOptions {
                connect_timeout_ms: 500,
                io_timeout_ms: 1_000,
                max_retries: 1,
            },
        )
        .ok()?;
        if !response.status.ok {
            return None;
        }
        response.location?.server_addr
    } else {
        configured_primary_addr.to_string()
    };
    if candidate.is_empty() || candidate == local_addr {
        None
    } else {
        Some(candidate)
    }
}

fn append_runtime_metrics(out: &mut String, runtime: &DataNodeRuntime) {
    let stats = runtime.stats();
    out.push_str("# HELP temporalstore_data_node_runtime_jobs_total Data node runtime job counters by kind.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_jobs_total counter\n");
    for (kind, value) in [
        ("submitted", stats.submitted_total),
        ("completed", stats.completed_total),
        ("rejected", stats.rejected_total),
        ("rejected_background", stats.rejected_background_total),
        ("timed_out", stats.timed_out_total),
        ("dump", stats.dump_runs),
        ("compaction", stats.compaction_runs),
        ("gc", stats.gc_runs),
    ] {
        out.push_str("temporalstore_data_node_runtime_jobs_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    out.push_str("# HELP temporalstore_data_node_runtime_queue_depth Current data node runtime queue depth.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_queue_depth gauge\n");
    out.push_str("temporalstore_data_node_runtime_queue_depth ");
    out.push_str(&stats.queue_depth.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_background_queue_depth Current background data node queue depth.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_background_queue_depth gauge\n");
    out.push_str("temporalstore_data_node_runtime_background_queue_depth ");
    out.push_str(&stats.background_queue_depth.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_queued_shards Current shard queues with pending work.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_queued_shards gauge\n");
    out.push_str("temporalstore_data_node_runtime_queued_shards ");
    out.push_str(&stats.queued_shard_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_runtime_running_shards Current shard lanes executing work.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_running_shards gauge\n");
    out.push_str("temporalstore_data_node_runtime_running_shards ");
    out.push_str(&stats.running_shard_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_dirty_objects Dirty object count.\n");
    out.push_str("# TYPE temporalstore_data_node_dirty_objects gauge\n");
    out.push_str("temporalstore_data_node_dirty_objects ");
    out.push_str(&stats.dirty_object_count.to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_dirty_shards Dirty shard count.\n");
    out.push_str("# TYPE temporalstore_data_node_dirty_shards gauge\n");
    out.push_str("temporalstore_data_node_dirty_shards ");
    out.push_str(&stats.dirty_shard_count.to_string());
    out.push('\n');
}

fn append_replica_replay_metrics(out: &mut String, status: &ReplicaReplayLoopStatus) {
    out.push_str("# HELP temporalstore_replica_replay_loop_enabled Whether background replica replay is enabled.\n");
    out.push_str("# TYPE temporalstore_replica_replay_loop_enabled gauge\n");
    out.push_str("temporalstore_replica_replay_loop_enabled ");
    out.push_str(if status.enabled { "1" } else { "0" });
    out.push('\n');

    out.push_str("# HELP temporalstore_replica_replay_loop_events_total Background replica replay loop events.\n");
    out.push_str("# TYPE temporalstore_replica_replay_loop_events_total counter\n");
    for (kind, value) in [
        ("attempt", status.attempts_total),
        ("success", status.success_total),
        ("failure", status.failure_total),
        ("skipped", status.skipped_total),
    ] {
        out.push_str("temporalstore_replica_replay_loop_events_total{shard_id=\"");
        out.push_str(&status.shard_id.to_string());
        out.push_str("\",kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }

    out.push_str("# HELP temporalstore_replica_replay_loop_consecutive_failures Current consecutive replay failures.\n");
    out.push_str("# TYPE temporalstore_replica_replay_loop_consecutive_failures gauge\n");
    out.push_str("temporalstore_replica_replay_loop_consecutive_failures{shard_id=\"");
    out.push_str(&status.shard_id.to_string());
    out.push_str("\"} ");
    out.push_str(&status.consecutive_failures.to_string());
    out.push('\n');

    out.push_str("# HELP temporalstore_replica_replay_loop_next_delay_ms Next background replay delay in milliseconds.\n");
    out.push_str("# TYPE temporalstore_replica_replay_loop_next_delay_ms gauge\n");
    out.push_str("temporalstore_replica_replay_loop_next_delay_ms{shard_id=\"");
    out.push_str(&status.shard_id.to_string());
    out.push_str("\"} ");
    out.push_str(&status.next_delay_ms.to_string());
    out.push('\n');
}

fn start_heartbeat_loop(
    engine: TemporalEngine,
    meta_addr: String,
    server_addr: String,
    binary_version: String,
    interval_ms: u64,
) {
    if interval_ms == 0 {
        return;
    }
    std::thread::spawn(move || loop {
        let _ = send_heartbeat(&engine, &meta_addr, &server_addr, &binary_version);
        std::thread::sleep(std::time::Duration::from_millis(interval_ms));
    });
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn send_heartbeat(
    engine: &TemporalEngine,
    meta_addr: &str,
    server_addr: &str,
    binary_version: &str,
) -> ServerHeartbeatResponse {
    let stats = engine.loaded_shard_stats();
    let shard_loads = stats
        .iter()
        .map(|stats| ShardLoad {
            shard_id: stats.shard_id,
            key_count: (stats.string_records
                + stats.hash_records
                + stats.set_records
                + stats.feature_records
                + stats.sequence_records
                + stats.ips_records
                + stats.risk_records) as u64,
            memory_bytes: stats.cache.memory_bytes as u64,
        })
        .collect();
    let partition_loads = stats
        .into_iter()
        .map(|stats| PartitionLoad {
            shard_id: stats.shard_id,
            partition_info: stats.partition_info,
        })
        .collect();
    let request = ServerHeartbeatRequest {
        server_addr: server_addr.to_string(),
        boot_time_ms: 0,
        binary_version: binary_version.to_string(),
        shard_loads,
        partition_loads,
    };
    post_json::<_, ServerHeartbeatResponse>(meta_addr, "/servers/heartbeat", &request)
        .unwrap_or_else(|err| ServerHeartbeatResponse {
            status: Status::error("heartbeat_failed", err.to_string()),
            forbid_auto_register: false,
        })
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use temporalstore_rust::http::get_json_with_options;
    use temporalstore_rust::types::{Command, CommandResponse};

    use super::*;

    #[test]
    fn startup_load_request_uses_readonly_secondary_env() {
        let keys = [
            "TS_SHARD_LOAD_VERSION",
            "TS_SHARD_URI",
            "TS_SHARD_START_ROUTING_SLOT",
            "TS_SHARD_END_ROUTING_SLOT",
            "TS_SHARD_READONLY",
            "TS_SERVER_READONLY",
            "TS_TABLE_NAME",
        ];
        for key in keys {
            std::env::remove_var(key);
        }
        std::env::set_var("TS_SHARD_LOAD_VERSION", "42");
        std::env::set_var("TS_SHARD_URI", "local://table/shard-31");
        std::env::set_var("TS_SHARD_START_ROUTING_SLOT", "100");
        std::env::set_var("TS_SHARD_END_ROUTING_SLOT", "199");
        std::env::set_var("TS_SHARD_READONLY", "true");
        std::env::set_var("TS_TABLE_NAME", "events");

        let request = startup_load_shard_request(31, 7);
        assert_eq!(request.shard_id, 31);
        assert_eq!(request.load_version, 42);
        assert_eq!(request.local_node_id, Some(7));
        assert_eq!(request.shard_uri, "local://table/shard-31");
        assert_eq!(request.start_routing_slot, 100);
        assert_eq!(request.end_routing_slot, 199);
        assert!(request.readonly);
        assert_eq!(request.table_name, "events");

        let engine_dir = tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            engine_dir.path().join("cache"),
            engine_dir.path().join("pages"),
            engine_dir.path().join("index"),
        );
        assert!(engine.load_shard_with(request).status.ok);
        let write = engine.execute(ExecuteRequest {
            shard_id: 31,
            command: Command::StringSet {
                key: "readonly-startup".to_string(),
                value: b"blocked".to_vec(),
            },
        });
        assert_eq!(write.status.code, "readonly_shard");

        for key in keys {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn membership_update_posts_finish_callback_to_meta() {
        let engine_dir = tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024,
            engine_dir.path().join("cache"),
            engine_dir.path().join("pages"),
            engine_dir.path().join("index"),
        );
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 41,
                    load_version: 99,
                    local_node_id: Some(7),
                    shard_uri: "local://table/shard-41".to_string(),
                    start_routing_slot: 0,
                    end_routing_slot: 999,
                    readonly: false,
                    table_name: "events".to_string(),
                })
                .status
                .ok
        );
        let callbacks = Arc::new(Mutex::new(Vec::<LoadFinishRequest>::new()));
        let meta_addr = start_finish_load_server(Arc::clone(&callbacks));

        let status = update_membership_with_finish_callback(
            &engine,
            &meta_addr,
            "server-a:17002",
            MembershipUpdateRequest {
                shard_id: 41,
                membership_version: 3,
                replica_membership_version: 5,
                replica_node_ids: vec![7, 8],
                leader_node_id: Some(7),
            },
        );
        assert!(status.ok, "{status:?}");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let seen = callbacks.lock().unwrap().clone();
            if let Some(request) = seen.first() {
                assert_eq!(request.server_addr, "server-a:17002");
                assert_eq!(request.shard_id, 41);
                assert_eq!(request.load_version, 99);
                assert!(request.status.ok);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "metaserver did not receive finish callback"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn server_replica_replay_endpoint_pulls_remote_streams() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            primary_dir.path().join("cache"),
            primary_dir.path().join("pages"),
            primary_dir.path().join("index"),
        );
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );
        primary.load_shard(17);
        primary.execute(ExecuteRequest {
            shard_id: 17,
            command: Command::StringSet {
                key: "server-replay".to_string(),
                value: b"remote".to_vec(),
            },
        });

        let primary_addr = free_local_addr();
        let primary_for_server = primary.clone();
        let bind_addr = primary_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    ("POST", "/read_stream") => {
                        match parse_json::<StreamReadRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.read_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    ("POST", "/scan_stream") => {
                        match parse_json::<ScanStreamRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.scan_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "unknown route")),
                }
            })
            .unwrap()
        });
        wait_for_http(&primary_addr);

        let response = run_replica_replay(
            &follower,
            &cursor_dir.path().display().to_string(),
            ReplicaReplayRequest {
                shard_id: 17,
                primary_addr,
                cursor_path: None,
                max_stream_bytes: None,
            },
        );
        assert!(response.status.ok, "{:?}", response.status);
        let report = response.report.unwrap();
        assert_eq!(report.installed_page_segments, vec![0]);
        assert_eq!(report.index_log_records, 1);
        assert_eq!(report.oplog_records, 1);

        let read = follower.execute(ExecuteRequest {
            shard_id: 17,
            command: Command::StringGet {
                key: "server-replay".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"remote".to_vec())
            }
        );
    }

    #[test]
    fn server_background_replica_replay_loop_pulls_remote_streams() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            primary_dir.path().join("cache"),
            primary_dir.path().join("pages"),
            primary_dir.path().join("index"),
        );
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );
        primary.load_shard(19);
        primary.execute(ExecuteRequest {
            shard_id: 19,
            command: Command::StringSet {
                key: "background-replay".to_string(),
                value: b"loop".to_vec(),
            },
        });

        let primary_addr = start_primary_stream_server(primary);
        start_replica_replay_loop(
            follower.clone(),
            cursor_dir.path().display().to_string(),
            String::new(),
            "127.0.0.1:0".to_string(),
            19,
            primary_addr,
            10,
            16 * 1024 * 1024,
            80,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let read = follower.execute(ExecuteRequest {
                shard_id: 19,
                command: Command::StringGet {
                    key: "background-replay".to_string(),
                },
            });
            if read.response
                == (CommandResponse::Bytes {
                    value: Some(b"loop".to_vec()),
                })
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "background replica replay loop did not catch up; last response: {:?}",
                read
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn server_background_replica_replay_loop_discovers_primary_from_meta() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = TemporalEngine::with_local_dirs(
            1024,
            primary_dir.path().join("cache"),
            primary_dir.path().join("pages"),
            primary_dir.path().join("index"),
        );
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );
        primary.load_shard(23);
        primary.execute(ExecuteRequest {
            shard_id: 23,
            command: Command::StringSet {
                key: "meta-discovered-replay".to_string(),
                value: b"route".to_vec(),
            },
        });

        let primary_addr = start_primary_stream_server(primary);
        let meta_addr = start_meta_route_server(23, primary_addr.clone());
        start_replica_replay_loop(
            follower.clone(),
            cursor_dir.path().display().to_string(),
            meta_addr,
            "secondary:17002".to_string(),
            23,
            String::new(),
            10,
            16 * 1024 * 1024,
            80,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let read = follower.execute(ExecuteRequest {
                shard_id: 23,
                command: Command::StringGet {
                    key: "meta-discovered-replay".to_string(),
                },
            });
            if read.response
                == (CommandResponse::Bytes {
                    value: Some(b"route".to_vec()),
                })
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "metaserver-discovered replica replay did not catch up; last response: {:?}",
                read
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn server_background_replica_replay_loop_reports_failures_with_backoff() {
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let follower = TemporalEngine::with_local_dirs(
            1024,
            follower_dir.path().join("cache"),
            follower_dir.path().join("pages"),
            follower_dir.path().join("index"),
        );

        let missing_primary = free_local_addr();
        let replay_loop = start_replica_replay_loop(
            follower,
            cursor_dir.path().display().to_string(),
            String::new(),
            "127.0.0.1:0".to_string(),
            29,
            missing_primary,
            10,
            16 * 1024,
            40,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = replay_loop.status();
            if status.failure_total >= 2 {
                assert_eq!(status.attempts_total, status.failure_total);
                assert!(status.consecutive_failures >= 2, "{status:?}");
                assert!(status.next_delay_ms >= 20, "{status:?}");
                assert!(status.last_error.is_some(), "{status:?}");
                return;
            }
            assert!(
                Instant::now() < deadline,
                "replica replay loop did not record failures; status: {:?}",
                status
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn server_raft_execute_uses_consensus_state_for_writes_and_reads() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1], false);

        let write = execute_via_server_raft(
            &state,
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "server-raft".to_string(),
                    value: b"ok".to_vec(),
                },
            },
        );
        assert!(write.status.ok, "{write:?}");

        let read = execute_via_server_raft(
            &state,
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "server-raft".to_string(),
                },
            },
        );
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"ok".to_vec())
            }
        );
    }

    #[test]
    fn server_raft_execute_rejects_follower_writes() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 2, vec![1, 2], false);

        let response = execute_via_server_raft(
            &state,
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "follower-write".to_string(),
                    value: b"blocked".to_vec(),
                },
            },
        );
        assert_eq!(response.status.code, "raft_error");
        assert!(response.status.message.contains("not leader"));
    }

    #[test]
    fn server_raft_bounded_stale_read_policy_reads_local_replica() {
        let dir = tempdir().unwrap();
        let mut state = test_server_raft_state(dir.path(), 2, vec![1, 2], false);
        state.read_policy = DataRaftReadPolicy {
            mode: DataRaftReadMode::BoundedStale,
            bounded_stale_max_index_lag: 0,
            read_index_timeout_ms: 100,
        };
        state
            .runtime
            .cluster()
            .propose(Command::StringSet {
                key: "bounded-stale".to_string(),
                value: b"local-replica".to_vec(),
            })
            .unwrap();

        let read = execute_via_server_raft(
            &state,
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "bounded-stale".to_string(),
                },
            },
        );
        assert!(read.status.ok, "{read:?}");
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"local-replica".to_vec())
            }
        );
    }

    #[test]
    fn server_exposes_raft_status_and_admin_routes() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2], true);

        let status_request = HttpRequest {
            method: "GET".to_string(),
            path: "/raft/status".to_string(),
            body: Vec::new(),
        };
        let (code, body) = handle_server_raft_route(&state, &status_request).unwrap();
        assert_eq!(code, 200);
        let status: temporalstore_rust::RaftClusterStatus = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.leader_id, 1);

        let elect_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/elect".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "node_id": 2 })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &elect_request).unwrap();
        assert_eq!(code, 200);
        let response: RaftAdminLivenessResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
        assert_eq!(state.runtime.status().leader_id, 2);
    }

    #[test]
    fn server_raft_admin_routes_can_be_disabled() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1], false);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/failover".to_string(),
            body: b"{}".to_vec(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 403);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.code, "forbidden");
    }

    fn start_primary_stream_server(primary: TemporalEngine) -> String {
        let primary_addr = free_local_addr();
        let primary_for_server = primary.clone();
        let bind_addr = primary_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    ("POST", "/read_stream") => {
                        match parse_json::<StreamReadRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.read_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    ("POST", "/scan_stream") => {
                        match parse_json::<ScanStreamRequest>(&request.body) {
                            Ok(req) => json_response(200, &primary_for_server.scan_stream(req)),
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    _ => json_response(404, &Status::error("not_found", "unknown route")),
                }
            })
            .unwrap()
        });
        wait_for_http(&primary_addr);
        primary_addr
    }

    fn start_meta_route_server(shard_id: u64, primary_addr: String) -> String {
        let meta_addr = free_local_addr();
        let bind_addr = meta_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", path) if path == format!("/shards/{shard_id}") => json_response(
                        200,
                        &GetShardResponse {
                            status: Status::ok(),
                            location: Some(temporalstore_rust::ShardLocation {
                                shard_id,
                                server_addr: primary_addr.clone(),
                                latest_snapshot: None,
                            }),
                        },
                    ),
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    _ => json_response(404, &Status::error("not_found", "unknown route")),
                }
            })
            .unwrap()
        });
        wait_for_http(&meta_addr);
        meta_addr
    }

    fn start_finish_load_server(callbacks: Arc<Mutex<Vec<LoadFinishRequest>>>) -> String {
        let meta_addr = free_local_addr();
        let bind_addr = meta_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/partitions/finish_load") => {
                        match parse_json::<LoadFinishRequest>(&request.body) {
                            Ok(req) => {
                                callbacks.lock().unwrap().push(req);
                                json_response(
                                    200,
                                    &AckResponse {
                                        status: Status::ok(),
                                    },
                                )
                            }
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    _ => json_response(404, &Status::error("not_found", "unknown route")),
                }
            })
            .unwrap()
        });
        wait_for_http(&meta_addr);
        meta_addr
    }

    fn free_local_addr() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    fn wait_for_http(addr: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if get_json_with_options::<Status>(
                addr,
                "/health",
                HttpRequestOptions {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 100,
                    max_retries: 0,
                },
            )
            .is_ok()
            {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "primary stream server did not start"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn test_server_raft_state(
        root: &std::path::Path,
        local_node_id: RaftNodeId,
        voters: Vec<RaftNodeId>,
        local_admin_enabled: bool,
    ) -> ServerRaftState {
        let nodes = voters
            .into_iter()
            .map(|node_id| ProductionRaftNode {
                node_id,
                addr: free_local_addr(),
            })
            .collect::<Vec<_>>();
        let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
            engine: ProductionRaftEngineKind::OpenRaft,
            shard_id: 1,
            local_node_id,
            nodes,
            wal_dir: root
                .join(format!("server-raft-node-{local_node_id}"))
                .display()
                .to_string(),
            config: RaftConfig::default(),
            rpc: RaftRpcRuntimeOptions {
                max_retries: 1,
                deadline_ms: 100,
                ..RaftRpcRuntimeOptions::default()
            },
            security: ProductionRaftSecurity::plaintext_for_local_chaos("test-token"),
            heartbeat_interval_ms: 20,
            election_tick_ms: 10,
            max_catchup_entries_per_heartbeat: 32,
            allow_plaintext_for_local_chaos: true,
        })
        .unwrap();
        ServerRaftState {
            runtime,
            local_node_id,
            read_policy: DataRaftReadPolicy::default(),
            local_admin_enabled,
            blocked_peers: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }
}
