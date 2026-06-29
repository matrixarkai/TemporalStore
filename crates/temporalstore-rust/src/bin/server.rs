use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use temporalstore_rust::context_workflow::{
    context_pipeline_manage_report, context_workflow_state_report, default_context_model_providers,
    extract_context, ingest_extract_context, inject_context, retrieve_context,
    ContextExtractRequest, ContextIngestExtractRequest, ContextInjectRequest,
    ContextModelProviderConfig, ContextRetrieveRequest,
};
use temporalstore_rust::data_node::{DataNodeLifecycleSnapshot, DataNodeTopologyValidationReport};
use temporalstore_rust::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json, serve, HttpRequest,
    HttpRequestOptions,
};
use temporalstore_rust::ingestion::{FlinkCheckpointStatus, IngestionBatchRequest};
use temporalstore_rust::meta::{
    AckResponse, GetShardResponse, GetTableTopologyRequest, LoadFinishRequest, PartitionLoad,
    RegisterServerRequest, RegisterShardRequest, RegisterShardResponse, ServerHeartbeatRequest,
    ServerHeartbeatResponse, ShardLoad, ShardSnapshotRef, TableTopologyResponse,
};
use temporalstore_rust::raft::{
    DataRaftCommittedLogApplier, DataRaftReadMode, DataRaftReadPolicy, RaftReplicaBootstrapPlan,
    RaftSnapshotPublishReport, RaftSnapshotTriggerReport, ReadIndexResponse,
};
use temporalstore_rust::types::{
    BatchExecuteRequest, ExecuteRequest, ExecuteResponse, ReplicatedBatchExecuteRequest,
    ReplicatedExecuteRequest, Status,
};
use temporalstore_rust::{
    handle_authenticated_raft_http, production_raft_security_from_env, production_readiness_report,
    BlockStoreOptions, CheckedBatchExecuteRequest, CheckedExecuteRequest, Command, CommandResponse,
    CompactionRequest, DataNodeRuntime, DataNodeRuntimeOptions, DistributedRaftCommandResponse,
    DistributedRaftProposeRequest, DistributedRaftReadRequest, DumpShardRequest, GcRequest,
    HttpReplicaStreamSource, LoadShardRequest, MembershipUpdateRequest, ProductionRaftEngineKind,
    ProductionRaftNode, ProductionRaftRuntime, ProductionRaftRuntimeOptions, RaftConfig,
    RaftControlLeadershipRequest, RaftFailoverReport, RaftMembershipChangeReport, RaftNodeId,
    RaftRpcRuntimeOptions, RaftTransport, ReplicaReplayLoop, ReplicaReplayOptions,
    ReplicaReplayRequest, ReplicaReplayResponse, RequestController, ScanStreamRequest,
    SchedulerLifecycleToken, SetConfigRequest, SlotDumpManifest, StorageCacheInvalidateSlotRequest,
    StorageLifecycleRequest, StorageProductionReadinessRequest, StreamReadRequest,
    UnloadShardRequest,
};
use temporalstore_snapshot::{FileObjectStore, S3SnapshotStore};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ServerTopologyValidationRequest {
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    server_addr: String,
    #[serde(default)]
    table_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ServerTopologyValidationResponse {
    status: Status,
    report: DataNodeTopologyValidationReport,
    fetched_tables: Vec<String>,
    fetch_errors: Vec<String>,
}

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
    let block_store_dir = std::env::var("TS_PAGE_STORE_DIR")
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
    let block_store_options = block_store_options_from_env();
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        cache_memory_bytes,
        cache_dir,
        block_store_dir,
        index_dir,
        block_store_options,
    );
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
        runtime.clone(),
        meta_addr.clone(),
        advertised_addr.clone(),
        binary_version.clone(),
        heartbeat_interval_ms,
    );
    let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> = Arc::default();
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
        if let Some(response) = handle_ping_route(&request) {
            return response;
        }
        if let Some(response) = handle_readiness_route(&request) {
            return response;
        }
        if let Some(response) = handle_cpp_server_service_route(
            &request,
            &engine,
            &runtime,
            raft_state.as_ref(),
            &meta_addr,
            &advertised_addr,
            &data_raft_appliers,
        ) {
            return response;
        }
        if let Some(raft_state) = &raft_state {
            if let Some(response) = handle_server_raft_route(raft_state, &request) {
                return response;
            }
        }
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/metrics") => {
                let mut metrics = engine.prometheus_metrics();
                append_ingestion_metrics(&mut metrics, &engine);
                append_runtime_metrics(&mut metrics, &runtime);
                append_replica_replay_metrics(&mut metrics, &replica_replay_loop.status());
                if let Some(raft_state) = &raft_state {
                    metrics.push_str(&raft_state.runtime.cluster().prometheus_metrics());
                }
                (200, metrics.into_bytes())
            }
            ("GET", "/server/info") => json_response(200, &engine.loaded_shard_stats()),
            ("GET", "/server/runtime_stats") => json_response(200, &runtime.stats()),
            ("GET", "/server/preflight") => json_response(200, &runtime.preflight_report()),
            ("GET", "/server/lifecycle") => json_response(200, &runtime.lifecycle_report()),
            ("GET", "/server/lifecycle/persistence") => {
                json_response(200, &runtime.lifecycle_persistence_report())
            }
            ("GET", "/server/lifecycle/tokens") => json_response(200, &runtime.lifecycle_tokens()),
            ("GET", "/server/lifecycle/snapshot") => {
                json_response(200, &runtime.lifecycle_snapshot())
            }
            ("POST", "/server/lifecycle/tokens/require") => {
                match parse_json::<SchedulerLifecycleToken>(&request.body) {
                    Ok(token) => {
                        runtime.require_lifecycle_token(token);
                        json_response(200, &Status::ok())
                    }
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/lifecycle/snapshot/restore") => {
                match parse_json::<DataNodeLifecycleSnapshot>(&request.body) {
                    Ok(snapshot) => {
                        json_response(200, &runtime.restore_lifecycle_snapshot(snapshot))
                    }
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/lifecycle/snapshot/save") => {
                match parse_json::<LifecycleSnapshotFileRequest>(&request.body) {
                    Ok(req) => json_response(200, &save_lifecycle_snapshot_file(&runtime, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/lifecycle/snapshot/load") => {
                match parse_json::<LifecycleSnapshotFileRequest>(&request.body) {
                    Ok(req) => json_response(200, &load_lifecycle_snapshot_file(&runtime, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/topology/validate") | ("POST", "/ServerService/ValidateTopology") => {
                match parse_json::<ServerTopologyValidationRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &validate_node_topology_from_meta(
                            &runtime,
                            &meta_addr,
                            &advertised_addr,
                            req,
                        ),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("GET", "/server/dirty_objects") => json_response(200, &runtime.dirty_objects()),
            ("GET", "/server/queued_shard_workers") => {
                json_response(200, &runtime.queued_shard_worker_infos())
            }
            ("GET", path) if path.starts_with("/server/storage/slots/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/slots/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.slot_storage_summaries(shard_id))
            }
            ("GET", path) if path.starts_with("/server/storage/dumps/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/dumps/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.list_slot_dump_manifests(shard_id))
            }
            ("GET", path) if path.starts_with("/server/storage/recovery_boundary/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/recovery_boundary/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.storage_recovery_boundary_report(shard_id))
            }
            ("GET", path) if path.starts_with("/server/storage/readiness/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/readiness/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &runtime.storage_production_readiness_report(shard_id))
            }
            ("GET", path) if path.starts_with("/server/storage/cache/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/cache/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.storage_cache_inspection_report(shard_id))
            }
            ("POST", "/server/storage/cache/invalidate_slot") => {
                match parse_json::<StorageCacheInvalidateSlotRequest>(&request.body) {
                    Ok(req) => match engine.invalidate_storage_cache_slot(req) {
                        Ok(report) => json_response(200, &report),
                        Err(status) => json_response(500, &status),
                    },
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/storage/readiness") => {
                match parse_json::<StorageProductionReadinessRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &runtime.storage_production_readiness_report_with_policy(
                            req.shard_id,
                            req.policy,
                        ),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/storage/lifecycle/plan") => {
                match parse_json::<StorageLifecycleRequest>(&request.body) {
                    Ok(req) => json_response(200, &runtime.storage_lifecycle_plan(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/storage/lifecycle/apply") => {
                match parse_json::<StorageLifecycleRequest>(&request.body) {
                    Ok(req) => json_response(200, &runtime.apply_storage_lifecycle(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/storage/manager/cycle") => {
                match parse_json::<StorageManagerCycleRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &runtime.submit_storage_manager_cycle(req, RequestController::default()),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/server/storage/dumps/install") => {
                match parse_json::<SlotDumpManifest>(&request.body) {
                    Ok(manifest) => match engine.install_slot_dump_manifest(&manifest) {
                        Ok(()) => json_response(200, &Status::ok()),
                        Err(status) => json_response(409, &status),
                    },
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("GET", "/server/replica_replay_status") => {
                json_response(200, &replica_replay_loop.status())
            }
            ("POST", "/heartbeat") => json_response(
                200,
                &send_heartbeat(
                    &engine,
                    &runtime,
                    &meta_addr,
                    &advertised_addr,
                    &binary_version,
                ),
            ),
            ("GET", path) if path.starts_with("/jobs/") => {
                let job_id = path
                    .trim_start_matches("/jobs/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &runtime.job_status(job_id))
            }
            ("POST", path) if path.starts_with("/jobs/") && path.ends_with("/cancel") => {
                let job_id = path
                    .trim_start_matches("/jobs/")
                    .trim_end_matches("/cancel")
                    .trim_end_matches('/')
                    .parse()
                    .unwrap_or_default();
                json_response(200, &runtime.cancel_job(job_id))
            }
            ("GET", path) if path.starts_with("/server/shard_worker/") => {
                let shard_id = path
                    .trim_start_matches("/server/shard_worker/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &runtime.shard_worker_info(shard_id))
            }
            ("POST", "/load") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &runtime.load_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/reload") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &runtime.reload_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/unload") => match parse_json::<UnloadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &runtime.unload_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/async_load") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &runtime.submit_load(req, RequestController::default()))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/async_reload") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_reload(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/async_unload") => match parse_json::<UnloadShardRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_unload(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/execute") => match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => {
                    if let Some(raft_state) = &raft_state {
                        match runtime.validate_foreground_write_allowed(
                            req.shard_id,
                            std::slice::from_ref(&req.command),
                        ) {
                            Ok(()) => json_response(200, &execute_via_server_raft(raft_state, req)),
                            Err(status) => json_response(
                                200,
                                &ExecuteResponse {
                                    status,
                                    response: temporalstore_rust::CommandResponse::Empty,
                                },
                            ),
                        }
                    } else {
                        json_response(200, &runtime.execute(req))
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
                    Ok(req) => json_response(200, &runtime.execute_checked(req)),
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
                Ok(req) => json_response(200, &runtime.batch_execute(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            ("POST", "/execute_replicated") => {
                match parse_json::<ReplicatedExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &engine.execute_replicated(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/batch_execute_replicated") => {
                match parse_json::<ReplicatedBatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &engine.batch_execute_replicated(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/ingest/batch") => ingest_batch_route(&engine, &request.body),
            ("GET", "/ingest/state") => json_response(200, &engine.ingestion_state_report()),
            ("POST", "/context/extract") => {
                match parse_json::<ContextExtractRequest>(&request.body) {
                    Ok(req) => json_response(200, &extract_context(&engine, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/context/ingest_extract") => {
                match parse_json::<ContextIngestExtractRequest>(&request.body) {
                    Ok(req) => json_response(200, &ingest_extract_context(&engine, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/context/retrieve") => {
                match parse_json::<ContextRetrieveRequest>(&request.body) {
                    Ok(req) => json_response(200, &retrieve_context(&engine, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/context/inject") => {
                match parse_json::<ContextInjectRequest>(&request.body) {
                    Ok(req) => json_response(200, &inject_context(&engine, req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("GET", "/context/workflow/state") => {
                json_response(200, &context_workflow_state_report())
            }
            ("GET", "/context/manage") => json_response(200, &context_pipeline_manage_report()),
            ("GET", "/context/model/providers") => {
                json_response(200, &default_context_model_providers())
            }
            ("POST", "/context/model/provider") => {
                match parse_json::<ContextModelProviderConfig>(&request.body) {
                    Ok(req) => json_response(200, &req),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/batch_execute_checked") => {
                match parse_json::<CheckedBatchExecuteRequest>(&request.body) {
                    Ok(req) => json_response(200, &runtime.batch_execute_checked(req)),
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
            ("POST", "/storage_manager/cycle") => {
                match parse_json::<StorageManagerCycleRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &runtime.submit_storage_manager_cycle(req, RequestController::default()),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
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

fn handle_ping_route(request: &HttpRequest) -> Option<(u16, Vec<u8>)> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET" | "POST", "/ping" | "/ServerService/Ping") => {
            Some(json_response(200, &Status::ok()))
        }
        _ => None,
    }
}

fn handle_readiness_route(request: &HttpRequest) -> Option<(u16, Vec<u8>)> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/readiness") | ("GET", "/cpp_parity") => {
            Some(json_response(200, &production_readiness_report()))
        }
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct ShardIdRouteRequest {
    shard_id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApplyDataRaftLogRouteRequest {
    partition_id: u64,
    raft_index: u64,
    committed_log: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ApplyDataRaftLogRouteResponse {
    status: Status,
    applied_raft_index: u64,
    applied_oplog_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LifecycleSnapshotFileRequest {
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct LifecycleSnapshotFileResponse {
    status: Status,
    #[serde(default)]
    snapshot: Option<DataNodeLifecycleSnapshot>,
}

fn save_lifecycle_snapshot_file(
    runtime: &DataNodeRuntime,
    request: LifecycleSnapshotFileRequest,
) -> LifecycleSnapshotFileResponse {
    let snapshot = runtime.lifecycle_snapshot();
    if let Some(parent) = request.path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(err) = fs::create_dir_all(parent) {
                return LifecycleSnapshotFileResponse {
                    status: Status::error("lifecycle_snapshot_io", err.to_string()),
                    snapshot: None,
                };
            }
        }
    }
    let bytes = match serde_json::to_vec_pretty(&snapshot) {
        Ok(bytes) => bytes,
        Err(err) => {
            return LifecycleSnapshotFileResponse {
                status: Status::error("bad_lifecycle_snapshot", err.to_string()),
                snapshot: None,
            };
        }
    };
    let tmp_path = request.path.with_extension("tmp");
    if let Err(err) = fs::write(&tmp_path, bytes).and_then(|_| fs::rename(&tmp_path, &request.path))
    {
        let _ = fs::remove_file(&tmp_path);
        return LifecycleSnapshotFileResponse {
            status: Status::error("lifecycle_snapshot_io", err.to_string()),
            snapshot: None,
        };
    }
    LifecycleSnapshotFileResponse {
        status: Status::ok(),
        snapshot: Some(snapshot),
    }
}

fn load_lifecycle_snapshot_file(
    runtime: &DataNodeRuntime,
    request: LifecycleSnapshotFileRequest,
) -> LifecycleSnapshotFileResponse {
    let bytes = match fs::read(&request.path) {
        Ok(bytes) => bytes,
        Err(err) => {
            return LifecycleSnapshotFileResponse {
                status: Status::error("lifecycle_snapshot_io", err.to_string()),
                snapshot: None,
            };
        }
    };
    let snapshot = match serde_json::from_slice::<DataNodeLifecycleSnapshot>(&bytes) {
        Ok(snapshot) => snapshot,
        Err(err) => {
            return LifecycleSnapshotFileResponse {
                status: Status::error("bad_lifecycle_snapshot", err.to_string()),
                snapshot: None,
            };
        }
    };
    let status = runtime.restore_lifecycle_snapshot(snapshot.clone());
    LifecycleSnapshotFileResponse {
        snapshot: status.ok.then_some(snapshot),
        status,
    }
}

fn ingest_batch_route(engine: &TemporalEngine, body: &[u8]) -> (u16, Vec<u8>) {
    match parse_json::<IngestionBatchRequest>(body) {
        Ok(req) => json_response(200, &engine.ingest_batch(req)),
        Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
    }
}

fn handle_cpp_server_service_route(
    request: &HttpRequest,
    engine: &TemporalEngine,
    runtime: &DataNodeRuntime,
    raft_state: Option<&ServerRaftState>,
    meta_addr: &str,
    advertised_addr: &str,
    data_raft_appliers: &Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>>,
) -> Option<(u16, Vec<u8>)> {
    let response = match (request.method.as_str(), request.path.as_str()) {
        ("POST", "/ServerService/Load") => match parse_json::<LoadShardRequest>(&request.body) {
            Ok(req) => json_response(200, &runtime.load_shard_with(req)),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/ServerService/Reload") => match parse_json::<LoadShardRequest>(&request.body) {
            Ok(req) => json_response(200, &runtime.reload_shard_with(req)),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/ServerService/Unload") => {
            match parse_json::<UnloadShardRequest>(&request.body) {
                Ok(req) => json_response(200, &runtime.unload_shard_with(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/AsyncLoad") => {
            match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &runtime.submit_load(req, RequestController::default()))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/AsyncReload") => {
            match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_reload(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/AsyncUnload") => {
            match parse_json::<UnloadShardRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.submit_unload(req, RequestController::default()),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/ExecuteCmd") => {
            match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => {
                    if let Some(raft_state) = raft_state {
                        match runtime.validate_foreground_write_allowed(
                            req.shard_id,
                            std::slice::from_ref(&req.command),
                        ) {
                            Ok(()) => json_response(200, &execute_via_server_raft(raft_state, req)),
                            Err(status) => json_response(
                                200,
                                &ExecuteResponse {
                                    status,
                                    response: temporalstore_rust::CommandResponse::Empty,
                                },
                            ),
                        }
                    } else {
                        json_response(200, &runtime.execute(req))
                    }
                }
                Err(err) => json_response(
                    400,
                    &ExecuteResponse {
                        status: Status::error("bad_request", err.to_string()),
                        response: temporalstore_rust::CommandResponse::Empty,
                    },
                ),
            }
        }
        ("POST", "/ServerService/BatchExecuteCmd") => {
            match parse_json::<BatchExecuteRequest>(&request.body) {
                Ok(req) => json_response(200, &runtime.batch_execute(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/IngestBatch") | ("POST", "/IngestionService/IngestBatch") => {
            ingest_batch_route(engine, &request.body)
        }
        ("GET", "/IngestionService/GetState")
        | ("POST", "/IngestionService/GetState")
        | ("GET", "/ServerService/GetIngestionState")
        | ("POST", "/ServerService/GetIngestionState") => {
            json_response(200, &engine.ingestion_state_report())
        }
        ("POST", "/ServerService/ApplyDataRaftLog") => {
            match parse_json::<ApplyDataRaftLogRouteRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &apply_data_raft_log_route(engine, data_raft_appliers, req),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/SetConfig") => match parse_json::<SetConfigRequest>(&request.body)
        {
            Ok(req) => json_response(200, &engine.set_config(req)),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/ServerService/GetConfig") => {
            match parse_json::<ShardIdRouteRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.get_config(req.shard_id)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/GetInfo") => {
            match parse_json::<ShardIdRouteRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.get_info(req.shard_id)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/GetStats") => {
            match parse_json::<ShardIdRouteRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.get_stats(req.shard_id)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/GetStorageReadiness") => {
            match parse_json::<StorageProductionReadinessRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime
                        .storage_production_readiness_report_with_policy(req.shard_id, req.policy),
                ),
                Err(_) => match parse_json::<ShardIdRouteRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &runtime.storage_production_readiness_report(req.shard_id),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                },
            }
        }
        ("POST", "/ServerService/GetStorageCache") => {
            match parse_json::<ShardIdRouteRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &engine.storage_cache_inspection_report(req.shard_id))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/InvalidateStorageCacheSlot") => {
            match parse_json::<StorageCacheInvalidateSlotRequest>(&request.body) {
                Ok(req) => match engine.invalidate_storage_cache_slot(req) {
                    Ok(report) => json_response(200, &report),
                    Err(status) => json_response(500, &status),
                },
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/ReadPartitionStream") => {
            match parse_json::<StreamReadRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.read_stream(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/ScanPartitionStream") => {
            match parse_json::<ScanStreamRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.scan_stream(req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/UpdateMembership") => {
            match parse_json::<MembershipUpdateRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &update_membership_with_finish_callback(
                        engine,
                        meta_addr,
                        advertised_addr,
                        req,
                    ),
                ),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("GET", "/ServerService/GetRuntimeStats") => json_response(200, &runtime.stats()),
        ("GET", "/ServerService/Preflight") => json_response(200, &runtime.preflight_report()),
        ("GET", "/ServerService/GetLifecycle") => json_response(200, &runtime.lifecycle_report()),
        ("GET", "/ServerService/GetLifecyclePersistence") => {
            json_response(200, &runtime.lifecycle_persistence_report())
        }
        ("GET", "/ServerService/ListLifecycleTokens") => {
            json_response(200, &runtime.lifecycle_tokens())
        }
        ("GET", "/ServerService/GetLifecycleSnapshot") => {
            json_response(200, &runtime.lifecycle_snapshot())
        }
        ("POST", "/ServerService/RequireLifecycleToken") => {
            match parse_json::<SchedulerLifecycleToken>(&request.body) {
                Ok(token) => {
                    runtime.require_lifecycle_token(token);
                    json_response(200, &Status::ok())
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/RestoreLifecycleSnapshot") => {
            match parse_json::<DataNodeLifecycleSnapshot>(&request.body) {
                Ok(snapshot) => json_response(200, &runtime.restore_lifecycle_snapshot(snapshot)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/SaveLifecycleSnapshot") => {
            match parse_json::<LifecycleSnapshotFileRequest>(&request.body) {
                Ok(req) => json_response(200, &save_lifecycle_snapshot_file(runtime, req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/ServerService/LoadLifecycleSnapshot") => {
            match parse_json::<LifecycleSnapshotFileRequest>(&request.body) {
                Ok(req) => json_response(200, &load_lifecycle_snapshot_file(runtime, req)),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("GET", "/ServerService/ListDirtyObjects") => json_response(200, &runtime.dirty_objects()),
        ("GET", "/ServerService/ListQueuedShardWorkers") => {
            json_response(200, &runtime.queued_shard_worker_infos())
        }
        _ => return None,
    };
    Some(response)
}

fn apply_data_raft_log_route(
    engine: &TemporalEngine,
    data_raft_appliers: &Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>>,
    request: ApplyDataRaftLogRouteRequest,
) -> ApplyDataRaftLogRouteResponse {
    let mut appliers = data_raft_appliers
        .lock()
        .expect("data raft applier lock poisoned");
    let applier = appliers
        .entry(request.partition_id)
        .or_insert_with(|| DataRaftCommittedLogApplier::new(request.partition_id));
    match applier.apply(request.raft_index, &request.committed_log, engine) {
        Ok(_) => ApplyDataRaftLogRouteResponse {
            status: Status::ok(),
            applied_raft_index: applier.applied_raft_index(),
            applied_oplog_sequence: applier.applied_oplog_sequence(),
        },
        Err(err) => ApplyDataRaftLogRouteResponse {
            status: Status::error("invalid_data_raft_log", err.to_string()),
            applied_raft_index: applier.applied_raft_index(),
            applied_oplog_sequence: applier.applied_oplog_sequence(),
        },
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReplicaReplayLoopStatus {
    enabled: bool,
    shard_id: u64,
    configured_primary_addr: String,
    last_primary_addr: Option<String>,
    primary_route_change_total: u64,
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

fn env_i32(name: &str, default: i32) -> i32 {
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

fn block_store_options_from_env() -> BlockStoreOptions {
    let defaults = BlockStoreOptions::default();
    BlockStoreOptions {
        compression_enabled: env_bool(
            "TS_PAGE_STORE_COMPRESSION_ENABLED",
            defaults.compression_enabled,
        ),
        compression_min_bytes: env_usize(
            "TS_PAGE_STORE_COMPRESSION_MIN_BYTES",
            defaults.compression_min_bytes,
        ),
        compression_level: env_i32(
            "TS_PAGE_STORE_COMPRESSION_LEVEL",
            defaults.compression_level,
        ),
    }
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

#[derive(Debug, Deserialize)]
struct RaftAdminWaitAppliedRequest {
    node_id: RaftNodeId,
    index: u64,
    timeout_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminBootstrapExternalSnapshotRequest {
    target_id: RaftNodeId,
    snapshot: ShardSnapshotRef,
    object_root: String,
    local_root: String,
    #[serde(default = "default_snapshot_cluster_id")]
    cluster_id: String,
    #[serde(default = "default_snapshot_bucket")]
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotRequest {
    object_root: String,
    local_root: String,
    #[serde(default = "default_snapshot_cluster_id")]
    cluster_id: String,
    #[serde(default = "default_snapshot_bucket")]
    bucket: String,
}

#[derive(Debug, Deserialize)]
struct RaftApplyHealthRequest {
    #[serde(default)]
    max_allowed_apply_lag: u64,
}

#[derive(Debug, Deserialize)]
struct RaftMembershipApplyRequest {
    voters: Vec<RaftNodeId>,
}

#[derive(Debug, Deserialize)]
struct RaftControlNodeRequest {
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

#[derive(Debug, Deserialize, Serialize)]
struct RaftMembershipApplyResponse {
    status: Status,
    report: Option<RaftMembershipChangeReport>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftControlMembershipResponse {
    status: Status,
    shard_id: u64,
    leader_id: RaftNodeId,
    voters: Vec<RaftNodeId>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftControlSnapshotResponse {
    status: Status,
    report: Option<RaftSnapshotTriggerReport>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftControlReadIndexResponse {
    status: Status,
    response: Option<ReadIndexResponse>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminBootstrapExternalSnapshotResponse {
    status: Status,
    plan: Option<RaftReplicaBootstrapPlan>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotResponse {
    status: Status,
    report: Option<RaftSnapshotPublishReport>,
}

fn default_snapshot_cluster_id() -> String {
    "cluster-a".to_string()
}

fn default_snapshot_bucket() -> String {
    "test".to_string()
}

fn uri_scheme(uri: &str) -> String {
    uri.split_once("://")
        .map(|(scheme, _)| scheme.to_string())
        .unwrap_or_else(|| "file".to_string())
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
    // Reads TS_RAFT_AUTH_TOKEN plus TS_RAFT_SECURITY_MODE/cert paths for process auth.
    let security = production_raft_security_from_env(
        "local-raft-token",
        env_bool("TS_RAFT_ALLOW_PLAINTEXT", true),
    );
    let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::TemporalRaft,
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
        security: security.security,
        heartbeat_interval_ms: env_u64("TS_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_RAFT_ELECTION_TICK_MS", 50),
        max_catchup_entries_per_heartbeat: env_u64(
            "TS_RAFT_MAX_CATCHUP_ENTRIES_PER_HEARTBEAT",
            256,
        ),
        allow_plaintext_for_local_chaos: security.allow_plaintext_for_local_chaos,
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
        ("GET", "/raft/control/byteraft_runtime_admin")
        | ("POST", "/raft/control/byteraft_runtime_admin") => json_response(
            200,
            &state.runtime.cluster().byteraft_runtime_admin_report(),
        ),
        ("GET", "/raft/control/byteraft_local_status")
        | ("POST", "/raft/control/byteraft_local_status") => {
            json_response(200, &state.runtime.cluster().byteraft_local_status_report())
        }
        ("POST", "/raft/apply_health") => match parse_json::<RaftApplyHealthRequest>(&request.body)
        {
            Ok(req) => json_response(
                200,
                &state.runtime.local_apply_health(req.max_allowed_apply_lag),
            ),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/raft/membership/apply") => {
            match parse_json::<RaftMembershipApplyRequest>(&request.body) {
                Ok(req) => {
                    let response = match state.runtime.apply_membership_change_safely(req.voters) {
                        Ok(report) => RaftMembershipApplyResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftMembershipApplyResponse {
                            status: Status::error("raft_error", err.to_string()),
                            report: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("GET", "/raft/control/list_membership") | ("POST", "/raft/control/list_membership") => {
            json_response(200, &server_raft_membership_response(state))
        }
        ("POST", "/raft/control/add_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let mut voters = state.runtime.cluster().membership().voters;
                    if !voters.contains(&req.node_id) {
                        voters.push(req.node_id);
                        voters.sort_unstable();
                    }
                    json_response(200, &server_raft_apply_membership_response(state, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/remove_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let voters = state
                        .runtime
                        .cluster()
                        .membership()
                        .voters
                        .into_iter()
                        .filter(|node_id| *node_id != req.node_id)
                        .collect::<Vec<_>>();
                    json_response(200, &server_raft_apply_membership_response(state, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/trigger_snapshot") => {
            let response = match state.runtime.cluster().maybe_trigger_snapshot() {
                Ok(report) => RaftControlSnapshotResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => RaftControlSnapshotResponse {
                    status: Status::error("raft_error", err.to_string()),
                    report: None,
                },
            };
            json_response(200, &response)
        }
        ("POST", "/raft/control/read_index") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let response = match state.runtime.cluster().read_index(req.node_id) {
                        Ok(read_index) => RaftControlReadIndexResponse {
                            status: Status::ok(),
                            response: Some(read_index),
                        },
                        Err(err) => RaftControlReadIndexResponse {
                            status: Status::error("raft_error", err.to_string()),
                            response: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/transfer_leader") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .cluster()
                        .transfer_leader(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/accept_leadership") => {
            match parse_json::<RaftControlLeadershipRequest>(&request.body) {
                Ok(req) => {
                    let status = if req.node_id != state.local_node_id {
                        Status::error(
                            "bad_request",
                            format!(
                                "node {} cannot accept leadership for node {}",
                                state.local_node_id, req.node_id
                            ),
                        )
                    } else {
                        state
                            .runtime
                            .cluster()
                            .catch_up(req.node_id)
                            .and_then(|_| state.runtime.cluster().transfer_leader(req.node_id))
                            .map(|_| Status::ok())
                            .unwrap_or_else(|err| Status::error("raft_error", err.to_string()))
                    };
                    json_response(200, &status)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
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
        ("POST", "/raft/admin/bootstrap_external_snapshot") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminBootstrapExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        uri_scheme(&req.snapshot.uri),
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    state
                                        .runtime
                                        .cluster()
                                        .bootstrap_replica_from_external_snapshot(
                                            req.target_id,
                                            &snapshot_store,
                                            &req.snapshot,
                                            PathBuf::from(&req.local_root)
                                                .join(format!("restore-node-{}", req.target_id)),
                                        ),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(plan) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::ok(),
                            plan: Some(plan),
                        },
                        Err(err) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            plan: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/publish_external_snapshot") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminPublishExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        "s3",
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    state
                                        .runtime
                                        .cluster()
                                        .publish_leader_snapshot_to_store(&snapshot_store),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(report) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            report: None,
                        },
                    };
                    json_response(200, &response)
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
        ("POST", "/raft/admin/wait_applied") => {
            if !state.local_admin_enabled {
                return Some(json_response(
                    403,
                    &Status::error("forbidden", "local admin disabled"),
                ));
            }
            match parse_json::<RaftAdminWaitAppliedRequest>(&request.body) {
                Ok(req) => {
                    let status = state
                        .runtime
                        .wait_for_applied_index(req.node_id, req.index, req.timeout_ms)
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
            handle_authenticated_raft_http(
                &state.runtime.cluster(),
                HttpRequest {
                    method: request.method.clone(),
                    path: request.path.clone(),
                    body: request.body.clone(),
                },
                state.runtime.peer_auth_token().unwrap_or_default(),
            )
        }
        _ => return None,
    };
    Some(response)
}

fn server_raft_membership_response(state: &ServerRaftState) -> RaftControlMembershipResponse {
    let membership = state.runtime.cluster().membership();
    RaftControlMembershipResponse {
        status: Status::ok(),
        shard_id: membership.shard_id,
        leader_id: membership.leader_id,
        voters: membership.voters,
    }
}

fn server_raft_apply_membership_response(
    state: &ServerRaftState,
    voters: Vec<RaftNodeId>,
) -> RaftMembershipApplyResponse {
    match state.runtime.apply_membership_change_safely(voters) {
        Ok(report) => RaftMembershipApplyResponse {
            status: Status::ok(),
            report: Some(report),
        },
        Err(err) => RaftMembershipApplyResponse {
            status: Status::error("raft_error", err.to_string()),
            report: None,
        },
    }
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
                    scheduler_task_id: None,
                    scheduler_generation: None,
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
        primary_route_change_total: 0,
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
                let route_changed = status
                    .last_primary_addr
                    .as_deref()
                    .map(|previous| previous != primary_addr)
                    .unwrap_or(false);
                if route_changed {
                    status.primary_route_change_total += 1;
                    status.consecutive_failures = 0;
                    status.last_error = None;
                }
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
        ("storage_manager", stats.storage_manager_runs),
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
    let lifecycle_persistence = runtime.lifecycle_persistence_report();
    out.push_str("# HELP temporalstore_data_node_lifecycle_snapshot_enabled Whether automatic lifecycle snapshot persistence is enabled.\n");
    out.push_str("# TYPE temporalstore_data_node_lifecycle_snapshot_enabled gauge\n");
    out.push_str("temporalstore_data_node_lifecycle_snapshot_enabled ");
    out.push_str(if lifecycle_persistence.enabled {
        "1"
    } else {
        "0"
    });
    out.push('\n');
    out.push_str("# HELP temporalstore_data_node_lifecycle_snapshot_events_total Lifecycle snapshot persistence events.\n");
    out.push_str("# TYPE temporalstore_data_node_lifecycle_snapshot_events_total counter\n");
    for (kind, value) in [
        (
            "restore_success",
            lifecycle_persistence.restore_success_total,
        ),
        (
            "restore_failure",
            lifecycle_persistence.restore_failure_total,
        ),
        (
            "persist_success",
            lifecycle_persistence.persist_success_total,
        ),
        (
            "persist_failure",
            lifecycle_persistence.persist_failure_total,
        ),
    ] {
        out.push_str("temporalstore_data_node_lifecycle_snapshot_events_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    if let Some(report) = runtime.last_storage_manager_cycle_report() {
        append_storage_manager_cycle_metrics(out, &report);
    }
}

fn append_storage_manager_cycle_metrics(out: &mut String, report: &StorageManagerCycleReport) {
    out.push_str("# HELP temporalstore_storage_manager_pressure Last StorageManager pressure snapshot by shard and signal.\n");
    out.push_str("# TYPE temporalstore_storage_manager_pressure gauge\n");
    let pressure = &report.pressure_snapshot;
    for (signal, value) in [
        ("dirty_slots", pressure.dirty_slot_count as u64),
        ("undumped_wal_records", pressure.undumped_wal_records),
        ("wal_bytes", pressure.wal_bytes),
        ("index_log_bytes", pressure.index_log_bytes),
        ("stale_page_bytes", pressure.stale_page_bytes),
        ("live_page_bytes", pressure.live_page_bytes),
        (
            "page_segment_stale_density_basis_points",
            pressure.page_segment_stale_density_basis_points,
        ),
        ("memory_cache_bytes", pressure.memory_cache_bytes),
        ("disk_cache_bytes", pressure.disk_cache_bytes),
        (
            "memory_cache_pressure_score",
            pressure.memory_cache_pressure_score,
        ),
        (
            "expired_slot_object_scan_debt",
            pressure.expired_slot_object_scan_debt as u64,
        ),
        (
            "delayed_destroy_segments",
            pressure.delayed_destroy_segment_count as u64,
        ),
        ("delayed_destroy_bytes", pressure.delayed_destroy_bytes),
        (
            "follower_cursor_retention_blockers",
            pressure.follower_cursor_retention_blockers as u64,
        ),
        (
            "raft_snapshot_retention_blockers",
            pressure.raft_snapshot_retention_blockers as u64,
        ),
        (
            "compaction_debt_models",
            pressure.compaction_debt_model_count as u64,
        ),
        ("compaction_debt_score", pressure.compaction_debt_score),
        ("total_pressure_score", pressure.total_pressure_score),
    ] {
        out.push_str("temporalstore_storage_manager_pressure{shard_id=\"");
        out.push_str(&report.shard_id.to_string());
        out.push_str("\",signal=\"");
        out.push_str(signal);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }

    out.push_str("# HELP temporalstore_storage_manager_phase_enabled Last StorageManager phase enabled state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_enabled gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_applied Last StorageManager phase applied state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_applied gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_skipped Last StorageManager phase skipped state.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_skipped gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_duration_ms Last StorageManager phase duration.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_duration_ms gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_pressure Last StorageManager phase pressure counters by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_pressure gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_work Last StorageManager phase work counters by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_work gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_bytes Last StorageManager phase bytes by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_bytes gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_floors Last StorageManager phase retention floors by kind.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_floors gauge\n");
    out.push_str("# HELP temporalstore_storage_manager_phase_errors Last StorageManager phase error count.\n");
    out.push_str("# TYPE temporalstore_storage_manager_phase_errors gauge\n");

    for stage in &report.stages {
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_enabled",
            report.shard_id,
            &stage.stage,
            stage.enabled,
        );
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_applied",
            report.shard_id,
            &stage.stage,
            stage.applied,
        );
        append_storage_manager_phase_bool(
            out,
            "temporalstore_storage_manager_phase_skipped",
            report.shard_id,
            &stage.stage,
            stage.skipped,
        );
        append_storage_manager_phase_value(
            out,
            "temporalstore_storage_manager_phase_duration_ms",
            report.shard_id,
            &stage.stage,
            stage.duration_ms,
        );
        for (kind, value) in [
            ("score", stage.pressure_score),
            ("threshold", stage.pressure_threshold),
            ("before", stage.pressure_before),
            ("after", stage.pressure_after),
            ("retention_blockers", stage.retention_blockers as u64),
            ("eviction_before", stage.eviction_pressure_before),
            ("eviction_after", stage.eviction_pressure_after),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_pressure",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("candidates", stage.candidate_count as u64),
            ("skipped", stage.skipped_count as u64),
            ("selected_slots", stage.selected_slots.len() as u64),
            (
                "selected_page_segments",
                stage.selected_page_segment_ids.len() as u64,
            ),
            ("dirty_slots", stage.dirty_slot_count as u64),
            ("dumped_slots", stage.dumped_slot_count as u64),
            ("wal_records_removed", stage.wal_records_removed as u64),
            (
                "index_log_records_removed",
                stage.index_log_records_removed as u64,
            ),
            (
                "expired_records_removed",
                stage.expired_records_removed as u64,
            ),
            ("cache_entries_removed", stage.cache_entries_removed as u64),
            ("dropped_objects", stage.dropped_object_count as u64),
            (
                "page_segments_reclaimed",
                stage.page_segments_reclaimed as u64,
            ),
            ("pages_compacted", stage.pages_compacted as u64),
            ("rewritten_page_refs", stage.rewritten_page_refs as u64),
            ("manifest_pruned", stage.manifest_pruned_count as u64),
            ("metrics_slots", stage.metrics_slot_count as u64),
            ("metrics_page_refs", stage.metrics_page_ref_count),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_work",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("before", stage.before_bytes),
            ("after", stage.after_bytes),
            ("live", stage.live_bytes),
            ("stale", stage.stale_bytes),
            ("reclaimed", stage.bytes_reclaimed),
            ("page_reclaimed", stage.page_bytes_reclaimed),
            ("cache_disk_removed", stage.cache_disk_bytes_removed),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_bytes",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        for (kind, value) in [
            ("wal", stage.wal_floor_sequence),
            ("index_log", stage.index_log_floor_sequence),
            ("retain_from_wal", stage.retain_from_wal_sequence),
            (
                "retain_from_index_log",
                stage.retain_from_index_log_sequence,
            ),
        ] {
            append_storage_manager_phase_kind_value(
                out,
                "temporalstore_storage_manager_phase_floors",
                report.shard_id,
                &stage.stage,
                kind,
                value,
            );
        }
        append_storage_manager_phase_value(
            out,
            "temporalstore_storage_manager_phase_errors",
            report.shard_id,
            &stage.stage,
            stage.errors.len() as u64,
        );
    }
}

fn append_storage_manager_phase_bool(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    value: bool,
) {
    append_storage_manager_phase_value(out, name, shard_id, phase, u64::from(value));
}

fn append_storage_manager_phase_value(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    value: u64,
) {
    out.push_str(name);
    out.push_str("{shard_id=\"");
    out.push_str(&shard_id.to_string());
    out.push_str("\",phase=\"");
    out.push_str(phase);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_storage_manager_phase_kind_value(
    out: &mut String,
    name: &str,
    shard_id: u64,
    phase: &str,
    kind: &str,
    value: u64,
) {
    out.push_str(name);
    out.push_str("{shard_id=\"");
    out.push_str(&shard_id.to_string());
    out.push_str("\",phase=\"");
    out.push_str(phase);
    out.push_str("\",kind=\"");
    out.push_str(kind);
    out.push_str("\"} ");
    out.push_str(&value.to_string());
    out.push('\n');
}

fn append_ingestion_metrics(out: &mut String, engine: &TemporalEngine) {
    let report = engine.ingestion_state_report();
    out.push_str(
        "# HELP temporalstore_ingestion_records_total Ingestion record counters by kind.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_records_total counter\n");
    for (kind, value) in [
        ("accepted", report.stats.accepted_total),
        ("failed", report.stats.failed_total),
        ("duplicate", report.stats.duplicate_total),
        ("dead_letter", report.stats.dead_letter_total),
        ("kafka_committed", report.stats.kafka_committed_total),
    ] {
        out.push_str("temporalstore_ingestion_records_total{kind=\"");
        out.push_str(kind);
        out.push_str("\"} ");
        out.push_str(&value.to_string());
        out.push('\n');
    }
    out.push_str(
        "# HELP temporalstore_ingestion_kafka_max_lag Max observed Kafka ingestion lag.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_kafka_max_lag gauge\n");
    out.push_str("temporalstore_ingestion_kafka_max_lag ");
    out.push_str(&report.stats.max_kafka_lag.max(0).to_string());
    out.push('\n');
    out.push_str(
        "# HELP temporalstore_ingestion_kafka_ledgers Current Kafka offset ledger entries.\n",
    );
    out.push_str("# TYPE temporalstore_ingestion_kafka_ledgers gauge\n");
    out.push_str("temporalstore_ingestion_kafka_ledgers ");
    out.push_str(&report.kafka_offsets.len().to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_ingestion_dead_letters Current persisted ingestion dead-letter records.\n");
    out.push_str("# TYPE temporalstore_ingestion_dead_letters gauge\n");
    out.push_str("temporalstore_ingestion_dead_letters ");
    out.push_str(&report.dead_letters.len().to_string());
    out.push('\n');
    out.push_str("# HELP temporalstore_ingestion_flink_checkpoints Current Flink checkpoint states by status.\n");
    out.push_str("# TYPE temporalstore_ingestion_flink_checkpoints gauge\n");
    for status in [
        FlinkCheckpointStatus::Precommitted,
        FlinkCheckpointStatus::Committed,
        FlinkCheckpointStatus::Aborted,
    ] {
        let label = match status {
            FlinkCheckpointStatus::Precommitted => "precommitted",
            FlinkCheckpointStatus::Committed => "committed",
            FlinkCheckpointStatus::Aborted => "aborted",
        };
        let count = report
            .flink_checkpoints
            .iter()
            .filter(|checkpoint| checkpoint.status == status)
            .count();
        out.push_str("temporalstore_ingestion_flink_checkpoints{status=\"");
        out.push_str(label);
        out.push_str("\"} ");
        out.push_str(&count.to_string());
        out.push('\n');
    }
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
        ("primary_route_change", status.primary_route_change_total),
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
    runtime: DataNodeRuntime,
    meta_addr: String,
    server_addr: String,
    binary_version: String,
    interval_ms: u64,
) {
    if interval_ms == 0 {
        return;
    }
    std::thread::spawn(move || loop {
        let _ = send_heartbeat(&engine, &runtime, &meta_addr, &server_addr, &binary_version);
        std::thread::sleep(std::time::Duration::from_millis(interval_ms));
    });
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

fn validate_node_topology_from_meta(
    runtime: &DataNodeRuntime,
    meta_addr: &str,
    advertised_addr: &str,
    request: ServerTopologyValidationRequest,
) -> ServerTopologyValidationResponse {
    let server_addr = if request.server_addr.is_empty() {
        advertised_addr.to_string()
    } else {
        request.server_addr
    };
    let mut table_names = if request.table_names.is_empty() {
        runtime
            .shard_serving_states()
            .into_iter()
            .filter(|state| state.loaded && !state.table_name.is_empty())
            .map(|state| state.table_name)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        request.table_names
    };
    table_names.sort();
    table_names.dedup();

    let mut topologies = Vec::new();
    let mut fetched_tables = Vec::new();
    let mut fetch_errors = Vec::new();
    for table_name in &table_names {
        let topology = post_json::<_, TableTopologyResponse>(
            meta_addr,
            "/tables/topology",
            &GetTableTopologyRequest {
                namespace: request.namespace.clone(),
                table_name: table_name.clone(),
                old_topology_version: 0,
            },
        );
        match topology {
            Ok(topology) if topology.status.ok => {
                fetched_tables.push(table_name.clone());
                topologies.push(topology);
            }
            Ok(topology) => fetch_errors.push(format!(
                "{table_name}:{}:{}",
                topology.status.code, topology.status.message
            )),
            Err(err) => fetch_errors.push(format!("{table_name}:http_error:{err}")),
        }
    }
    let report = runtime.validate_topology_against_metaserver(&server_addr, &topologies);
    let status = if fetch_errors.is_empty() {
        Status::ok()
    } else {
        Status::error("topology_fetch_failed", fetch_errors.join(","))
    };
    ServerTopologyValidationResponse {
        status,
        report,
        fetched_tables,
        fetch_errors,
    }
}

fn send_heartbeat(
    engine: &TemporalEngine,
    runtime: &DataNodeRuntime,
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
        runtime_load: runtime.server_runtime_load(),
        shard_states: runtime.shard_serving_states(),
    };
    let response =
        post_json::<_, ServerHeartbeatResponse>(meta_addr, "/servers/heartbeat", &request)
            .unwrap_or_else(|err| ServerHeartbeatResponse {
                status: Status::error("heartbeat_failed", err.to_string()),
                forbid_auto_register: false,
                topology_version: 0,
                server_state: String::new(),
            });
    runtime.record_metaserver_heartbeat(&response);
    response
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use temporalstore_rust::http::get_json_with_options;
    use temporalstore_rust::ingestion::{IngestionBatchReport, IngestionRecord, IngestionSource};
    use temporalstore_rust::raft::{serialize_data_raft_log, DataRaftLogCodecEntry};
    use temporalstore_rust::types::{Command, CommandResponse};
    use temporalstore_rust::{
        BatchExecuteResponse, DataNodeTaskKind, DataNodeTaskStatus, LoadShardResponse,
        ProductionReadinessReport, UnloadShardResponse,
    };

    use super::*;

    const TEST_CACHE_BYTES: usize = 1024;

    fn test_engine(root: &Path, role: &str) -> TemporalEngine {
        test_engine_with_cache(root, role, TEST_CACHE_BYTES)
    }

    fn test_engine_with_cache(root: &Path, role: &str, cache_bytes: usize) -> TemporalEngine {
        TemporalEngine::with_local_dirs(
            cache_bytes,
            root.join(format!("{role}-cache")),
            root.join(format!("{role}-pages")),
            root.join(format!("{role}-index")),
        )
    }

    // shared-corpus: storage_manager_metrics_admin_phase_reports
    #[test]
    fn storage_manager_cycle_prometheus_exposes_phase_and_pressure_metrics() {
        let dir = tempdir().unwrap();
        let engine = test_engine(dir.path(), "engine");
        engine.load_shard(73);
        let write = engine.execute(ExecuteRequest {
            shard_id: 73,
            command: Command::StringSet {
                key: "storage-manager:metrics".to_string(),
                value: b"phase-pressure".to_vec(),
            },
        });
        assert!(write.status.ok, "{write:?}");

        let report = engine.run_storage_manager_cycle(StorageManagerCycleRequest {
            shard_id: 73,
            max_dump_slots_per_round: 4,
            warm_cache: true,
            ..StorageManagerCycleRequest::default()
        });
        assert!(report.completed, "{report:?}");

        let mut metrics = String::new();
        append_storage_manager_cycle_metrics(&mut metrics, &report);
        assert!(metrics.contains("# TYPE temporalstore_storage_manager_pressure gauge"));
        assert!(metrics.contains(
            "temporalstore_storage_manager_pressure{shard_id=\"73\",signal=\"dirty_slots\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_pressure{shard_id=\"73\",signal=\"wal_bytes\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_pressure{shard_id=\"73\",signal=\"follower_cursor_retention_blockers\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_phase_enabled{shard_id=\"73\",phase=\"prepare\"} 1"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_phase_work{shard_id=\"73\",phase=\"reclaim_oplog\",kind=\"wal_records_removed\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_phase_pressure{shard_id=\"73\",phase=\"evict\",kind=\"eviction_before\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_phase_floors{shard_id=\"73\",phase=\"index_gc\",kind=\"index_log\"}"
        ));
        assert!(metrics.contains(
            "temporalstore_storage_manager_phase_bytes{shard_id=\"73\",phase=\"reclaim_page\",kind=\"reclaimed\"}"
        ));
    }

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
        let engine = test_engine(engine_dir.path(), "engine");
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
    fn server_topology_validation_fetches_metaserver_partition_map() {
        let engine = TemporalEngine::default();
        assert!(
            engine
                .load_shard_with(LoadShardRequest {
                    shard_id: 9,
                    table_name: "orders".to_string(),
                    shard_uri: "local://orders/9".to_string(),
                    start_routing_slot: 90,
                    end_routing_slot: 99,
                    readonly: false,
                    load_version: 7,
                    local_node_id: Some(1),
                })
                .status
                .ok
        );
        let runtime = DataNodeRuntime::new(
            engine,
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );
        let meta_addr = free_local_addr();
        let bind_addr = meta_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    ("POST", "/tables/topology") => {
                        let req = parse_json::<GetTableTopologyRequest>(&request.body).unwrap();
                        assert_eq!(req.namespace, "prod");
                        assert_eq!(req.table_name, "orders");
                        json_response(
                            200,
                            &TableTopologyResponse {
                                status: Status::ok(),
                                table: Some(temporalstore_rust::meta::TableMetaInfo {
                                    table_id: 1,
                                    namespace: "prod".to_string(),
                                    table_name: "orders".to_string(),
                                    state: temporalstore_rust::meta::MetaEntityState::Normal,
                                    topology_version: 44,
                                    first_shard_id: 9,
                                    shard_count: 1,
                                    replica_count: 1,
                                    use_cpp_partition_ids: false,
                                    partition_version: 0,
                                    serving_options:
                                        temporalstore_rust::meta::TableServingOptions::default(),
                                }),
                                partitions: vec![temporalstore_rust::meta::TablePartition {
                                    shard_id: 9,
                                    start_slot: 90,
                                    end_slot: 99,
                                    primary: Some("server-a".to_string()),
                                    replicas: vec!["server-a".to_string()],
                                    primary_endpoint: None,
                                    replica_endpoints: Vec::new(),
                                }],
                                unchanged: false,
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let response = validate_node_topology_from_meta(
            &runtime,
            &meta_addr,
            "server-a",
            ServerTopologyValidationRequest {
                namespace: "prod".to_string(),
                server_addr: String::new(),
                table_names: Vec::new(),
            },
        );
        assert!(response.status.ok, "{response:?}");
        assert_eq!(response.fetched_tables, vec!["orders"]);
        assert!(response.fetch_errors.is_empty());
        assert!(response.report.validated_against_metaserver);
        assert_eq!(response.report.authoritative_topology_version, 44);
        assert_eq!(response.report.mismatch_count, 0);
        assert_eq!(response.report.loaded_shards, vec![9]);
    }

    #[test]
    fn server_exposes_cpp_parity_readiness_report() {
        for path in ["/readiness", "/cpp_parity"] {
            let (_, body) = handle_readiness_route(&HttpRequest {
                method: "GET".to_string(),
                path: path.to_string(),
                body: Vec::new(),
            })
            .expect("readiness route should match");
            let report: ProductionReadinessReport = serde_json::from_slice(&body).unwrap();
            assert!(report.production_ready);
            assert!(report.cpp_parity_ready);
            assert_eq!(report.missing_count(), 0);
        }
    }

    #[test]
    fn block_store_options_read_compression_policy_env() {
        let keys = [
            "TS_PAGE_STORE_COMPRESSION_ENABLED",
            "TS_PAGE_STORE_COMPRESSION_MIN_BYTES",
            "TS_PAGE_STORE_COMPRESSION_LEVEL",
        ];
        for key in keys {
            std::env::remove_var(key);
        }

        let defaults = block_store_options_from_env();
        assert!(defaults.compression_enabled);

        std::env::set_var("TS_PAGE_STORE_COMPRESSION_ENABLED", "false");
        std::env::set_var("TS_PAGE_STORE_COMPRESSION_MIN_BYTES", "4096");
        std::env::set_var("TS_PAGE_STORE_COMPRESSION_LEVEL", "3");
        let options = block_store_options_from_env();

        assert!(!options.compression_enabled);
        assert_eq!(options.compression_min_bytes, 4096);
        assert_eq!(options.compression_level, 3);

        for key in keys {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn membership_update_posts_finish_callback_to_meta() {
        let engine_dir = tempdir().unwrap();
        let engine = test_engine(engine_dir.path(), "engine");
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
        let primary = test_engine(primary_dir.path(), "primary");
        let follower = test_engine(follower_dir.path(), "follower");
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
        let primary = test_engine(primary_dir.path(), "primary");
        let follower = test_engine(follower_dir.path(), "follower");
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
        let primary = test_engine(primary_dir.path(), "primary");
        let follower = test_engine(follower_dir.path(), "follower");
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
    fn server_background_replica_replay_loop_resets_on_primary_route_change() {
        let primary_dir = tempdir().unwrap();
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let primary = test_engine(primary_dir.path(), "primary");
        let follower = test_engine(follower_dir.path(), "follower");
        primary.load_shard(27);
        primary.execute(ExecuteRequest {
            shard_id: 27,
            command: Command::StringSet {
                key: "route-change-replay".to_string(),
                value: b"new-primary".to_vec(),
            },
        });

        let bad_primary_addr = free_local_addr();
        let routed_primary = Arc::new(Mutex::new(bad_primary_addr));
        let meta_addr = start_dynamic_meta_route_server(27, Arc::clone(&routed_primary));
        let replay_loop = start_replica_replay_loop(
            follower.clone(),
            cursor_dir.path().display().to_string(),
            meta_addr,
            "secondary:17002".to_string(),
            27,
            String::new(),
            10,
            16 * 1024 * 1024,
            120,
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = replay_loop.status();
            if status.failure_total > 0 {
                assert!(status.consecutive_failures > 0, "{status:?}");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "replica replay loop did not observe initial bad primary; status: {:?}",
                status
            );
            thread::sleep(Duration::from_millis(10));
        }

        let good_primary_addr = start_primary_stream_server(primary);
        *routed_primary.lock().unwrap() = good_primary_addr.clone();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let status = replay_loop.status();
            let read = follower.execute(ExecuteRequest {
                shard_id: 27,
                command: Command::StringGet {
                    key: "route-change-replay".to_string(),
                },
            });
            if read.response
                == (CommandResponse::Bytes {
                    value: Some(b"new-primary".to_vec()),
                })
            {
                assert_eq!(
                    status.last_primary_addr.as_deref(),
                    Some(good_primary_addr.as_str())
                );
                assert!(status.primary_route_change_total >= 1, "{status:?}");
                assert_eq!(status.consecutive_failures, 0, "{status:?}");
                return;
            }
            assert!(
                Instant::now() < deadline,
                "replica replay loop did not recover after primary route change; status: {:?}, read: {:?}",
                status,
                read
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn server_background_replica_replay_loop_reports_failures_with_backoff() {
        let follower_dir = tempdir().unwrap();
        let cursor_dir = tempdir().unwrap();
        let follower = test_engine(follower_dir.path(), "follower");

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

    // shared-corpus: cpp_redis_live_storage_smoke_parity_surfaces
    #[test]
    fn server_ping_routes_match_cpp_ping_rpc() {
        for (method, path) in [
            ("GET", "/ping"),
            ("POST", "/ping"),
            ("GET", "/ServerService/Ping"),
            ("POST", "/ServerService/Ping"),
        ] {
            let request = HttpRequest {
                method: method.to_string(),
                path: path.to_string(),
                body: Vec::new(),
            };
            let (code, body) = handle_ping_route(&request).unwrap();
            assert_eq!(code, 200, "{method} {path}");
            let status: Status = serde_json::from_slice(&body).unwrap();
            assert!(status.ok, "{method} {path}: {status:?}");
        }

        let unknown = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Unknown".to_string(),
            body: Vec::new(),
        };
        assert!(handle_ping_route(&unknown).is_none());
    }

    // shared-corpus: control_cpp_server_service_alias_surface
    #[test]
    fn cpp_server_service_aliases_cover_partition_manager_surface() {
        let dir = tempdir().unwrap();
        let engine = test_engine_with_cache(dir.path(), "engine", 1024 * 1024);
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();
        let runtime = DataNodeRuntime::new(engine.clone(), DataNodeRuntimeOptions::default());

        let token = SchedulerLifecycleToken {
            task_id: 99,
            shard_id: 44,
            operation: "load".to_string(),
            load_version: 7,
            generation: 100,
        };
        let require_token = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/RequireLifecycleToken".to_string(),
            body: serde_json::to_vec(&token).unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &require_token,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert!(status.ok, "{status:?}");

        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "GET".to_string(),
                path: "/ServerService/ListLifecycleTokens".to_string(),
                body: Vec::new(),
            },
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let tokens: Vec<SchedulerLifecycleToken> = serde_json::from_slice(&body).unwrap();
        assert_eq!(tokens, vec![token]);

        let load = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Load".to_string(),
            body: serde_json::to_vec(&LoadShardRequest {
                shard_id: 44,
                load_version: 7,
                local_node_id: Some(1),
                shard_uri: "local://cpp-alias/shard-44".to_string(),
                start_routing_slot: 0,
                end_routing_slot: u32::MAX,
                readonly: false,
                table_name: "cpp_alias".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &load,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: LoadShardResponse = serde_json::from_slice(&body).unwrap();
        assert!(status.status.ok, "{status:?}");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(99));
        assert_eq!(lifecycle.transitions[0].scheduler_generation, Some(100));

        let stale_reload = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Reload".to_string(),
            body: serde_json::to_vec(&LoadShardRequest {
                shard_id: 44,
                load_version: 6,
                local_node_id: Some(2),
                shard_uri: "local://cpp-alias/stale-shard-44".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 20,
                readonly: true,
                table_name: "stale_cpp_alias".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &stale_reload,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: LoadShardResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.status.code, "stale_load_version");

        let reload = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Reload".to_string(),
            body: serde_json::to_vec(&LoadShardRequest {
                shard_id: 44,
                load_version: 8,
                local_node_id: Some(2),
                shard_uri: "local://cpp-alias/reloaded-shard-44".to_string(),
                start_routing_slot: 10,
                end_routing_slot: 20,
                readonly: false,
                table_name: "cpp_alias_reloaded".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &reload,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: LoadShardResponse = serde_json::from_slice(&body).unwrap();
        assert!(status.status.ok, "{status:?}");
        let info = engine.get_info(44).info.unwrap();
        assert_eq!(info.load_version, 8);
        assert_eq!(info.local_node_id, Some(2));
        assert_eq!(info.table_name, "cpp_alias_reloaded");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.transitions.len(), 1);
        assert_eq!(lifecycle.transitions[0].shard_id, 44);
        assert_eq!(lifecycle.transitions[0].state, "serving");
        assert_eq!(lifecycle.transitions[0].operation, "reload");

        let execute = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/ExecuteCmd".to_string(),
            body: serde_json::to_vec(&ExecuteRequest {
                shard_id: 44,
                command: Command::StringSet {
                    key: "cpp-service-key".to_string(),
                    value: b"via-cpp-name".to_vec(),
                },
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &execute,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let response: ExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");

        let batch = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/BatchExecuteCmd".to_string(),
            body: serde_json::to_vec(&BatchExecuteRequest {
                shard_id: 44,
                commands: vec![Command::StringGet {
                    key: "cpp-service-key".to_string(),
                }],
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &batch,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let response: BatchExecuteResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
        assert_eq!(
            response.responses[0].response,
            CommandResponse::Bytes {
                value: Some(b"via-cpp-name".to_vec())
            }
        );

        for path in [
            "/ServerService/GetConfig",
            "/ServerService/GetInfo",
            "/ServerService/GetStats",
        ] {
            let request = HttpRequest {
                method: "POST".to_string(),
                path: path.to_string(),
                body: serde_json::to_vec(&serde_json::json!({ "shard_id": 44 })).unwrap(),
            };
            let (code, body) = handle_cpp_server_service_route(
                &request,
                &engine,
                &runtime,
                None,
                "",
                "server-a",
                &data_raft_appliers,
            )
            .unwrap();
            assert_eq!(code, 200, "{path}");
            let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(status["status"]["ok"], true, "{path}: {status}");
        }

        let storage_readiness = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/GetStorageReadiness".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "shard_id": 44 })).unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &storage_readiness,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let readiness: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(readiness["shard_id"], 44);
        assert_eq!(readiness["production_ready"], true);

        let committed_log = serialize_data_raft_log(&DataRaftLogCodecEntry {
            shard_id: 44,
            raft_index: 9,
            log_id: 10,
            log_size: 0,
            oplog_sequence: 21,
            command: Command::StringSet {
                key: "cpp-service-raft-key".to_string(),
                value: b"via-apply-data-raft-log".to_vec(),
            },
        })
        .unwrap();
        let apply_raft = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/ApplyDataRaftLog".to_string(),
            body: serde_json::to_vec(&ApplyDataRaftLogRouteRequest {
                partition_id: 44,
                raft_index: 9,
                committed_log,
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &apply_raft,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let response: ApplyDataRaftLogRouteResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
        assert_eq!(response.applied_raft_index, 9);
        assert_eq!(response.applied_oplog_sequence, 21);

        let duplicate: ApplyDataRaftLogRouteResponse = serde_json::from_slice(
            &handle_cpp_server_service_route(
                &apply_raft,
                &engine,
                &runtime,
                None,
                "",
                "server-a",
                &data_raft_appliers,
            )
            .unwrap()
            .1,
        )
        .unwrap();
        assert!(duplicate.status.ok, "{duplicate:?}");
        assert_eq!(duplicate.applied_raft_index, 9);
        assert_eq!(duplicate.applied_oplog_sequence, 21);

        let read = engine.execute(ExecuteRequest {
            shard_id: 44,
            command: Command::StringGet {
                key: "cpp-service-raft-key".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"via-apply-data-raft-log".to_vec())
            }
        );

        let unload = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Unload".to_string(),
            body: serde_json::to_vec(&UnloadShardRequest { shard_id: 44 }).unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &unload,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: UnloadShardResponse = serde_json::from_slice(&body).unwrap();
        assert!(status.status.ok, "{status:?}");
        let lifecycle = runtime.lifecycle_report();
        assert_eq!(lifecycle.loaded_shard_count, 0);
        assert_eq!(lifecycle.transitions[0].state, "unloaded");
        assert_eq!(lifecycle.transitions[0].operation, "unload");

        let unknown = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/Unknown".to_string(),
            body: Vec::new(),
        };
        assert!(handle_cpp_server_service_route(
            &unknown,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers
        )
        .is_none());
    }

    #[test]
    fn cpp_server_service_lifecycle_snapshot_routes_restore_scheduler_state() {
        let dir = tempdir().unwrap();
        let engine = test_engine_with_cache(dir.path(), "source", 1024 * 1024);
        let runtime = DataNodeRuntime::new(engine.clone(), DataNodeRuntimeOptions::default());
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();

        let token = SchedulerLifecycleToken {
            task_id: 121,
            shard_id: 55,
            operation: "load".to_string(),
            load_version: 9,
            generation: 700,
        };
        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/RequireLifecycleToken".to_string(),
                body: serde_json::to_vec(&token).unwrap(),
            },
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert!(status.ok, "{status:?}");

        let load = LoadShardRequest {
            shard_id: 55,
            load_version: 9,
            local_node_id: Some(4),
            shard_uri: "local://snapshot/shard-55".to_string(),
            start_routing_slot: 100,
            end_routing_slot: 199,
            readonly: false,
            table_name: "snapshot_table".to_string(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/Load".to_string(),
                body: serde_json::to_vec(&load).unwrap(),
            },
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let loaded: LoadShardResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok, "{loaded:?}");

        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "GET".to_string(),
                path: "/ServerService/GetLifecycleSnapshot".to_string(),
                body: Vec::new(),
            },
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let snapshot: DataNodeLifecycleSnapshot = serde_json::from_slice(&body).unwrap();
        assert_eq!(snapshot.tokens, vec![token.clone()]);
        assert_eq!(snapshot.transitions.len(), 1);
        assert_eq!(snapshot.transitions[0].scheduler_task_id, Some(121));

        let restored_engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("restored-cache"),
            dir.path().join("restored-pages"),
            dir.path().join("restored-index"),
        );
        let restored =
            DataNodeRuntime::new(restored_engine.clone(), DataNodeRuntimeOptions::default());
        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/RestoreLifecycleSnapshot".to_string(),
                body: serde_json::to_vec(&snapshot).unwrap(),
            },
            &restored_engine,
            &restored,
            None,
            "",
            "server-b",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert!(status.ok, "{status:?}");
        assert_eq!(restored.lifecycle_tokens(), vec![token.clone()]);
        assert_eq!(restored.lifecycle_report().transitions[0].shard_id, 55);

        let snapshot_path = dir.path().join("lifecycle-snapshot.json");
        let request = LifecycleSnapshotFileRequest {
            path: snapshot_path.clone(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/SaveLifecycleSnapshot".to_string(),
                body: serde_json::to_vec(&request).unwrap(),
            },
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let saved: LifecycleSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(saved.status.ok, "{saved:?}");
        assert!(snapshot_path.exists());
        assert_eq!(saved.snapshot.unwrap().tokens, vec![token.clone()]);

        let file_engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("file-cache"),
            dir.path().join("file-pages"),
            dir.path().join("file-index"),
        );
        let file_restored =
            DataNodeRuntime::new(file_engine.clone(), DataNodeRuntimeOptions::default());
        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/LoadLifecycleSnapshot".to_string(),
                body: serde_json::to_vec(&request).unwrap(),
            },
            &file_engine,
            &file_restored,
            None,
            "",
            "server-c",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let loaded: LifecycleSnapshotFileResponse = serde_json::from_slice(&body).unwrap();
        assert!(loaded.status.ok, "{loaded:?}");
        assert_eq!(loaded.snapshot.unwrap().tokens, vec![token.clone()]);
        assert_eq!(file_restored.lifecycle_tokens(), vec![token]);
        assert_eq!(
            file_restored.lifecycle_report().transitions[0].operation,
            "load"
        );

        let (code, body) = handle_cpp_server_service_route(
            &HttpRequest {
                method: "POST".to_string(),
                path: "/ServerService/RestoreLifecycleSnapshot".to_string(),
                body: serde_json::to_vec(&DataNodeLifecycleSnapshot {
                    format_version: 99,
                    transitions: Vec::new(),
                    tokens: Vec::new(),
                })
                .unwrap(),
            },
            &file_engine,
            &file_restored,
            None,
            "",
            "server-c",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert_eq!(status.code, "bad_lifecycle_snapshot");
    }

    #[test]
    fn cpp_server_service_lifecycle_snapshot_survives_http_restart_boundary() {
        let dir = tempdir().unwrap();
        let source_engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("source-cache"),
            dir.path().join("source-pages"),
            dir.path().join("source-index"),
        );
        let source_runtime =
            DataNodeRuntime::new(source_engine.clone(), DataNodeRuntimeOptions::default());
        let source_addr =
            start_lifecycle_snapshot_server(source_engine.clone(), source_runtime.clone());

        let load_token = SchedulerLifecycleToken {
            task_id: 200,
            shard_id: 77,
            operation: "load".to_string(),
            load_version: 11,
            generation: 901,
        };
        let reload_token = SchedulerLifecycleToken {
            task_id: 201,
            shard_id: 77,
            operation: "reload".to_string(),
            load_version: 12,
            generation: 902,
        };
        for token in [&load_token, &reload_token] {
            let status =
                post_json::<_, Status>(&source_addr, "/ServerService/RequireLifecycleToken", token)
                    .unwrap();
            assert!(status.ok, "{status:?}");
        }

        let loaded = post_json::<_, LoadShardResponse>(
            &source_addr,
            "/ServerService/Load",
            &LoadShardRequest {
                shard_id: 77,
                load_version: 11,
                local_node_id: Some(7),
                shard_uri: "local://restart/source-77".to_string(),
                start_routing_slot: 700,
                end_routing_slot: 799,
                readonly: false,
                table_name: "restart_table".to_string(),
            },
        )
        .unwrap();
        assert!(loaded.status.ok, "{loaded:?}");

        let snapshot_path = dir.path().join("restart-lifecycle-snapshot.json");
        let saved = post_json::<_, LifecycleSnapshotFileResponse>(
            &source_addr,
            "/ServerService/SaveLifecycleSnapshot",
            &LifecycleSnapshotFileRequest {
                path: snapshot_path.clone(),
            },
        )
        .unwrap();
        assert!(saved.status.ok, "{saved:?}");
        assert!(snapshot_path.exists());
        assert_eq!(saved.snapshot.as_ref().unwrap().tokens.len(), 2);
        assert_eq!(
            saved.snapshot.as_ref().unwrap().transitions[0].scheduler_task_id,
            Some(200)
        );

        let restarted_engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("restarted-cache"),
            dir.path().join("restarted-pages"),
            dir.path().join("restarted-index"),
        );
        let restarted_runtime =
            DataNodeRuntime::new(restarted_engine.clone(), DataNodeRuntimeOptions::default());
        let restarted_addr =
            start_lifecycle_snapshot_server(restarted_engine.clone(), restarted_runtime.clone());

        let loaded_snapshot = post_json::<_, LifecycleSnapshotFileResponse>(
            &restarted_addr,
            "/ServerService/LoadLifecycleSnapshot",
            &LifecycleSnapshotFileRequest {
                path: snapshot_path.clone(),
            },
        )
        .unwrap();
        assert!(loaded_snapshot.status.ok, "{loaded_snapshot:?}");
        assert_eq!(loaded_snapshot.snapshot.as_ref().unwrap().tokens.len(), 2);

        let restored_snapshot = get_json_with_options::<DataNodeLifecycleSnapshot>(
            &restarted_addr,
            "/ServerService/GetLifecycleSnapshot",
            HttpRequestOptions {
                connect_timeout_ms: 100,
                io_timeout_ms: 100,
                max_retries: 0,
            },
        )
        .unwrap();
        assert_eq!(
            restored_snapshot.tokens,
            vec![load_token.clone(), reload_token.clone()]
        );
        assert_eq!(restored_snapshot.transitions[0].shard_id, 77);

        let reloaded = post_json::<_, LoadShardResponse>(
            &restarted_addr,
            "/ServerService/Reload",
            &LoadShardRequest {
                shard_id: 77,
                load_version: 12,
                local_node_id: Some(8),
                shard_uri: "local://restart/reloaded-77".to_string(),
                start_routing_slot: 800,
                end_routing_slot: 899,
                readonly: true,
                table_name: "restart_table_reloaded".to_string(),
            },
        )
        .unwrap();
        assert!(reloaded.status.ok, "{reloaded:?}");
        let lifecycle =
            get_json_with_options::<temporalstore_rust::data_node::DataNodeLifecycleReport>(
                &restarted_addr,
                "/ServerService/GetLifecycle",
                HttpRequestOptions {
                    connect_timeout_ms: 100,
                    io_timeout_ms: 100,
                    max_retries: 0,
                },
            )
            .unwrap();
        assert_eq!(lifecycle.transitions[0].operation, "reload");
        assert_eq!(lifecycle.transitions[0].scheduler_task_id, Some(201));
        assert_eq!(lifecycle.transitions[0].scheduler_generation, Some(902));
        assert_eq!(lifecycle.readonly_count, 1);
    }

    #[test]
    fn server_cpp_async_lifecycle_alias_submits_load_job() {
        let dir = tempdir().unwrap();
        let engine = test_engine_with_cache(dir.path(), "engine", 1024 * 1024);
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();
        let runtime = DataNodeRuntime::new(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 4,
                max_background_queue_depth: 2,
            },
        );
        let async_load = HttpRequest {
            method: "POST".to_string(),
            path: "/ServerService/AsyncLoad".to_string(),
            body: serde_json::to_vec(&LoadShardRequest {
                shard_id: 45,
                load_version: 1,
                local_node_id: Some(1),
                shard_uri: "local://cpp-alias/shard-45".to_string(),
                start_routing_slot: 0,
                end_routing_slot: u32::MAX,
                readonly: true,
                table_name: "cpp_alias_async".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_cpp_server_service_route(
            &async_load,
            &engine,
            &runtime,
            None,
            "",
            "server-a",
            &data_raft_appliers,
        )
        .unwrap();
        assert_eq!(code, 200);
        let async_status: DataNodeTaskStatus = serde_json::from_slice(&body).unwrap();
        assert!(async_status.status.ok, "{async_status:?}");
        assert_eq!(async_status.kind, DataNodeTaskKind::Load);
        assert!(async_status.output.is_none());
    }

    // shared-corpus: ingestion_kafka_offset_ledger
    #[test]
    fn server_ingest_batch_routes_execute_api_kafka_and_flink_records() {
        let dir = tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("index"),
        );
        engine.load_shard(7);
        let runtime = DataNodeRuntime::new(engine.clone(), DataNodeRuntimeOptions::default());
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();
        let request = IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: Vec::new(),
            flink_checkpoints: Vec::new(),
            records: vec![
                IngestionRecord {
                    source: IngestionSource::Api {
                        request_id: "api-route-1".to_string(),
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "api-ingest-route".to_string(),
                        value: b"api-value".to_vec(),
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "route-topic".to_string(),
                        partition: 0,
                        offset: 10,
                        key: Some("hash-key".to_string()),
                        timestamp_ms: Some(99),
                    },
                    shard_id: 7,
                    command: Command::HashSet {
                        key: "kafka-hash".to_string(),
                        field: "field".to_string(),
                        value: b"kafka-value".to_vec(),
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Flink {
                        job_id: "flink-job".to_string(),
                        operator_uid: "sink".to_string(),
                        subtask_index: 1,
                        checkpoint_id: 42,
                        record_index: 2,
                    },
                    shard_id: 7,
                    command: Command::FeatureAppend {
                        key: "flink-feature".to_string(),
                        points: vec![temporalstore_rust::FeaturePoint {
                            timestamp_ms: 42,
                            value: b"flink-value".to_vec(),
                        }],
                    },
                },
            ],
        };

        for (alias_index, path) in [
            "/ServerService/IngestBatch",
            "/IngestionService/IngestBatch",
        ]
        .into_iter()
        .enumerate()
        {
            let mut request = request.clone();
            if let IngestionSource::Api { request_id } = &mut request.records[0].source {
                *request_id = format!("{request_id}-{alias_index}");
            }
            if let IngestionSource::Kafka { offset, .. } = &mut request.records[1].source {
                *offset += alias_index as i64;
            }
            if let IngestionSource::Flink {
                checkpoint_id,
                record_index,
                ..
            } = &mut request.records[2].source
            {
                *checkpoint_id += alias_index as u64;
                *record_index += alias_index as u64;
            }
            let (code, body) = handle_cpp_server_service_route(
                &HttpRequest {
                    method: "POST".to_string(),
                    path: path.to_string(),
                    body: serde_json::to_vec(&request).unwrap(),
                },
                &engine,
                &runtime,
                None,
                "",
                "server-a",
                &data_raft_appliers,
            )
            .unwrap();
            assert_eq!(code, 200, "{path}");
            let report: IngestionBatchReport = serde_json::from_slice(&body).unwrap();
            assert!(report.status.ok, "{path}: {report:?}");
            assert_eq!(report.accepted_count, 3);
            assert_eq!(report.failed_count, 0);
        }

        let read = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "api-ingest-route".to_string(),
            },
        });
        assert_eq!(
            read.response,
            CommandResponse::Bytes {
                value: Some(b"api-value".to_vec())
            }
        );
        let hash = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::HashGet {
                key: "kafka-hash".to_string(),
                field: "field".to_string(),
            },
        });
        assert_eq!(
            hash.response,
            CommandResponse::Bytes {
                value: Some(b"kafka-value".to_vec())
            }
        );
    }

    #[test]
    fn server_ingest_batch_rest_route_rejects_duplicate_kafka_offset_without_noop() {
        let dir = tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("index"),
        );
        engine.load_shard(7);
        let request = IngestionBatchRequest {
            stop_on_error: false,
            kafka_high_watermarks: Vec::new(),
            flink_checkpoints: Vec::new(),
            records: vec![
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "dup-topic".to_string(),
                        partition: 1,
                        offset: 99,
                        key: None,
                        timestamp_ms: None,
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "first-offset".to_string(),
                        value: b"accepted".to_vec(),
                    },
                },
                IngestionRecord {
                    source: IngestionSource::Kafka {
                        topic: "dup-topic".to_string(),
                        partition: 1,
                        offset: 99,
                        key: None,
                        timestamp_ms: None,
                    },
                    shard_id: 7,
                    command: Command::StringSet {
                        key: "duplicate-offset".to_string(),
                        value: b"rejected".to_vec(),
                    },
                },
            ],
        };

        let (code, body) = ingest_batch_route(&engine, &serde_json::to_vec(&request).unwrap());
        assert_eq!(code, 200);
        let report: IngestionBatchReport = serde_json::from_slice(&body).unwrap();
        assert_eq!(report.status.code, "partial_ingestion_failure");
        assert_eq!(report.accepted_count, 1);
        assert_eq!(report.failed_count, 1);
        assert_eq!(report.duplicate_count, 1);
        assert_eq!(report.results[1].status.code, "duplicate_ingestion_record");

        let accepted = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "first-offset".to_string(),
            },
        });
        assert_eq!(
            accepted.response,
            CommandResponse::Bytes {
                value: Some(b"accepted".to_vec())
            }
        );
        let duplicate = engine.execute(ExecuteRequest {
            shard_id: 7,
            command: Command::StringGet {
                key: "duplicate-offset".to_string(),
            },
        });
        assert_eq!(duplicate.response, CommandResponse::Bytes { value: None });
    }

    #[test]
    fn server_cpp_admin_aliases_expose_runtime_preflight_dirty_and_queue_state() {
        let dir = tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("index"),
        );
        engine.load_shard(1);
        let runtime = DataNodeRuntime::new(engine.clone(), DataNodeRuntimeOptions::default());
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();

        let write = runtime.submit_execute(
            ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "dirty-admin".to_string(),
                    value: b"value".to_vec(),
                },
            },
            RequestController::default(),
        );
        assert!(write.status.ok, "{write:?}");
        let _ = runtime.job_status(write.job_id);

        for path in [
            "/ServerService/GetRuntimeStats",
            "/ServerService/Preflight",
            "/ServerService/GetLifecycle",
            "/ServerService/GetLifecyclePersistence",
            "/ServerService/ListDirtyObjects",
            "/ServerService/ListQueuedShardWorkers",
        ] {
            let (code, body) = handle_cpp_server_service_route(
                &HttpRequest {
                    method: "GET".to_string(),
                    path: path.to_string(),
                    body: Vec::new(),
                },
                &engine,
                &runtime,
                None,
                "",
                "server-a",
                &data_raft_appliers,
            )
            .unwrap();
            assert_eq!(code, 200, "{path}");
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert!(value.is_object() || value.is_array(), "{path}: {value}");
        }
    }

    // shared-corpus: server_raft_status_admin_routes
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
    fn server_raft_admin_bootstrap_external_snapshot_installs_downloaded_snapshot() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3], true);
        state.runtime.cluster().set_alive(3, false).unwrap();
        state
            .runtime
            .cluster()
            .propose(Command::StringSet {
                key: "server-external-snapshot-key".to_string(),
                value: b"server-external-snapshot-value".to_vec(),
            })
            .unwrap();
        state.runtime.cluster().set_alive(3, true).unwrap();
        assert_eq!(
            state
                .runtime
                .cluster()
                .local_status(3)
                .unwrap()
                .commit_index,
            0
        );

        let tmp = tempdir().unwrap();
        let object_root = tmp.path().join("objects");
        let publish_local_root = tmp.path().join("publish-local");
        let restore_local_root = tmp.path().join("restore-local");
        let publish_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/publish_external_snapshot".to_string(),
            body: serde_json::to_vec(&RaftAdminPublishExternalSnapshotRequest {
                object_root: object_root.display().to_string(),
                local_root: publish_local_root.display().to_string(),
                cluster_id: "cluster-a".to_string(),
                bucket: "test".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &publish_request).unwrap();
        assert_eq!(code, 200);
        let published: RaftAdminPublishExternalSnapshotResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(published.status.ok, "{published:?}");
        let snapshot_ref = published.report.unwrap().meta_ref;

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/bootstrap_external_snapshot".to_string(),
            body: serde_json::to_vec(&RaftAdminBootstrapExternalSnapshotRequest {
                target_id: 3,
                snapshot: snapshot_ref,
                object_root: object_root.display().to_string(),
                local_root: restore_local_root.display().to_string(),
                cluster_id: "cluster-a".to_string(),
                bucket: "test".to_string(),
            })
            .unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let response: RaftAdminBootstrapExternalSnapshotResponse =
            serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
        assert_eq!(response.plan.unwrap().target_id, 3);

        let read = state
            .runtime
            .cluster()
            .read_local(
                3,
                Command::StringGet {
                    key: "server-external-snapshot-key".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            read,
            CommandResponse::Bytes {
                value: Some(b"server-external-snapshot-value".to_vec())
            }
        );
    }

    // shared-corpus: server_raft_apply_health_route
    #[test]
    fn server_exposes_raft_apply_health_route() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2], true);
        state
            .runtime
            .cluster()
            .propose(Command::StringSet {
                key: "apply-health-route".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/apply_health".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "max_allowed_apply_lag": 0 })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let health: temporalstore_rust::RaftApplyHealth = serde_json::from_slice(&body).unwrap();
        let expected = state.runtime.local_apply_health(0);
        assert!(health.healthy, "{health:?}");
        assert_eq!(health, expected);
        assert_eq!(health.leader_commit_index, 1);
        assert_eq!(health.max_apply_lag, 0);
    }

    // shared-corpus: raft_byteraft_metrics_admin_pipeline_status server_raft_byteraft_runtime_admin_route
    #[test]
    fn server_exposes_byteraft_runtime_admin_route() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3], true);
        let cluster = state.runtime.cluster();
        cluster
            .propose(Command::StringSet {
                key: "server-byteraft-admin-snapshot".to_string(),
                value: b"seed".to_vec(),
            })
            .unwrap();
        cluster.maybe_trigger_snapshot().unwrap();
        let snapshot = cluster.build_install_snapshot_request(2).unwrap();
        cluster.receive_install_snapshot(snapshot).unwrap();
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "server-byteraft-admin-lag".to_string(),
                value: b"lag".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();
        let _ = cluster.check_write_authority(3);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/raft/control/byteraft_runtime_admin".to_string(),
            body: Vec::new(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let report: temporalstore_rust::raft::ByteRaftRuntimeAdminReport =
            serde_json::from_slice(&body).unwrap();
        assert!(report.read_index_validated);
        assert!(report.lease_read_validated);
        assert!(report.stale_follower_read_rejected);
        assert!(report.stale_follower_write_rejected);
        assert!(report.snapshot_sender_lifecycle_present);
        assert!(report.snapshot_downloader_lifecycle_present);
        assert!(report.wal_segment_lifecycle_present);
        assert!(report.admin_status_surface_complete);
        let expected_capabilities = [
            "per_peer_replication_pipeline_state",
            "reorder_queue_runtime",
            "snapshot_sender_downloader_lifecycle",
            "lease_read_index_pre_vote_semantics",
            "wal_segment_lifecycle",
            "admin_status_surface",
        ];
        for capability in expected_capabilities {
            let row = report
                .capability_matrix
                .iter()
                .find(|row| row.capability == capability)
                .unwrap_or_else(|| panic!("missing capability row {capability}"));
            assert!(!row.evidence_field.is_empty());
        }
        assert!(report
            .peer_pipeline_states
            .iter()
            .any(|peer| peer.peer_id == 3 && peer.append_queue_depth > 0));
        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/raft/control/byteraft_local_status".to_string(),
            body: Vec::new(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let local: temporalstore_rust::raft::ByteRaftLocalStatusReport =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(local.leader_id, report.leader_id);
        assert!(!local.peers.is_empty());
        assert!(local
            .peers
            .iter()
            .any(|peer| peer.pipeline_state.peer_id == peer.status.node_id));

        let metrics = state.runtime.cluster().prometheus_metrics();
        assert!(metrics.contains("temporalstore_raft_byteraft_ready"));
        assert!(metrics.contains("temporalstore_raft_byteraft_capability_ready"));
        assert!(metrics.contains("capability=\"wal_segment_lifecycle\""));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_queue_depth"));
        assert!(metrics.contains("replica_role=\"voter\""));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_queue_depth"));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_installed_index"));
        assert!(metrics.contains("temporalstore_raft_byteraft_wal_segment_count"));
    }

    // shared-corpus: server_raft_membership_apply_route
    #[test]
    fn server_exposes_raft_membership_apply_route() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3], false);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/membership/apply".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "voters": [1, 2] })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let response: RaftMembershipApplyResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
        let report = response
            .report
            .expect("membership report should be present");
        assert_eq!(report.committed_membership.voters, vec![1, 2]);
        assert_eq!(state.runtime.status().majority, 2);
    }

    // shared-corpus: server_raft_control_scale_up_down
    #[test]
    fn server_raft_control_scale_up_down_preserves_serving() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3, 4], false);
        for node_id in [2, 3, 4] {
            state.runtime.cluster().set_alive(node_id, true).unwrap();
            state.runtime.cluster().catch_up(node_id).unwrap();
        }

        let remove_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/control/remove_node".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "node_id": 4 })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &remove_request).unwrap();
        assert_eq!(code, 200);
        let removed: RaftMembershipApplyResponse = serde_json::from_slice(&body).unwrap();
        assert!(removed.status.ok, "{removed:?}");
        assert_eq!(state.runtime.cluster().membership().voters, vec![1, 2, 3]);
        for node_id in [2, 3] {
            state.runtime.cluster().set_alive(node_id, true).unwrap();
            state.runtime.cluster().catch_up(node_id).unwrap();
        }

        state
            .runtime
            .cluster()
            .propose(Command::StringSet {
                key: "server-scale-down".to_string(),
                value: b"after-scale-down".to_vec(),
            })
            .unwrap();
        for node_id in [1, 2, 3] {
            let read = state
                .runtime
                .read_local(
                    node_id,
                    Command::StringGet {
                        key: "server-scale-down".to_string(),
                    },
                )
                .unwrap();
            assert_eq!(
                read,
                CommandResponse::Bytes {
                    value: Some(b"after-scale-down".to_vec())
                }
            );
        }

        let add_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/control/add_node".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "node_id": 4 })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &add_request).unwrap();
        assert_eq!(code, 200);
        let added: RaftMembershipApplyResponse = serde_json::from_slice(&body).unwrap();
        assert!(added.status.ok, "{added:?}");
        assert_eq!(
            state.runtime.cluster().membership().voters,
            vec![1, 2, 3, 4]
        );
        for node_id in [2, 3, 4] {
            state.runtime.cluster().set_alive(node_id, true).unwrap();
            state.runtime.cluster().catch_up(node_id).unwrap();
        }

        state
            .runtime
            .cluster()
            .propose(Command::StringSet {
                key: "server-scale-up".to_string(),
                value: b"after-scale-up".to_vec(),
            })
            .unwrap();
        for node_id in [1, 2, 3, 4] {
            let read = state
                .runtime
                .read_local(
                    node_id,
                    Command::StringGet {
                        key: "server-scale-up".to_string(),
                    },
                )
                .unwrap();
            assert_eq!(
                read,
                CommandResponse::Bytes {
                    value: Some(b"after-scale-up".to_vec())
                }
            );
        }
    }

    // shared-corpus: server_raft_control_accept_leadership
    #[test]
    fn server_raft_control_accept_leadership_matches_raft_node_route() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 2, vec![1, 2, 3], false);
        assert_eq!(state.runtime.status().leader_id, 1);
        state.runtime.cluster().set_alive(2, true).unwrap();
        state.runtime.cluster().catch_up(2).unwrap();

        let wrong_node_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/control/accept_leadership".to_string(),
            body: serde_json::to_vec(&RaftControlLeadershipRequest { node_id: 3 }).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &wrong_node_request).unwrap();
        assert_eq!(code, 200);
        let wrong_node_status: Status = serde_json::from_slice(&body).unwrap();
        assert!(!wrong_node_status.ok);
        assert_eq!(wrong_node_status.code, "bad_request");
        assert_eq!(state.runtime.status().leader_id, 1);

        let accept_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/control/accept_leadership".to_string(),
            body: serde_json::to_vec(&RaftControlLeadershipRequest { node_id: 2 }).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &accept_request).unwrap();
        assert_eq!(code, 200);
        let status: Status = serde_json::from_slice(&body).unwrap();
        assert!(status.ok, "{status:?}");
        assert_eq!(state.runtime.status().leader_id, 2);
    }

    // shared-corpus: server_raft_admin_wait_applied
    #[test]
    fn server_raft_admin_wait_applied_reports_lag_and_success_after_catchup() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3], true);
        let cluster = state.runtime.cluster();
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "wait-applied-route".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();

        let wait_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/wait_applied".to_string(),
            body: serde_json::to_vec(
                &serde_json::json!({ "node_id": 3, "index": 1, "timeout_ms": 1 }),
            )
            .unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &wait_request).unwrap();
        assert_eq!(code, 200);
        let response: RaftAdminLivenessResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(response.status.code, "raft_error");
        assert!(response.status.message.contains("did not apply raft index"));

        let catchup_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/local_catch_up".to_string(),
            body: serde_json::to_vec(&serde_json::json!({ "node_id": 3 })).unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &catchup_request).unwrap();
        assert_eq!(code, 200);
        let response: RaftAdminLivenessResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");

        let wait_request = HttpRequest {
            method: "POST".to_string(),
            path: "/raft/admin/wait_applied".to_string(),
            body: serde_json::to_vec(
                &serde_json::json!({ "node_id": 3, "index": 1, "timeout_ms": 0 }),
            )
            .unwrap(),
        };
        let (code, body) = handle_server_raft_route(&state, &wait_request).unwrap();
        assert_eq!(code, 200);
        let response: RaftAdminLivenessResponse = serde_json::from_slice(&body).unwrap();
        assert!(response.status.ok, "{response:?}");
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

    fn start_lifecycle_snapshot_server(engine: TemporalEngine, runtime: DataNodeRuntime) -> String {
        let server_addr = free_local_addr();
        let bind_addr = server_addr.clone();
        let advertised_addr = server_addr.clone();
        let engine_for_server = engine.clone();
        let runtime_for_server = runtime.clone();
        let data_raft_appliers: Arc<Mutex<BTreeMap<u64, DataRaftCommittedLogApplier>>> =
            Arc::default();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    _ => handle_cpp_server_service_route(
                        &request,
                        &engine_for_server,
                        &runtime_for_server,
                        None,
                        "",
                        &advertised_addr,
                        &data_raft_appliers,
                    )
                    .unwrap_or_else(|| {
                        json_response(404, &Status::error("not_found", "unknown route"))
                    }),
                }
            })
            .unwrap()
        });
        wait_for_http(&server_addr);
        server_addr
    }

    fn start_meta_route_server(shard_id: u64, primary_addr: String) -> String {
        start_dynamic_meta_route_server(shard_id, Arc::new(Mutex::new(primary_addr)))
    }

    fn start_dynamic_meta_route_server(shard_id: u64, primary_addr: Arc<Mutex<String>>) -> String {
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
                                server_addr: primary_addr.lock().unwrap().clone(),
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
            engine: ProductionRaftEngineKind::TemporalRaft,
            shard_id: 1,
            local_node_id,
            nodes,
            wal_dir: root
                .join(format!("server-raft-node-{local_node_id}"))
                .display()
                .to_string(),
            config: RaftConfig {
                enable_pre_vote: true,
                lease_duration_ms: 1_000,
                max_segment_bytes: 512,
                min_keep_segment_num: 1,
                ..RaftConfig::default()
            },
            rpc: RaftRpcRuntimeOptions {
                max_retries: 1,
                deadline_ms: 100,
                ..RaftRpcRuntimeOptions::default()
            },
            security: temporalstore_rust::ProductionRaftSecurity::plaintext_for_local_chaos(
                "test-token",
            ),
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
