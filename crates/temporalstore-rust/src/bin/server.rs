// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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
use temporalstore_rust::http::{json_response, parse_json, post_json, serve, HttpRequest};
use temporalstore_rust::ingestion::{FlinkCheckpointStatus, IngestionBatchRequest};
use temporalstore_rust::meta::{
    AckResponse, GetTableTopologyRequest, LoadFinishRequest, ShardStatLoad, RegisterServerRequest,
    RegisterShardRequest, RegisterShardResponse, ServerHeartbeatRequest, ServerHeartbeatResponse,
    ShardLoad, ShardSnapshotRef, TableTopologyResponse,
};
use temporalstore_rust::raft::{
    DataRaftReadMode, DataRaftReadPolicy, RaftReplicaBootstrapPlan,
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
    LoadShardRequest, MembershipUpdateRequest, ProductionRaftEngineKind, ProductionRaftNode,
    ProductionRaftRuntime, ProductionRaftRuntimeOptions, RaftConfig, RaftControlLeadershipRequest,
    RaftFailoverReport, RaftMembershipChangeReport, RaftNodeId, RaftRpcRuntimeOptions,
    RequestController, ScanStreamRequest, SchedulerLifecycleToken, SetConfigRequest,
    BucketDumpManifest, StorageCacheInvalidateBucketRequest, StorageLifecycleRequest,
    StorageProductionReadinessRequest, StreamReadRequest, UnloadShardRequest,
};
use temporalstore_snapshot::object_store::ObjectStore;
use temporalstore_snapshot::{FileObjectStore, S3SnapshotStore};
use bytes::Bytes;

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

/// Receipt returned by the streamed attachment upload endpoint (`POST /blob/<key>`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BlobReceipt {
    status: Status,
    key: String,
    bytes_written: u64,
    object_length: u64,
    chunks: u64,
}

fn main() {
    let addr = std::env::var("TS_SERVER_BIND_ADDR")
        .or_else(|_| std::env::var("TS_SERVER_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17002".to_string());
    let advertised_addr = std::env::var("TS_SERVER_ADVERTISE_ADDR")
        .unwrap_or_else(|_| std::env::var("TS_SERVER_ADDR").unwrap_or_else(|_| addr.clone()));
    let meta_addr_raw = std::env::var("TS_META_ADDR").ok();
    let meta_addr = meta_addr_raw
        .clone()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1:17001".to_string());
    let shard_id = std::env::var("TS_SHARD_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let cache_dir =
        std::env::var("TS_CACHE_DIR").unwrap_or_else(|_| "target/temporalstore-cache".to_string());
    let block_store_dir = std::env::var("TS_PAGE_STORE_DIR")
        .unwrap_or_else(|_| "target/temporalstore-pages".to_string());
    // Directory for the streamed attachment/blob tier (POST/GET /blob/<key>);
    // computed here so it does not outlive the move of `block_store_dir`.
    let blob_store_dir =
        std::env::var("TS_BLOB_STORE_DIR").unwrap_or_else(|_| format!("{block_store_dir}/blobs"));
    let index_dir = std::env::var("TS_INDEX_DIR")
        .unwrap_or_else(|_| "target/temporalstore-indexes".to_string());
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
    // Resolve the distributed storage/replication backend for this node:
    // matrixobject shared storage when detected, else a configured shared object
    // store, else raft replication (the default when nothing is configured).
    let storage_backend = temporalstore_rust::StorageBackendConfig::from_env().resolve();
    println!(
        "storage backend: {} (replication {:?})",
        storage_backend.describe(),
        storage_backend.replication_mode()
    );
    // Construct the shared object store early so a broken shared-storage config
    // fails fast at startup rather than on the first write.
    match storage_backend.build_shared_object_store() {
        Ok(Some(_shared_store)) => println!(
            "shared-storage backend ready: {} — shard durability served by shared storage",
            storage_backend.describe()
        ),
        Ok(None) => {}
        Err(err) => {
            eprintln!(
                "configured storage backend {} is unusable: {err}",
                storage_backend.describe()
            );
            std::process::exit(1);
        }
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
    let raft_state = start_server_raft_from_env(shard_id, node_id, &advertised_addr);

    // Standalone (no-metaserver) mode is the DEFAULT: run the datanode with no
    // metaserver, skipping server + shard registration and the heartbeat loop and
    // serving the locally-loaded shard directly. This is the right default for
    // single-node / open-source local use — a fresh `matrixark_rust_datanode` with no
    // config just works instead of hanging on metaserver registration.
    //
    // Opt INTO the distributed topology by giving a real TS_META_ADDR (a non-empty,
    // non-sentinel address) or setting TS_DISTRIBUTED=1. TS_STANDALONE forces the mode
    // explicitly and wins over both: TS_STANDALONE=1 → standalone, =0 → distributed
    // (using TS_META_ADDR, defaulting to 127.0.0.1:17001). Sentinels for TS_META_ADDR
    // ("local", "none", "standalone", "off", or empty) always mean standalone.
    let meta_addr_is_real = meta_addr_raw
        .as_deref()
        .map(|value| {
            !matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "" | "local" | "none" | "standalone" | "off"
            )
        })
        .unwrap_or(false);
    let standalone = match std::env::var("TS_STANDALONE")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("1") | Some("true") | Some("yes") | Some("on") => true,
        Some("0") | Some("false") | Some("no") | Some("off") => false,
        _ => !(meta_addr_is_real || env_bool("TS_DISTRIBUTED", false)),
    };
    if standalone {
        println!(
            "standalone datanode: no metaserver — serving shard {shard_id} locally at {advertised_addr}"
        );
    } else {
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
    }
    // Streamed large-file / attachment tier: POST /blob/<key> writes the request
    // body to the blob store in chunks via the ObjectStore::append_blob path (the
    // same primitive shared_store's append_blob_with_retry uses); GET /blob/<key>
    // streams it back. Wired to a FileObjectStore here because the matrixobject
    // feature is optional; a MatrixObject store drops in behind the same trait.
    let blob_store = Arc::new(FileObjectStore::new(PathBuf::from(&blob_store_dir)));
    let blob_chunk_bytes = env_usize("TS_BLOB_CHUNK_BYTES", 1024 * 1024).max(1);
    let blob_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(env_usize("TS_BLOB_RUNTIME_THREADS", 4))
            .enable_all()
            .build()
            .expect("blob tokio runtime should start"),
    );

    println!("temporalstore server listening on {addr}");
    serve(&addr, move |request| {
        if let Some(response) = handle_ping_route(&request) {
            return response;
        }
        if let Some(response) = handle_readiness_route(&request) {
            return response;
        }
        if let Some(response) = handle_blob_route(
            &request,
            &blob_store,
            &blob_runtime,
            blob_chunk_bytes,
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
                append_storage_backend_metric(&mut metrics, &storage_backend);
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
                json_response(200, &engine.bucket_storage_summaries(shard_id))
            }
            ("GET", path) if path.starts_with("/server/storage/dumps/") => {
                let shard_id = path
                    .trim_start_matches("/server/storage/dumps/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &engine.list_bucket_dump_manifests(shard_id))
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
                match parse_json::<StorageCacheInvalidateBucketRequest>(&request.body) {
                    Ok(req) => match engine.invalidate_storage_cache_bucket(req) {
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
                match parse_json::<BucketDumpManifest>(&request.body) {
                    Ok(manifest) => match engine.install_bucket_dump_manifest(&manifest) {
                        Ok(()) => json_response(200, &Status::ok()),
                        Err(status) => json_response(409, &status),
                    },
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
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
        ("GET", "/readiness") => {
            Some(json_response(200, &production_readiness_report()))
        }
        _ => None,
    }
}

/// Streamed attachment endpoint. `POST /blob/<key>` writes the request body to
/// the blob store in `chunk_bytes` slices through `ObjectStore::append_blob`
/// (chunked, so memory does not scale with a single giant write); returns a
/// JSON receipt. `GET /blob/<key>` returns the stored object bytes.
fn handle_blob_route(
    request: &HttpRequest,
    blob_store: &Arc<FileObjectStore>,
    runtime: &Arc<tokio::runtime::Runtime>,
    chunk_bytes: usize,
) -> Option<(u16, Vec<u8>)> {
    let key = request.path.strip_prefix("/blob/")?;
    if key.is_empty() {
        return Some(json_response(
            400,
            &Status::error("bad_request", "missing blob key"),
        ));
    }
    match request.method.as_str() {
        "POST" | "PUT" => {
            let result: Result<(u64, u64, u64), String> = runtime.block_on(async {
                // Replace any prior object so an upload is idempotent.
                let _ = blob_store.delete(key).await;
                let mut bytes_written = 0u64;
                let mut object_length = 0u64;
                let mut chunks = 0u64;
                for slice in request.body.chunks(chunk_bytes) {
                    let receipt = blob_store
                        .append_blob(key, Bytes::copy_from_slice(slice))
                        .await
                        .map_err(|err| err.to_string())?;
                    bytes_written += receipt.bytes_written;
                    object_length = receipt.object_length;
                    chunks += 1;
                }
                Ok((bytes_written, object_length, chunks))
            });
            Some(match result {
                Ok((bytes_written, object_length, chunks)) => json_response(
                    200,
                    &BlobReceipt {
                        status: Status::ok(),
                        key: key.to_string(),
                        bytes_written,
                        object_length,
                        chunks,
                    },
                ),
                Err(err) => json_response(500, &Status::error("blob_write_failed", err)),
            })
        }
        "GET" => {
            let result = runtime.block_on(async { blob_store.get(key).await });
            Some(match result {
                Ok(bytes) => (200, bytes.to_vec()),
                Err(err) => {
                    json_response(404, &Status::error("blob_not_found", err.to_string()))
                }
            })
        }
        _ => Some(json_response(
            405,
            &Status::error("method_not_allowed", "use POST or GET on /blob/<key>"),
        )),
    }
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
    #[serde(rename = "slot")]
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotRequest {
    object_root: String,
    local_root: String,
    #[serde(default = "default_snapshot_cluster_id")]
    cluster_id: String,
    #[serde(default = "default_snapshot_bucket")]
    #[serde(rename = "slot")]
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

include!("server/raft_route.rs");
include!("server/metrics.rs");
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
                + stats.control_state_records) as u64,
            memory_bytes: stats.cache.memory_bytes as u64,
        })
        .collect();
    let shard_stat_loads = stats
        .into_iter()
        .map(|stats| ShardStatLoad {
            shard_id: stats.shard_id,
            shard_stat_info: stats.shard_stat_info,
        })
        .collect();
    let request = ServerHeartbeatRequest {
        server_addr: server_addr.to_string(),
        boot_time_ms: 0,
        binary_version: binary_version.to_string(),
        shard_loads,
        shard_stat_loads,
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
    use temporalstore_rust::http::{get_json_with_options, HttpRequestOptions};
    use temporalstore_rust::ingestion::{IngestionBatchReport, IngestionRecord, IngestionSource};
    use temporalstore_rust::types::{Command, CommandResponse};
    use temporalstore_rust::{
        BatchExecuteResponse, DataNodeTaskKind, DataNodeTaskStatus, LoadShardResponse,
        ProductionReadinessReport, UnloadShardResponse,
    };

    use super::*;

    const TEST_CACHE_BYTES: usize = 1024;

    #[test]
    fn control_state_set_and_get_is_a_replicated_write_not_a_local_read() {
        // Regression: a mutating command must never be classified as a raft *read*. Reads are
        // served locally via read_via_server_raft (execute_via_server_raft), which skips the raft
        // log; only non-reads are proposed + replicated. ControlStateSetAndGet MUTATES (adds
        // `amount` to the series and persists a control-state page) and the engine's
        // is_write_command treats it as a write -- matching how the
        // control-state SETANDGET family is registered as a write. Classifying it as a read applied the mutation
        // only on the leader and dropped it from the raft log, so followers diverged and the
        // write was lost on failover. It must fall through to `propose`.
        let mutating = Command::ControlStateSetAndGet {
            family: temporalstore_rust::types::ControlStateFamily::Distinct,
            key: "k".to_string(),
            timestamp_ms: 10,
            amount: 4,
            start_ms: 0,
            end_ms: 100,
            aggregator: "sum".to_string(),
        };
        assert!(
            !is_raft_read_command(&mutating),
            "ControlStateSetAndGet mutates; it must be proposed to raft, not served as a local read"
        );

        // Same for the options variant (it also mutates + persists).
        let mutating_with_options = Command::ControlStateSetAndGetWithOptions {
            family: temporalstore_rust::types::ControlStateFamily::Distinct,
            key: "k".to_string(),
            timestamp_ms: 10,
            amount: 4,
            start_ms: 0,
            end_ms: 100,
            aggregator: "sum".to_string(),
            precision_ms: None,
            ttl_ms: None,
            uuid: None,
        };
        assert!(
            !is_raft_read_command(&mutating_with_options),
            "ControlStateSetAndGetWithOptions mutates; it must be proposed to raft, not a local read"
        );

        // Contrast: a genuine read-only ControlState command must remain locally servable.
        let read_only = Command::ControlStateQuery {
            key: "k".to_string(),
            start_ms: 0,
            end_ms: 100,
            aggregator: "sum".to_string(),
        };
        assert!(
            is_raft_read_command(&read_only),
            "ControlStateQuery is read-only and must remain locally servable"
        );
    }

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
            max_dump_buckets_per_round: 4,
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
            "temporalstore_storage_manager_phase_work{shard_id=\"73\",phase=\"reclaim_wal\",kind=\"wal_records_removed\"}"
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
        assert_eq!(request.start_routing_bucket, 100);
        assert_eq!(request.end_routing_bucket, 199);
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
                    start_routing_bucket: 90,
                    end_routing_bucket: 99,
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
                                    partition_version: 0,
                                    serving_options:
                                        temporalstore_rust::meta::TableServingOptions::default(),
                                }),
                                shards: vec![temporalstore_rust::meta::TableShard {
                                    shard_id: 9,
                                    start_bucket: 90,
                                    end_bucket: 99,
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
                    start_routing_bucket: 0,
                    end_routing_bucket: 999,
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

    // shared-corpus: native_redis_live_storage_smoke_parity_surfaces
    #[test]
    fn server_ping_routes_match_ping_rpc() {
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

    // shared-corpus: raft_matrixraft_metrics_admin_pipeline_status server_raft_matrixraft_runtime_admin_route
    #[test]
    fn server_exposes_matrixraft_runtime_admin_route() {
        let dir = tempdir().unwrap();
        let state = test_server_raft_state(dir.path(), 1, vec![1, 2, 3], true);
        let cluster = state.runtime.cluster();
        cluster
            .propose(Command::StringSet {
                key: "server-matrixraft-admin-snapshot".to_string(),
                value: b"seed".to_vec(),
            })
            .unwrap();
        cluster.maybe_trigger_snapshot().unwrap();
        let snapshot = cluster.build_install_snapshot_request(2).unwrap();
        cluster.receive_install_snapshot(snapshot).unwrap();
        cluster.set_alive(3, false).unwrap();
        cluster
            .propose(Command::StringSet {
                key: "server-matrixraft-admin-lag".to_string(),
                value: b"lag".to_vec(),
            })
            .unwrap();
        cluster.set_alive(3, true).unwrap();
        let _ = cluster.check_write_authority(3);

        let request = HttpRequest {
            method: "GET".to_string(),
            path: "/raft/control/matrixraft_runtime_admin".to_string(),
            body: Vec::new(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let report: temporalstore_rust::raft::MatrixRaftRuntimeAdminReport =
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
            path: "/raft/control/matrixraft_local_status".to_string(),
            body: Vec::new(),
        };
        let (code, body) = handle_server_raft_route(&state, &request).unwrap();
        assert_eq!(code, 200);
        let local: temporalstore_rust::raft::MatrixRaftLocalStatusReport =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(local.leader_id, report.leader_id);
        assert!(!local.peers.is_empty());
        assert!(local
            .peers
            .iter()
            .any(|peer| peer.pipeline_state.peer_id == peer.status.node_id));

        let metrics = state.runtime.cluster().prometheus_metrics();
        assert!(metrics.contains("matrixraft_ready"));
        assert!(metrics.contains("matrixraft_capability_ready"));
        assert!(metrics.contains("matrixraft_capability_field_present"));
        assert!(metrics.contains("temporalstore_raft_matrixraft_ready"));
        assert!(metrics.contains("temporalstore_raft_matrixraft_capability_ready"));
        assert!(metrics.contains("capability=\"wal_segment_lifecycle\""));
        assert!(metrics.contains("temporalstore_raft_matrixraft_peer_append_queue_depth"));
        assert!(metrics.contains("replica_role=\"voter\""));
        assert!(metrics.contains("temporalstore_raft_matrixraft_peer_reorder_queue_depth"));
        assert!(metrics.contains("temporalstore_raft_matrixraft_peer_snapshot_installed_index"));
        assert!(metrics.contains("temporalstore_raft_matrixraft_wal_segment_count"));
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

    fn start_finish_load_server(callbacks: Arc<Mutex<Vec<LoadFinishRequest>>>) -> String {
        let meta_addr = free_local_addr();
        let bind_addr = meta_addr.clone();
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/shards/finish_load") => {
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
