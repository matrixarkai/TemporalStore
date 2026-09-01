// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use temporalstore_rust::context_workflow::{
    context_pipeline_manage_report, context_workflow_state_report, default_context_model_providers,
    embed_drainer_config_from_env, embed_drainer_enabled, extract_context, ingest_extract_context,
    inject_context, retrieve_context, run_embed_drainer_loop, ContextExtractRequest,
    ContextIngestExtractRequest, ContextInjectRequest, ContextModelProviderConfig,
    ContextRetrieveRequest,
};
use temporalstore_rust::ContextProviderKind;
use temporalstore_rust::data_node::{DataNodeLifecycleSnapshot, DataNodeTopologyValidationReport};
use temporalstore_rust::engine::reports::{StorageManagerCycleReport, StorageManagerCycleRequest};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::http::{
    get_bytes_with_headers, json_response, parse_json, post_json,
    post_json_with_options_and_headers, serve_with_stream_handler, HttpRequest,
    HttpRequestOptions, StreamAction, StreamTransfer,
};
use std::io::Read as _;
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
    ReplicatedExecuteRequest, ShardId, Status,
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
    SharedStoreReplicationError, SharedStoreReplicator, SharedStoreWalEntry, StorageBackend,
    BucketDumpManifest, StorageCacheInvalidateBucketRequest, StorageLifecycleRequest,
    StorageProductionReadinessRequest, StreamReadRequest, UnloadShardRequest,
};
use temporalstore_snapshot::object_store::{MatrixObjectHttpStore, ObjectStore};
use temporalstore_snapshot::{FileObjectStore, S3SnapshotStore};
use bytes::Bytes;
use tracing::{debug, error, info, warn};

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
    temporalstore_rust::telemetry::init();
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
    // Whether this node already holds local shard state on disk, captured
    // BEFORE the engine constructs/loads (which may create empty dir
    // scaffolding). Drives matrixobject recovery: absent local state (a fresh
    // node, or one whose local dirs were wiped) is rebuilt from shared storage,
    // while intact local state is left untouched so non-idempotent commands are
    // not double-applied. Used by both the node-local (feature-gated) and the
    // networked (default-feature) matrixobject durability paths.
    let local_state_present =
        local_shard_state_present(&[&cache_dir, &block_store_dir, &index_dir]);
    let block_store_options = block_store_options_from_env();
    // Resolved BEFORE the engine is built, because the cache's shape depends on it: the disk
    // cache tier exists to span the distance to shared storage, and there is no such distance
    // when the durable copy is this node's own disk. Resolving after construction (where this
    // used to sit) meant every deployment paid for a disk tier whether or not it could help.
    let storage_decision = temporalstore_rust::StorageBackendConfig::from_env().resolve_decision();
    let storage_backend = storage_decision.backend.clone();
    // Kept alongside the backend so /metrics can publish why this node chose it. The log
    // line below is not reachable from a portal, an operator's browser, or a dashboard.
    let storage_reason = storage_decision.reason.clone();
    let disk_cache_tier = storage_backend.wants_disk_cache_tier();
    info!(
        backend = %storage_backend.describe(),
        disk_cache_tier,
        "cache tier follows the storage backend"
    );
    let engine = TemporalEngine::with_local_dirs_block_store_options_and_disk_cache(
        cache_memory_bytes,
        cache_dir,
        block_store_dir,
        index_dir,
        block_store_options,
        disk_cache_tier,
    );
    // Join-empty mode (TS_SERVER_JOIN_EMPTY): register as a server but load and
    // self-register no shard, waiting for the metaserver to place shards here via
    // `/load`. This is the clean path for metaserver-driven placement onto a fresh
    // node (as opposed to a node self-declaring ownership of its TS_SHARD_ID). OFF
    // by default, so normal single-shard startup is unchanged.
    let join_empty = env_bool("TS_SERVER_JOIN_EMPTY", false);
    // Networked shared-store cross-node data-follow: a FRESH node (no local state)
    // configured with a `matrixobject://` shared store restores its on-disk index from
    // the newest shared checkpoint BEFORE it loads, so the load reads the restored
    // index into memory and the node auto-serves the followed data with NO manual
    // `/load`. In that one case the startup load is DEFERRED into the restore wiring
    // below (`wire_matrixobject_networked_durability`), which installs the index first,
    // then loads (observing it), then replays the shared WAL tail on top. Every other
    // startup — no shared URI, or a node with intact local state, or join-empty
    // placement — loads here exactly as before, so default behavior is unchanged.
    let networked_uri = matrixobject_networked_uri();
    let defer_startup_load_to_networked_restore =
        !join_empty && networked_uri.is_some() && !local_state_present;
    if !join_empty && !defer_startup_load_to_networked_restore {
        let startup_load = startup_load_shard_request(shard_id, node_id);
        let load_response = engine.load_shard_with(startup_load);
        if !load_response.status.ok {
            error!(
                shard_id,
                message = %load_response.status.message,
                "startup shard load failed"
            );
        }
    } else if join_empty {
        info!("join-empty datanode: awaiting metaserver shard placement");
    } else {
        info!(
            shard_id,
            "fresh node with networked shared store: deferring startup load until shared index is restored"
        );
    }
    // Resolve the distributed storage/replication backend for this node:
    // matrixobject shared storage when detected, else a configured shared object
    // store, else raft replication (the default when nothing is configured).
    // `auto` (the default) probes a configured TS_MATRIXOBJECT_ENDPOINT and only
    // selects the shared MatrixObject store when it is reachable, otherwise it
    // degrades to shared-path/raft — the `reason` records exactly which and why.
    info!(
        backend = %storage_backend.describe(),
        replication = ?storage_backend.replication_mode(),
        reason = %storage_decision.reason,
        "resolved storage backend"
    );
    // Construct the shared object store early so a broken shared-storage config
    // fails fast at startup rather than on the first write.
    match storage_backend.build_shared_object_store() {
        Ok(Some(_shared_store)) => info!(
            backend = %storage_backend.describe(),
            "shared-storage backend ready — shard durability served by shared storage"
        ),
        Ok(None) => {}
        Err(err) => {
            error!(
                backend = %storage_backend.describe(),
                %err,
                "configured storage backend is unusable"
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

    // Async embedding drainer (gated by MATRIXARK_EMBED_DRAINER, default off).
    // Attaches vectors to nodes left embedding-dirty by raw-first bulk ingest or a
    // live-path embed failure, so a bulk-loaded store becomes semantically
    // retrievable without slowing ingest. Scans only the pending dirty set
    // (O(pending)); the interval is a short idle fallback (MATRIXARK_EMBED_DRAINER_INTERVAL_MS).
    if embed_drainer_enabled() {
        let drainer_engine = engine.clone();
        // Provider: a configured OpenAI-compatible embed server (MATRIXARK_EMBED_BASE_URL)
        // else the default deterministic provider (safe offline; real vectors need a
        // real server + MATRIXARK_REQUIRE_MODEL_EMBEDDINGS).
        let provider = match std::env::var("MATRIXARK_EMBED_BASE_URL").ok() {
            Some(base_url) if !base_url.trim().is_empty() => ContextModelProviderConfig {
                provider_name: "embed-drainer".to_string(),
                provider_kind: ContextProviderKind::OpenAiCompatible,
                base_url,
                api_key_env: std::env::var("MATRIXARK_EMBED_API_KEY_ENV").unwrap_or_default(),
                embedding_model: std::env::var("MATRIXARK_EMBEDDING_MODEL")
                    .unwrap_or_else(|_| "all-MiniLM-L6-v2".to_string()),
                mock_mode: false,
                ..ContextModelProviderConfig::default()
            },
            _ => ContextModelProviderConfig::default(),
        };
        let drainer_config = embed_drainer_config_from_env(shard_id, 0, provider);
        println!(
            "embed drainer enabled: shard {shard_id}, batch {}, interval {}ms",
            drainer_config.batch_size,
            drainer_config.interval.as_millis()
        );
        std::thread::spawn(move || {
            run_embed_drainer_loop(&drainer_engine, &drainer_config, || false);
        });
    }
    // Wire the durable matrixobject shared store into the running node: replay
    // shard data from shared storage when local state is absent, and mirror
    // every accepted write to shared storage from here on. Kept alive for the
    // process lifetime so the durability runtime/sink outlive request handling.
    // Only active for the matrixobject shared-storage backend; every other
    // backend (raft/local/shared-path) is completely unchanged.
    //
    // Networked cross-node lazy data-follow: when `TS_SHARED_STORE_URI` names a
    // networked matrixobject object-store service, wire the *networked* durability
    // path (lazy checkpoint restore + WAL-tail replay + sync write mirroring) so
    // shard data follows shards across nodes. This path compiles under default
    // features (`MatrixObjectHttpStore` needs no enterprise crate). Absent the URI
    // it is a complete no-op and behavior is byte-identical.
    let _matrixobject_networked_durability = networked_uri.as_ref().and_then(|uri| {
        wire_matrixobject_networked_durability(
            uri,
            &storage_backend,
            &engine,
            &runtime,
            shard_id,
            node_id,
            local_state_present,
            !join_empty,
        )
    });
    #[cfg(feature = "matrixobject")]
    let _matrixobject_durability_rt = if networked_uri.is_some() {
        // The networked path owns shard durability; skip the node-local on-disk path
        // so writes are not mirrored twice.
        None
    } else {
        wire_matrixobject_durability(&storage_backend, &engine, &runtime, shard_id, local_state_present)
    };

    let location = std::env::var("TS_SERVER_LOCATION").unwrap_or_default();
    let binary_version = env!("CARGO_PKG_VERSION").to_string();
    // Milliseconds since the epoch when this process started, reported on every
    // heartbeat so the metaserver can see an in-place restart.
    //
    // This used to send a literal 0. The metaserver anchors on the first value a server
    // reports and then treats a DIFFERENT, non-zero one as a restart -- so a constant 0
    // anchored at 0 and matched forever, and no datanode restart was ever detected. It
    // failed quietly: a restarted datanode has dropped every shard the metaserver still
    // believes it serves, so routing keeps going there and returns misses that read like
    // lost data rather than a restart. Everything downstream of the verdict -- reboot
    // conviction in the failure detector, the reboot grace in the shard check -- never ran
    // either, while their tests passed because they set the flag on a fixture by hand.
    let boot_time_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since_epoch| since_epoch.as_millis() as u64)
        .unwrap_or_default();
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
        info!(
            shard_id,
            addr = %advertised_addr,
            "standalone datanode: no metaserver — serving shard locally"
        );
    } else {
        let server_registration = RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: advertised_addr.clone(),
            node_id,
            location,
            binary_version: binary_version.clone(),
        };
        match post_json_with_options_and_headers::<_, AckResponse>(
            &meta_addr,
            "/servers/register",
            &server_registration,
            &temporalstore_rust::meta::admin_auth_header(),
            HttpRequestOptions::default(),
        ) {
            Ok(response) if response.status.ok => {
                info!(server = %advertised_addr, meta = %meta_addr, "registered server with metaserver");
            }
            Ok(response) => {
                warn!(
                    server = %advertised_addr,
                    message = %response.status.message,
                    "metaserver rejected server registration"
                );
            }
            Err(err) => {
                warn!(server = %advertised_addr, %err, "failed to register server");
            }
        }

        if !join_empty {
            let registration = RegisterShardRequest {
                registered_at_ms: 0,
                shard_id,
                server_addr: advertised_addr.clone(),
            };
            match post_json_with_options_and_headers::<_, RegisterShardResponse>(
                &meta_addr,
                "/register_shard",
                &registration,
                &temporalstore_rust::meta::admin_auth_header(),
                HttpRequestOptions::default(),
            ) {
                Ok(response) if response.status.ok => {
                    info!(shard_id, meta = %meta_addr, "registered shard with metaserver");
                }
                Ok(response) => {
                    warn!(
                        shard_id,
                        message = %response.status.message,
                        "metaserver rejected shard registration"
                    );
                }
                Err(err) => {
                    warn!(shard_id, %err, "failed to register shard");
                }
            }
        }

        start_heartbeat_loop(
            engine.clone(),
            runtime.clone(),
            meta_addr.clone(),
            advertised_addr.clone(),
            binary_version.clone(),
            heartbeat_interval_ms,
            boot_time_ms,
        );
    }
    // Streamed large-file / attachment tier: POST /blob/<key> writes the request
    // body to the blob store in chunks via the ObjectStore::append_blob path (the
    // same primitive shared_store's append_blob_with_retry uses); GET /blob/<key>
    // streams it back. Wired to a FileObjectStore here because the matrixobject
    // feature is optional; a MatrixObject store drops in behind the same trait.
    let blob_store = Arc::new(FileObjectStore::new(PathBuf::from(&blob_store_dir)));
    let blob_chunk_bytes = env_usize("TS_BLOB_CHUNK_BYTES", 1024 * 1024).max(1);

    // Cross-peer blob availability (opt-in, TS_BLOB_PEER_FETCH): on a local
    // `GET /blob/<key>` MISS, fetch the blob from a raft peer that has it, serve it
    // to the caller, and cache it locally (read-through). This makes large-file
    // attachments available cluster-wide in multi-node raft mode WITHOUT full
    // replication.
    //
    // AVAILABILITY, NOT DURABILITY: peer-fetch only lets any node *serve* a blob
    // that already exists on SOME live peer. It provides no redundancy — if the one
    // node holding a blob dies before another node has fetched (and thereby cached)
    // it, the blob is gone. Real durability requires explicit replication or the
    // enterprise object store; this feature is deliberately scoped to availability.
    //
    // The peer address list comes from the raft `peer_map()` (every node's
    // advertised addr except this one — the same addr each node serves its `/blob`
    // tier on). `None` when raft is off or the cluster is single-node, so
    // standalone nodes skip peer-fetch entirely and behavior is byte-identical to
    // today (404 on local miss).
    let blob_peer_addrs: Option<Arc<Vec<String>>> = if env_bool("TS_BLOB_PEER_FETCH", false) {
        raft_state
            .as_ref()
            .map(|state| state.runtime.peer_addrs())
            .filter(|peers| !peers.is_empty())
            .map(Arc::new)
    } else {
        None
    };
    if let Some(peers) = &blob_peer_addrs {
        info!(
            peers = peers.len(),
            "cross-peer blob availability enabled (read-through peer-fetch on local miss)"
        );
    }
    let blob_peer_timeout_ms = env_u64("TS_BLOB_PEER_FETCH_TIMEOUT_MS", 5_000);
    let blob_runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(env_usize("TS_BLOB_RUNTIME_THREADS", 4))
            .enable_all()
            .build()
            .expect("blob tokio runtime should start"),
    );

    // Shared-storage shard-data mover (opt-in, TS_AUTO_REBALANCE_DATA_MOVE): when a
    // shared-path backend is configured, a shard reassigned to this node restores
    // its data from shared storage on `/load`, and `/shard/publish_checkpoint`
    // publishes the shard's durable state so a future owner can restore it. OFF by
    // default, so single-node/standalone durability behavior is unchanged.
    let shard_replicator: Option<Arc<SharedStoreReplicator<FileObjectStore>>> =
        if env_bool("TS_AUTO_REBALANCE_DATA_MOVE", false) {
            match &storage_backend {
                StorageBackend::SharedPath { root, cluster_id } => {
                    let store = Arc::new(FileObjectStore::new(root.clone()));
                    info!(
                        checkpoints = %root.display(),
                        cluster = %cluster_id,
                        "shard data movement enabled: shared-storage checkpoints"
                    );
                    Some(Arc::new(SharedStoreReplicator::new(
                        cluster_id.clone(),
                        store,
                    )))
                }
                other => {
                    warn!(
                        backend = %other.describe(),
                        "TS_AUTO_REBALANCE_DATA_MOVE set but backend is not a shared path — data movement disabled"
                    );
                    None
                }
            }
        } else {
            None
        };

    info!(%addr, "temporalstore server listening");
    // Streaming pre-handler: owns `/blob/<key>` and moves bytes straight between
    // the socket and the object store (no full-body buffering). Everything else
    // is Declined and falls through to the buffered handler below.
    let stream_blob_store = Arc::clone(&blob_store);
    let stream_blob_runtime = Arc::clone(&blob_runtime);
    let stream_blob_peer_addrs = blob_peer_addrs.clone();
    let stream_blob_peer_timeout_ms = blob_peer_timeout_ms;
    let handler_replicator = shard_replicator.clone();
    let handler_block_runtime = Arc::clone(&blob_runtime);
    let stream_chunk_bytes = blob_chunk_bytes;
    if let Err(err) = serve_with_stream_handler(
        &addr,
        move |head, transfer| {
            handle_blob_stream(
                head,
                transfer,
                &stream_blob_store,
                &stream_blob_runtime,
                stream_chunk_bytes,
                stream_blob_peer_addrs.as_deref().map(Vec::as_slice),
                stream_blob_peer_timeout_ms,
            )
        },
        move |request| {
        debug!(method = %request.method, path = %request.path, "serving request");
        if let Some(response) = handle_ping_route(&request) {
            return response;
        }
        if let Some(response) = handle_readiness_route(&request) {
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
                append_storage_backend_metric(&mut metrics, &storage_backend, &storage_reason);
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
                    boot_time_ms,
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
            ("POST", "/shard/publish_checkpoint") => {
                match parse_json::<PublishShardCheckpointRequest>(&request.body) {
                    Ok(req) => json_response(
                        200,
                        &publish_shard_checkpoint(
                            &handler_replicator,
                            &handler_block_runtime,
                            &engine,
                            req.shard_id,
                        ),
                    ),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("POST", "/load") => match parse_json::<LoadShardRequest>(&request.body) {
                Ok(req) => {
                    // A shard reassigned here restores its data from shared storage
                    // (when a shared backend is configured). Only a foreign shard (no
                    // local WAL) restores, so a node's own shard is never double-applied.
                    // Ordering matters: restore the on-disk index from the shared
                    // checkpoint BEFORE the load so the load reads the restored index
                    // into memory and the shard serves immediately; then replay the
                    // shared WAL tail on top AFTER the shard is loaded (the tail applies
                    // through execute, which needs a loaded shard).
                    let shard_id = req.shard_id;
                    let had_local_wal = engine
                        .write_ahead_log_store()
                        .stats(shard_id)
                        .last_sequence
                        > 0;
                    let restore_after_wal_index = if had_local_wal {
                        None
                    } else {
                        restore_shared_index_before_load(
                            &handler_replicator,
                            &handler_block_runtime,
                            &engine,
                            shard_id,
                        )
                    };
                    let load_response = runtime.load_shard_with(req);
                    if load_response.status.ok && !had_local_wal {
                        replay_shared_wal_tail(
                            &handler_replicator,
                            &handler_block_runtime,
                            &engine,
                            shard_id,
                            restore_after_wal_index.unwrap_or(0),
                        );
                    }
                    json_response(200, &load_response)
                }
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
        },
    ) {
        error!(%err, "server serve loop exited");
        std::process::exit(1);
    }
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

/// Streamed attachment endpoint (zero-copy / no full-body buffering).
///
/// `POST|PUT /blob/<key>` reads the request body off the socket in `chunk_bytes`
/// slices and appends each slice straight into `ObjectStore::append_blob`, so
/// neither the request body nor the object is ever materialized whole in memory;
/// returns a JSON receipt. `GET /blob/<key>` `stat`s the stored object, sends the
/// `Content-Length`, then streams the file to the socket in `chunk_bytes` slices.
///
/// The handler owns every `/blob/<key>` request (always returns
/// `StreamAction::Handled`); any other path is `Declined` so the buffered handler
/// runs. On the error / non-blob-method paths it drains the request body first so
/// the kept-alive connection stays framed.
///
/// Cross-peer availability: when `peer_addrs` is `Some` (raft mode +
/// `TS_BLOB_PEER_FETCH`), a `GET` that misses locally is retried against each peer
/// in turn; the first peer to return 200 has its bytes streamed back to the caller
/// AND cached locally via `append_blob` (read-through), so subsequent reads are
/// local. A request that itself arrives carrying the loop-guard header
/// (`head.blob_peer_fetch_loop_guard`) is served local-only and NEVER re-forwarded,
/// which is what stops peers from fetching from each other forever.
fn handle_blob_stream(
    head: &temporalstore_rust::http::RequestHead,
    transfer: &mut StreamTransfer,
    blob_store: &Arc<FileObjectStore>,
    runtime: &Arc<tokio::runtime::Runtime>,
    chunk_bytes: usize,
    peer_addrs: Option<&[String]>,
    peer_timeout_ms: u64,
) -> StreamAction {
    let Some(key) = head.path.strip_prefix("/blob/") else {
        return StreamAction::Declined;
    };
    match head.method.as_str() {
        "POST" | "PUT" => {
            if key.is_empty() {
                let _ = transfer.drain_body();
                write_stream_json(
                    transfer,
                    400,
                    &Status::error("bad_request", "missing blob key"),
                );
                return StreamAction::Handled;
            }
            let key = key.to_string();
            match stream_blob_upload(transfer, blob_store, runtime, chunk_bytes, &key) {
                Ok((bytes_written, object_length, chunks)) => write_stream_json(
                    transfer,
                    200,
                    &BlobReceipt {
                        status: Status::ok(),
                        key,
                        bytes_written,
                        object_length,
                        chunks,
                    },
                ),
                Err(err) => {
                    write_stream_json(transfer, 500, &Status::error("blob_write_failed", err))
                }
            }
            StreamAction::Handled
        }
        "GET" => {
            if key.is_empty() {
                write_stream_json(
                    transfer,
                    400,
                    &Status::error("bad_request", "missing blob key"),
                );
                return StreamAction::Handled;
            }
            match stream_blob_download(transfer, blob_store, chunk_bytes, key) {
                Ok(true) => {}
                Ok(false) => {
                    // Local miss. Try cross-peer fetch unless this request is
                    // itself a peer-fetch hop (loop guard) or peer-fetch is off.
                    let peers = if head.blob_peer_fetch_loop_guard {
                        None
                    } else {
                        peer_addrs
                    };
                    match peers.and_then(|peers| {
                        peer_fetch_blob(
                            transfer,
                            blob_store,
                            runtime,
                            chunk_bytes,
                            key,
                            peers,
                            peer_timeout_ms,
                        )
                    }) {
                        // A peer served the blob (already streamed to the caller).
                        Some(Ok(())) => {}
                        // A peer had it but the socket broke mid-stream to the
                        // caller: head already sent, nothing to recover.
                        Some(Err(_)) => {}
                        // No peer had it (or peer-fetch disabled): 404 as today.
                        None => write_stream_json(
                            transfer,
                            404,
                            &Status::error("blob_not_found", key.to_string()),
                        ),
                    }
                }
                // A mid-stream socket error: the head is already sent, nothing to
                // recover; drop the connection.
                Err(_) => {}
            }
            StreamAction::Handled
        }
        _ => {
            let _ = transfer.drain_body();
            write_stream_json(
                transfer,
                405,
                &Status::error("method_not_allowed", "use POST or GET on /blob/<key>"),
            );
            StreamAction::Handled
        }
    }
}

/// Serialize `value` and write it as a complete streamed JSON response.
fn write_stream_json<T: Serialize>(transfer: &mut StreamTransfer, status: u16, value: &T) {
    let (_status, body) = json_response(status, value);
    let _ = transfer.send_head(status, "application/json", body.len());
    let _ = transfer.write_chunk(&body);
    let _ = transfer.flush();
}

/// Stream a `POST|PUT /blob/<key>` body from the socket into the object store in
/// `chunk_bytes` appends. Memory is bounded to one `chunk_bytes` buffer, never
/// the whole upload. Returns `(bytes_written, object_length, chunks)`.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PublishShardCheckpointRequest {
    shard_id: ShardId,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PublishShardCheckpointResponse {
    status: Status,
    #[serde(default)]
    checkpoint_id: Option<String>,
    #[serde(default)]
    page_slab_count: usize,
    #[serde(default)]
    checkpoint_wal_index: u64,
}

/// Replay a reassigned shard's data from the shared object store after a fresh
/// local load. No-op when data movement is disabled. `had_local_wal` guards
/// against double-apply: a node reloading its own shard replays its local WAL
/// during load, so shared replay runs only for a foreign shard placed here by the
/// metaserver (which has no local WAL). A missing shared WAL is not an error.
/// Restore the served INDEX + a lazy slab address map (no slab bytes) from the newest
/// shared checkpoint onto the on-disk base, so a subsequent `load_shard_with` reads the
/// restored index into memory and the shard serves the followed data immediately. Old
/// pages are read lazily through the block store's shared read-through on first access.
/// Returns the checkpoint's WAL index (the watermark for the post-load tail replay);
/// `None` when no shared replicator is configured. This MUST run BEFORE the load so the
/// load observes the restored index — running it after leaves an empty in-memory shard
/// (the ordering bug this split fixes). With no checkpoint, returns `Some(0)` and the
/// caller replays the full shared WAL after loading an empty index (WAL-only mirrors).
fn restore_shared_index_before_load(
    replicator: &Option<Arc<SharedStoreReplicator<FileObjectStore>>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    engine: &TemporalEngine,
    shard_id: ShardId,
) -> Option<u64> {
    let replicator = replicator.as_ref()?;
    let after_wal_index =
        match runtime.block_on(replicator.restore_index_and_page_addresses(
            shard_id,
            engine,
            &engine.block_store(),
        )) {
            Ok(manifest) => {
                info!(
                    shard_id,
                    checkpoint_id = %manifest.checkpoint_id,
                    checkpoint_wal_index = manifest.checkpoint_wal_index,
                    page_slabs = manifest.page_slabs.len(),
                    "restored shard index and lazy page addresses from shared checkpoint (pre-load)"
                );
                manifest.checkpoint_wal_index
            }
            Err(SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            Err(err) => {
                warn!(shard_id, %err, "shared checkpoint restore failed; replaying full shared WAL");
                0
            }
        };
    // Inherit the dump lineage along with the data. Without it this node treats every live
    // generation as never dumped, which blocks WAL reclaim until it has re-dumped the whole
    // shard itself. Advisory: the data is already restored and serving, so a lineage that
    // cannot be read costs re-dumping, not correctness.
    match runtime.block_on(replicator.restore_bucket_dump_manifests(shard_id, engine)) {
        Ok(0) => {}
        Ok(restored) => info!(
            shard_id,
            restored, "inherited bucket-dump manifests from shared storage"
        ),
        Err(err) => warn!(
            shard_id, %err,
            "could not restore bucket-dump manifests; this node will re-dump before it can reclaim"
        ),
    }
    Some(after_wal_index)
}

/// Replay the shared WAL tail (records after `after_wal_index`) on top of the already
/// loaded shard, applying each through `engine.execute` — which is why this runs AFTER
/// the shard is loaded. No-op when no shared replicator is configured.
fn replay_shared_wal_tail(
    replicator: &Option<Arc<SharedStoreReplicator<FileObjectStore>>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    engine: &TemporalEngine,
    shard_id: ShardId,
    after_wal_index: u64,
) {
    let Some(replicator) = replicator else {
        return;
    };
    match runtime.block_on(replicator.replay_wal(shard_id, after_wal_index, engine)) {
        Ok(report) => {
            if report.applied > 0 {
                info!(
                    shard_id,
                    records = report.applied,
                    wal_index = report.last_wal_index,
                    after_wal_index,
                    "replayed shard WAL tail from shared storage"
                );
            }
        }
        Err(err) => warn!(shard_id, %err, "no shared-storage data replayed for shard"),
    }
}
/// Read the blocks a set of results points at, so they can travel with them.
///
/// A successor installs an address; it can only serve that address if the bytes are reachable.
/// Locally they are in the block store. Across nodes they are not, unless something carries them.
fn gather_result_pages(
    engine: &TemporalEngine,
    shard_id: ShardId,
    outcomes: &[temporalstore_rust::wal::WalOutcomeItem],
) -> Vec<temporalstore_rust::wal::StagedPage> {
    let mut pages = Vec::new();
    for item in outcomes {
        let Some(address) = item.resolved_address() else {
            continue;
        };
        if let Ok(bytes) = engine.block_store().read(&address) {
            pages.push(temporalstore_rust::wal::StagedPage {
                object_id: item.object_id,
                bytes,
            });
        }
    }
    let _ = shard_id;
    pages
}


/// Publish this node's data for `shard_id` to the shared object store so a future
/// owner can replay it. Reads the shard's write-ahead log (read-only — never
/// disturbs the live shard) and mirrors each record as a shared-store WAL entry.
/// Returns an error status when data movement is not enabled.
fn publish_shard_checkpoint(
    replicator: &Option<Arc<SharedStoreReplicator<FileObjectStore>>>,
    runtime: &Arc<tokio::runtime::Runtime>,
    engine: &TemporalEngine,
    shard_id: ShardId,
) -> PublishShardCheckpointResponse {
    let Some(replicator) = replicator else {
        return PublishShardCheckpointResponse {
            status: Status::error(
                "shared_store_disabled",
                "shard data movement is not enabled on this node",
            ),
            checkpoint_id: None,
            page_slab_count: 0,
            checkpoint_wal_index: 0,
        };
    };
    // Read the shard's WAL records (read-only) and mirror each to shared storage.
    let records = match engine
        .write_ahead_log_store()
        .scan(shard_id, 0, u64::MAX, u64::MAX)
    {
        Ok(records) => records,
        Err(err) => {
            return PublishShardCheckpointResponse {
                status: Status::error("wal_scan_failed", err.to_string()),
                checkpoint_id: None,
                page_slab_count: 0,
                checkpoint_wal_index: 0,
            };
        }
    };
    let mut published = 0usize;
    let mut last_wal_index = 0u64;
    for (_offset, line) in records {
        let record = match temporalstore_rust::wal::decode_wal_line(&line) {
            Ok(record) => record,
            Err(err) => {
                return PublishShardCheckpointResponse {
                    status: Status::error("wal_decode_failed", err.to_string()),
                    checkpoint_id: None,
                    page_slab_count: published,
                    checkpoint_wal_index: last_wal_index,
                };
            }
        };
        let entry = SharedStoreWalEntry {
            shard_id,
            wal_index: record.sequence,
            command: record.command,
            // What the write did travels with it. A successor installs these rather than
            // re-running the command against its own clock and its own config.
            outcomes: record.outcomes.clone(),
            // The pages the results point at, so a successor can actually READ what it
            // installs. A result names an address in THIS node's block store; a successor has
            // its own, and the checkpoint only covers what was written before it. Without this
            // the successor's index is right and every read of the tail returns nothing.
            //
            // Empty on the local record for a synchronous write -- that page went to the block
            // store rather than into the record -- so they are gathered here.
            staged_pages: if record.staged_pages.is_empty() {
                gather_result_pages(engine, shard_id, &record.outcomes)
            } else {
                record.staged_pages
            },
        };
        if let Err(err) = runtime.block_on(replicator.publish_wal_entry(entry)) {
            return PublishShardCheckpointResponse {
                status: Status::error("publish_wal_failed", err.to_string()),
                checkpoint_id: None,
                page_slab_count: published,
                checkpoint_wal_index: last_wal_index,
            };
        }
        published += 1;
        last_wal_index = record.sequence;
    }
    // Send the dump lineage before the checkpoint manifest, so the checkpoint manifest is
    // still the last object written and therefore still the commit point for a restore.
    // Advisory on failure: the checkpoint alone is what a restore needs today, and failing
    // the whole publish over the lineage would make this strictly worse than not having it.
    match runtime.block_on(
        replicator.publish_bucket_dump_manifests(
            shard_id,
            &engine.list_bucket_dump_manifests(shard_id),
        ),
    ) {
        Ok(0) => {}
        Ok(count) => info!(
            shard_id,
            manifests = count,
            "published bucket-dump manifests to shared storage"
        ),
        Err(err) => warn!(
            shard_id, %err,
            "could not publish bucket-dump manifests; checkpoint still published without lineage"
        ),
    }
    // Publish a real metadata+slab checkpoint at the current last-applied WAL index so
    // a future owner can lazily restore (index + slab addresses) and replay only the
    // WAL tail after this index, rather than replaying the full history. Reuses the
    // shared WAL mirrored above for the tail.
    let checkpoint = match runtime.block_on(replicator.publish_checkpoint(
        shard_id,
        last_wal_index,
        engine,
        &engine.block_store(),
    )) {
        Ok(manifest) => manifest,
        Err(err) => {
            return PublishShardCheckpointResponse {
                status: Status::error("publish_checkpoint_failed", err.to_string()),
                checkpoint_id: None,
                page_slab_count: published,
                checkpoint_wal_index: last_wal_index,
            };
        }
    };
    PublishShardCheckpointResponse {
        status: Status::ok(),
        checkpoint_id: Some(checkpoint.checkpoint_id),
        page_slab_count: checkpoint.page_slabs.len(),
        checkpoint_wal_index: last_wal_index,
    }
}

fn stream_blob_upload(
    transfer: &mut StreamTransfer,
    blob_store: &Arc<FileObjectStore>,
    runtime: &Arc<tokio::runtime::Runtime>,
    chunk_bytes: usize,
    key: &str,
) -> Result<(u64, u64, u64), String> {
    // Replace any prior object so an upload is idempotent.
    runtime.block_on(async {
        let _ = blob_store.delete(key).await;
    });
    let mut buf = vec![0u8; chunk_bytes];
    let mut filled = 0usize;
    let mut bytes_written = 0u64;
    let mut object_length = 0u64;
    let mut chunks = 0u64;
    loop {
        if filled == buf.len() {
            let receipt = runtime
                .block_on(blob_store.append_blob(key, Bytes::copy_from_slice(&buf[..filled])))
                .map_err(|err| err.to_string())?;
            bytes_written += receipt.bytes_written;
            object_length = receipt.object_length;
            chunks += 1;
            filled = 0;
        }
        let n = transfer
            .read_body(&mut buf[filled..])
            .map_err(|err| err.to_string())?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    if filled > 0 {
        let receipt = runtime
            .block_on(blob_store.append_blob(key, Bytes::copy_from_slice(&buf[..filled])))
            .map_err(|err| err.to_string())?;
        bytes_written += receipt.bytes_written;
        object_length = receipt.object_length;
        chunks += 1;
    }
    // An empty body still creates a zero-length object so a later GET succeeds.
    if chunks == 0 {
        let receipt = runtime
            .block_on(blob_store.append_blob(key, Bytes::new()))
            .map_err(|err| err.to_string())?;
        object_length = receipt.object_length;
    }
    Ok((bytes_written, object_length, chunks))
}

/// Stream a `GET /blob/<key>` response: `stat` the object, send `Content-Length`,
/// then copy the file to the socket in `chunk_bytes` slices. Returns `Ok(false)`
/// (before any head is sent) when the object does not exist so the caller can
/// send a 404.
fn stream_blob_download(
    transfer: &mut StreamTransfer,
    blob_store: &Arc<FileObjectStore>,
    chunk_bytes: usize,
    key: &str,
) -> std::io::Result<bool> {
    let path = match blob_store.object_path(key) {
        Ok(path) => path,
        Err(_) => return Ok(false),
    };
    let mut file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    let length = file.metadata()?.len() as usize;
    transfer.send_head(200, "application/octet-stream", length)?;
    let mut buf = vec![0u8; chunk_bytes];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        transfer.write_chunk(&buf[..n])?;
    }
    transfer.flush()?;
    Ok(true)
}

/// Cross-peer blob availability read-through. On a local `GET /blob/<key>` MISS,
/// query each raft peer in order for `GET /blob/<key>`, tagging the request with the
/// `X-Ts-Blob-Peer-Fetch: 0` loop-guard header so the queried peer serves local-only
/// and never re-forwards. The FIRST peer that returns the object wins: its bytes are
/// cached locally via `append_blob` (read-through, so subsequent reads are local) and
/// then streamed back to the original caller.
///
/// Returns:
/// - `Some(Ok(()))`  — a peer had the blob; it was cached and streamed to the caller.
/// - `Some(Err(_))`  — a peer had the blob and the 200 head was sent, but the socket
///                     to the caller broke mid-stream (unrecoverable; drop the conn).
/// - `None`          — no peer had the blob; the caller should send a 404 (as today).
///
/// Memory note: the peer response is buffered whole before being re-streamed, because
/// the raw peer HTTP client returns the complete body. For the attachment tier this
/// bounds peer-fetch memory to one blob per in-flight miss; a fully streamed pass-
/// through would require a chunked peer client, which the raw client does not provide.
/// This is an AVAILABILITY mechanism, not a durability one — see the wiring comment in
/// `main`.
fn peer_fetch_blob(
    transfer: &mut StreamTransfer,
    blob_store: &Arc<FileObjectStore>,
    runtime: &Arc<tokio::runtime::Runtime>,
    chunk_bytes: usize,
    key: &str,
    peer_addrs: &[String],
    peer_timeout_ms: u64,
) -> Option<std::io::Result<()>> {
    let options = HttpRequestOptions {
        connect_timeout_ms: 200,
        io_timeout_ms: peer_timeout_ms.max(1),
        max_retries: 0,
    };
    let path = format!("/blob/{key}");
    for peer in peer_addrs {
        // Peer addrs are advertised host:port; tolerate an accidental scheme.
        let peer = peer
            .strip_prefix("http://")
            .or_else(|| peer.strip_prefix("https://"))
            .unwrap_or(peer.as_str());
        // Loop guard: the queried peer MUST serve local-only and never re-forward.
        let bytes =
            match get_bytes_with_headers(peer, &path, "X-Ts-Blob-Peer-Fetch: 0\r\n", options) {
                Ok(bytes) => bytes,
                // This peer missed (404 -> non-200 BadResponse) or was unreachable;
                // try the next peer.
                Err(_) => continue,
            };
        info!(
            %key,
            peer,
            bytes = bytes.len(),
            "cross-peer blob fetch hit; caching locally (read-through) and serving"
        );
        // Read-through cache: replace any partial then append the fetched bytes so a
        // subsequent GET is served locally. Best-effort — a cache-write failure still
        // serves this response (availability is preserved; the blob is simply
        // re-fetched next time).
        runtime.block_on(async {
            let _ = blob_store.delete(key).await;
            if let Err(err) = blob_store
                .append_blob(key, Bytes::copy_from_slice(&bytes))
                .await
            {
                warn!(%key, %err, "peer-fetched blob cache write failed; serving without caching");
            }
        });
        // Stream the buffered bytes back to the original caller.
        let result = (|| -> std::io::Result<()> {
            transfer.send_head(200, "application/octet-stream", bytes.len())?;
            for chunk in bytes.chunks(chunk_bytes.max(1)) {
                transfer.write_chunk(chunk)?;
            }
            transfer.flush()
        })();
        return Some(result);
    }
    None
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

/// `true` when any of the local shard dirs already holds on-disk state.
///
/// Used to decide matrixobject recovery: an empty/absent set of dirs means a
/// fresh node or one whose local dirs were wiped, so its shard is rebuilt from
/// shared storage; a non-empty set means intact local state that must not be
/// replayed over (some commands are not idempotent).
fn local_shard_state_present(dirs: &[&str]) -> bool {
    dirs.iter().any(|dir| match std::fs::read_dir(dir) {
        Ok(mut entries) => entries.next().is_some(),
        Err(_) => false,
    })
}

/// Durable shared-storage sink backed by a matrixobject WAL writer.
///
/// The datanode's request/worker threads are plain OS threads, so blocking on
/// the dedicated durability runtime is safe. In the default **async** mode
/// `record_write` only enqueues the entry (cheap — a lock + push) and returns;
/// a background task on the same runtime drains the queue in batches off the
/// write critical path. The local WAL+page are already durable before the entry
/// is enqueued, so acknowledgement no longer waits on the matrixobject append.
///
/// With `TS_MATRIXOBJECT_SYNC_FLUSH=1` the writer runs in **sync** mode instead:
/// every write is published to the durable store before the write is
/// acknowledged.
#[cfg(feature = "matrixobject")]
struct MatrixObjectWalSink {
    handle: tokio::runtime::Handle,
    writer: std::sync::Arc<
        temporalstore_rust::SharedStoreStorageWriter<
            temporalstore_rust::matrixobject_store::MatrixObjectObjectStore,
        >,
    >,
    /// `false` in sync mode (each write publishes inline); `true` in async mode
    /// (each write enqueues and the background flusher publishes).
    async_mode: bool,
    /// Wake the background flusher when the queue reaches `batch_full`.
    flush_signal: std::sync::Arc<tokio::sync::Notify>,
    batch_full: usize,
}

#[cfg(feature = "matrixobject")]
impl std::fmt::Debug for MatrixObjectWalSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixObjectWalSink").finish_non_exhaustive()
    }
}

#[cfg(feature = "matrixobject")]
impl temporalstore_rust::SharedWalSink for MatrixObjectWalSink {
    fn record_write(&self, shard_id: u64, command: &Command) {
        let writer = std::sync::Arc::clone(&self.writer);
        let command = command.clone();
        // In async mode `write` only locks the queue and pushes (no durable I/O
        // is awaited), so this returns immediately; in sync mode it publishes to
        // the durable store before returning.
        let result = self
            .handle
            .block_on(async move { writer.write(shard_id, command).await });
        if let Err(err) = result {
            // Durability failure: log loudly. The local write already
            // succeeded, so the node stays available; shared storage will catch
            // up on the next successful publish or on the next full replay.
            eprintln!("matrixobject durable write failed for shard {shard_id}: {err}");
        }
        // Threshold flush: if the queue has filled up, wake the flusher now
        // instead of waiting for the next interval tick.
        if self.async_mode && self.writer.queued_len() >= self.batch_full {
            self.flush_signal.notify_one();
        }
    }
}

/// Owns the dedicated durability runtime and the shared-store writer. Its
/// `Drop` drains any queued (async) writes before the runtime shuts down, so a
/// clean stop loses nothing.
#[cfg(feature = "matrixobject")]
struct MatrixObjectDurability {
    rt: tokio::runtime::Runtime,
    writer: std::sync::Arc<
        temporalstore_rust::SharedStoreStorageWriter<
            temporalstore_rust::matrixobject_store::MatrixObjectObjectStore,
        >,
    >,
    shutdown: std::sync::Arc<std::sync::atomic::AtomicBool>,
    async_mode: bool,
    drain_batch: usize,
}

#[cfg(feature = "matrixobject")]
impl Drop for MatrixObjectDurability {
    fn drop(&mut self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if !self.async_mode {
            return;
        }
        // Graceful drain: publish everything still queued before the runtime
        // (and the background flusher) go away. `rt` is dropped after this
        // returns, so the handle is still live here.
        let writer = std::sync::Arc::clone(&self.writer);
        let drain_batch = self.drain_batch;
        let drained = self.rt.handle().block_on(async move {
            let mut total = 0usize;
            loop {
                match writer.flush_pending(drain_batch).await {
                    Ok(report) => {
                        total += report.flushed;
                        if report.remaining == 0 {
                            break;
                        }
                    }
                    Err(err) => {
                        eprintln!("matrixobject shutdown drain failed: {err}");
                        break;
                    }
                }
            }
            total
        });
        if drained > 0 {
            println!("matrixobject durability: drained {drained} queued writes on shutdown");
        }
    }
}

/// Build the durable matrixobject store, replay shard data from it when local
/// state is absent, and attach a write-mirroring sink to `runtime`. Returns a
/// guard that must be kept alive for the process lifetime (it owns the
/// durability runtime and drains queued writes on drop). A no-op (returns
/// `None`) for any backend other than matrixobject.
#[cfg(feature = "matrixobject")]
fn wire_matrixobject_durability(
    storage_backend: &temporalstore_rust::StorageBackend,
    engine: &TemporalEngine,
    runtime: &DataNodeRuntime,
    shard_id: u64,
    local_state_present: bool,
) -> Option<MatrixObjectDurability> {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use temporalstore_rust::matrixobject_store::MatrixObjectObjectStore;
    use temporalstore_rust::{
        SharedStoreReplicator, SharedStoreStorageMode, SharedStoreWalAppendMode, StorageBackend,
    };

    let (bucket, cluster_id) = match storage_backend {
        StorageBackend::MatrixObject { bucket, cluster_id } => (bucket.clone(), cluster_id.clone()),
        // Other backends (raft/local/shared-path) keep their existing behavior.
        _ => return None,
    };

    // TODO(networked-store): when TS_MATRIXOBJECT_ENDPOINT is configured, this
    // durability path must target the *networked* MatrixObject object-store
    // service (see the matching TODO in storage_backend::build_shared_object_store)
    // instead of a node-local on-disk snapshot dir, so shard data follows shards
    // across nodes on rebalance / node loss. The `auto` resolver already probes
    // the endpoint and selects matrixobject only when reachable; wiring the
    // networked ObjectStore impl here (and in build_shared_object_store) is the
    // remaining piece. Until then this stays node-local.
    let store_dir = std::env::var("TS_MATRIXOBJECT_STORE_DIR")
        .unwrap_or_else(|_| "target/temporalstore-matrixobject".to_string());

    // Durable, on-disk matrixobject store: flush-on-commit snapshot that is read
    // back on construction, so its bytes survive a process restart.
    let store = match MatrixObjectObjectStore::with_persistent_dir(&bucket, &store_dir) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("matrixobject durable store at {store_dir} is unusable: {err}");
            std::process::exit(1);
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("failed to start matrixobject durability runtime: {err}");
            std::process::exit(1);
        }
    };

    // `TS_MATRIXOBJECT_SYNC_FLUSH=1` forces every write durable before ack; group commit (the
    // `[wal] group_commit` / `TS_GROUP_COMMIT` gate, default ON) then coalesces concurrent sync
    // writers onto a shared durable barrier. Coalescing requires the single-appendable-log
    // (ProtobufAppendBlob) WAL layout, so select it here when group commit is active on the sync
    // path. `latest_persisted_wal_index` / replay read BOTH layouts (union by WAL index), so a shard
    // that previously wrote the per-key layout continues monotonically with no migration.
    let sync_flush = env_bool("TS_MATRIXOBJECT_SYNC_FLUSH", false);
    let group_commit = sync_flush && temporalstore_rust::wal::group_commit_configured();
    let commit_delay = temporalstore_rust::wal::group_commit_delay();
    let mut replicator = SharedStoreReplicator::new(cluster_id, Arc::new(store));
    if group_commit {
        replicator = replicator.with_wal_append_mode(SharedStoreWalAppendMode::ProtobufAppendBlob);
    }

    let latest = match rt.block_on(replicator.latest_persisted_wal_index(shard_id)) {
        Ok(latest) => latest,
        Err(err) => {
            eprintln!("failed to read matrixobject WAL state for shard {shard_id}: {err}");
            std::process::exit(1);
        }
    };

    if !local_state_present && latest > 0 {
        // S1 checkpoint recovery: restore the served index plus a lazy slab address map from
        // the latest checkpoint, then replay ONLY the WAL tail after it -- old pages are read
        // out of the store on demand. A store with no checkpoint yet (or a failed restore)
        // falls back to the full replay from 0: the old behavior, correct but O(history).
        let after_wal_index = match rt.block_on(replicator.restore_index_and_page_addresses(
            shard_id,
            engine,
            &engine.block_store(),
        )) {
            Ok(manifest) => {
                println!(
                    "restored shard {shard_id} index and lazy page addresses from matrixobject checkpoint {} (WAL tail from {})",
                    manifest.checkpoint_id, manifest.checkpoint_wal_index
                );
                // Read the restored on-disk index into memory before the tail replay, which
                // applies through the engine and needs a loaded shard.
                engine.load_shard(shard_id);
                manifest.checkpoint_wal_index
            }
            Err(temporalstore_rust::SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            Err(err) => {
                eprintln!(
                    "matrixobject checkpoint restore failed for shard {shard_id} ({err}); replaying full WAL"
                );
                0
            }
        };
        match rt.block_on(replicator.replay_wal(shard_id, after_wal_index, engine)) {
            Ok(report) => println!(
                "recovered shard {shard_id} from matrixobject shared storage at {store_dir}: {} WAL entries replayed (through index {})",
                report.applied, report.last_wal_index
            ),
            Err(err) => {
                eprintln!("matrixobject recovery replay failed for shard {shard_id}: {err}");
                std::process::exit(1);
            }
        }
    } else if local_state_present {
        println!(
            "matrixobject durability active for shard {shard_id} at {store_dir}; local state intact, skipping replay (WAL resumes at {})",
            latest + 1
        );
    } else {
        println!(
            "matrixobject durability active for shard {shard_id} at {store_dir}; no shared WAL yet (fresh cluster)"
        );
    }

    // Publish this node's authoritative state as a checkpoint (index + slabs) at start, so a
    // future fresh recovery replays only the tail instead of all history. Opt-out via env.
    if local_state_present && env_bool("TS_MATRIXOBJECT_CHECKPOINT_ON_START", true) {
        match rt.block_on(replicator.publish_checkpoint(
            shard_id,
            latest,
            engine,
            &engine.block_store(),
        )) {
            Ok(manifest) => println!(
                "published matrixobject start checkpoint {} for shard {shard_id} at WAL index {}",
                manifest.checkpoint_id, manifest.checkpoint_wal_index
            ),
            Err(err) => eprintln!(
                "matrixobject start checkpoint publish failed for shard {shard_id}: {err}"
            ),
        }
    }

    let mode = SharedStoreStorageMode::from_sync_flag(sync_flush);
    let async_mode = mode.is_async();

    // Continue publishing at latest + 1 so we never overwrite persisted WAL
    // entries.
    let writer = Arc::new(
        replicator
            .storage_writer(mode, latest + 1)
            .with_group_commit(group_commit, commit_delay),
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let flush_signal = Arc::new(tokio::sync::Notify::new());

    let flush_interval_ms = env_u64("TS_MATRIXOBJECT_FLUSH_INTERVAL_MS", 100).max(1);
    let flush_batch = env_usize("TS_MATRIXOBJECT_FLUSH_BATCH", 256).max(1);

    if async_mode {
        // Background flusher: drains the queue in batches every interval, or as
        // soon as the queue reaches `flush_batch` (threshold flush). One drain
        // pass coalesces up to the whole backlog into batched appends.
        let bg_writer = Arc::clone(&writer);
        let bg_shutdown = Arc::clone(&shutdown);
        let bg_signal = Arc::clone(&flush_signal);
        rt.spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_millis(flush_interval_ms));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = bg_signal.notified() => {}
                }
                if bg_shutdown.load(Ordering::SeqCst) {
                    break;
                }
                loop {
                    match bg_writer.flush_pending(flush_batch).await {
                        Ok(report) => {
                            if report.remaining == 0 {
                                break;
                            }
                        }
                        Err(err) => {
                            eprintln!("matrixobject async flush failed for shard {shard_id}: {err}");
                            break;
                        }
                    }
                }
            }
        });
        println!(
            "matrixobject durability: async batched flush (interval {flush_interval_ms}ms, batch {flush_batch}); durable local WAL covers process crash"
        );
    } else if group_commit {
        println!(
            "matrixobject durability: sync flush with timer-less group commit (concurrent writers coalesce onto a shared durable barrier; commit_delay {}us)",
            commit_delay.as_micros()
        );
    } else {
        println!("matrixobject durability: sync flush (every write durable before ack)");
    }

    runtime.set_shared_wal_sink(Arc::new(MatrixObjectWalSink {
        handle: rt.handle().clone(),
        writer: Arc::clone(&writer),
        async_mode,
        flush_signal,
        batch_full: flush_batch,
    }));

    Some(MatrixObjectDurability {
        rt,
        writer,
        shutdown,
        async_mode,
        drain_batch: flush_batch.max(4096),
    })
}

/// Networked URI of the shared matrixobject object-store service, if configured.
/// `TS_SHARED_STORE_URI=matrixobject://host:port` opts a datanode into networked
/// cross-node lazy data-follow. Absent (or a non-matrixobject scheme) leaves the
/// datanode on its existing behavior, byte-identical.
fn matrixobject_networked_uri() -> Option<String> {
    std::env::var("TS_SHARED_STORE_URI")
        .ok()
        .map(|uri| uri.trim().to_string())
        .filter(|uri| uri.starts_with("matrixobject://"))
}

/// Durable shared-storage sink backed by a NETWORKED matrixobject WAL writer.
/// Mirrors every accepted write to the networked object store in sync mode (the
/// local WAL+page are already durable before this runs, so this rides after the
/// local commit). Compiles under default features: `MatrixObjectHttpStore` needs
/// no enterprise crate.
struct MatrixObjectNetworkedWalSink {
    handle: tokio::runtime::Handle,
    writer: Arc<temporalstore_rust::SharedStoreStorageWriter<MatrixObjectHttpStore>>,
}

impl std::fmt::Debug for MatrixObjectNetworkedWalSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MatrixObjectNetworkedWalSink")
            .finish_non_exhaustive()
    }
}

impl temporalstore_rust::SharedWalSink for MatrixObjectNetworkedWalSink {
    fn record_write(&self, shard_id: u64, command: &Command) {
        let writer = Arc::clone(&self.writer);
        let command = command.clone();
        let result = self
            .handle
            .block_on(async move { writer.write(shard_id, command).await });
        if let Err(err) = result {
            // Durability failure: log loudly. The local write already succeeded, so
            // the node stays available; the networked store catches up on the next
            // successful publish or on the next full replay.
            eprintln!("matrixobject networked durable write failed for shard {shard_id}: {err}");
        }
    }
}

/// Owns the networked durability runtime + writer for the process lifetime. The
/// sink mirrors writes in sync mode, so there is no queued backlog to drain on
/// drop; keeping the runtime alive is all that is required.
struct MatrixObjectNetworkedDurability {
    _rt: tokio::runtime::Runtime,
    _writer: Arc<temporalstore_rust::SharedStoreStorageWriter<MatrixObjectHttpStore>>,
}

/// Wire the NETWORKED matrixobject shared store into the running node so shard data
/// follows shards across nodes. On a fresh node (`!local_state_present`), restore
/// the served index + a lazy slab address map from the newest networked checkpoint
/// and replay only the WAL tail — old pages are then fetched on demand over the
/// network on first access, never eagerly downloaded. When this node already holds
/// authoritative local state, publish its current state as a networked checkpoint
/// (index + slabs) so future owners can lazily follow it. Finally, mirror every
/// accepted write to the networked store from here on. Active only when a
/// `matrixobject://` URI is configured; returns `None` on a bad URI or runtime
/// failure so the node still serves from local durability.
fn wire_matrixobject_networked_durability(
    uri: &str,
    storage_backend: &StorageBackend,
    engine: &TemporalEngine,
    runtime: &DataNodeRuntime,
    shard_id: u64,
    node_id: u64,
    local_state_present: bool,
    auto_load: bool,
) -> Option<MatrixObjectNetworkedDurability> {
    let store = match MatrixObjectHttpStore::new(uri) {
        Ok(store) => store,
        Err(err) => {
            error!(%err, uri, "invalid TS_SHARED_STORE_URI; networked matrixobject durability disabled");
            return None;
        }
    };
    let cluster_id = match storage_backend {
        StorageBackend::MatrixObject { cluster_id, .. } => cluster_id.clone(),
        _ => std::env::var("TS_CLUSTER_ID").unwrap_or_else(|_| "default".to_string()),
    };
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            error!(%err, "failed to start networked matrixobject durability runtime");
            return None;
        }
    };
    let replicator = Arc::new(SharedStoreReplicator::new(cluster_id, Arc::new(store)));

    // Fresh node: rebuild from the networked store via conformance lazy
    // data-follow (index + address map, then WAL tail; old pages fetched on demand).
    if !local_state_present {
        let after_wal_index = match rt.block_on(replicator.restore_index_and_page_addresses(
            shard_id,
            engine,
            &engine.block_store(),
        )) {
            Ok(manifest) => {
                info!(
                    shard_id,
                    checkpoint_id = %manifest.checkpoint_id,
                    checkpoint_wal_index = manifest.checkpoint_wal_index,
                    page_slabs = manifest.page_slabs.len(),
                    "restored shard index and lazy page addresses from networked matrixobject checkpoint"
                );
                manifest.checkpoint_wal_index
            }
            Err(SharedStoreReplicationError::CheckpointNotFound(_)) => 0,
            Err(err) => {
                warn!(shard_id, %err, "networked matrixobject checkpoint restore failed; replaying full shared WAL");
                0
            }
        };
        // Read the just-restored on-disk index into memory so this node auto-serves the
        // followed data. The process-level startup load was deferred to here precisely
        // so it observes the index installed by `restore_index_and_page_addresses`
        // above; the shared WAL-tail replay below then needs a loaded shard (it applies
        // through `engine.execute`). Join-empty placement instead loads via a later
        // `/load`, so `auto_load` is false and the load is skipped here (behavior
        // unchanged for that mode).
        if auto_load {
            let load = startup_load_shard_request(shard_id, node_id);
            let load_response = engine.load_shard_with(load);
            if !load_response.status.ok {
                warn!(
                    shard_id,
                    message = %load_response.status.message,
                    "load of restored networked shard index failed"
                );
            }
        }
        match rt.block_on(replicator.replay_wal(shard_id, after_wal_index, engine)) {
            Ok(report) => {
                if report.applied > 0 {
                    info!(
                        shard_id,
                        records = report.applied,
                        wal_index = report.last_wal_index,
                        after_wal_index,
                        "replayed shard WAL tail from networked matrixobject storage"
                    );
                }
            }
            Err(err) => {
                warn!(shard_id, %err, "no networked matrixobject data replayed for shard")
            }
        }
    }

    // Resume publishing at latest + 1 so we never clobber persisted WAL entries.
    let latest = match rt.block_on(replicator.latest_persisted_wal_index(shard_id)) {
        Ok(latest) => latest,
        Err(err) => {
            error!(shard_id, %err, "failed to read networked matrixobject WAL state; durability disabled");
            return None;
        }
    };

    // Publish this node's authoritative state as a networked checkpoint (index +
    // slabs) so a future owner can lazily follow it. Opt-out via env.
    if local_state_present && env_bool("TS_MATRIXOBJECT_NETWORKED_CHECKPOINT_ON_START", true) {
        match rt.block_on(replicator.publish_checkpoint(
            shard_id,
            latest,
            engine,
            &engine.block_store(),
        )) {
            Ok(manifest) => info!(
                shard_id,
                checkpoint_id = %manifest.checkpoint_id,
                page_slabs = manifest.page_slabs.len(),
                checkpoint_wal_index = manifest.checkpoint_wal_index,
                "published networked matrixobject checkpoint (index + slabs)"
            ),
            Err(err) => {
                warn!(shard_id, %err, "failed to publish networked matrixobject checkpoint at startup")
            }
        }
    }

    let writer = Arc::new(
        replicator.storage_writer(temporalstore_rust::SharedStoreStorageMode::Sync, latest + 1),
    );
    runtime.set_shared_wal_sink(Arc::new(MatrixObjectNetworkedWalSink {
        handle: rt.handle().clone(),
        writer: Arc::clone(&writer),
    }));
    info!(
        shard_id,
        uri, "networked matrixobject durability active (sync write mirror + lazy cross-node data-follow)"
    );

    Some(MatrixObjectNetworkedDurability {
        _rt: rt,
        _writer: writer,
    })
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

/// Build the datanode's `RaftConfig`, overlaying the write-path tuning knobs from env. Unset ->
/// `RaftConfig::default()` verbatim (replication_deadline_ms 5000, max_inflights_replicate 128),
/// so behavior is byte-identical unless an operator opts in.
fn raft_config_from_env() -> RaftConfig {
    let defaults = RaftConfig::default();
    RaftConfig {
        // 8 MB of applied log per shard before the periodic check compacts it into a
        // state-image snapshot. Left unbounded, every segment rotation rewrites an
        // ever-growing base record, and restart replays all of it.
        max_applied_log_bytes: env_u64("TS_RAFT_MAX_APPLIED_LOG_BYTES", 8 * 1024 * 1024),
        replication_deadline_ms: env_u64(
            "TS_RAFT_REPLICATION_DEADLINE_MS",
            defaults.replication_deadline_ms,
        ),
        max_inflights_replicate: env_u64(
            "TS_RAFT_MAX_INFLIGHTS_REPLICATE",
            defaults.max_inflights_replicate,
        ),
        ..defaults
    }
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
    // Make the write-path tuning knobs reachable from the datanode entrypoint. Byte-identical
    // when unset: replication_deadline_ms stays 5000 (the legacy hardcoded deadline) and
    // max_inflights_replicate stays 128. (This entrypoint is env-only; there is no config-file
    // loader in scope here, so the `[raft]` file equivalents are not read at this layer.)
    let raft_config = raft_config_from_env();
    let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
        // The cadence the metaserver already uses; 0 disables the check.
        snapshot_check_interval_ms: env_u64("TS_RAFT_SNAPSHOT_CHECK_INTERVAL_MS", 30_000),
        engine: ProductionRaftEngineKind::TemporalRaft,
        shard_id: raft_shard_id,
        local_node_id,
        nodes,
        wal_dir,
        config: raft_config,
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
        let topology = post_json_with_options_and_headers::<_, TableTopologyResponse>(
            meta_addr,
            "/tables/topology",
            &GetTableTopologyRequest {
                client_location: String::new(),
                namespace: request.namespace.clone(),
                table_name: table_name.clone(),
                old_topology_version: 0,
            },
            &temporalstore_rust::meta::admin_auth_header(),
            HttpRequestOptions::default(),
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
    boot_time_ms: u64,
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
        boot_time_ms,
        binary_version: binary_version.to_string(),
        shard_loads,
        shard_stat_loads,
        runtime_load: runtime.server_runtime_load(),
        shard_states: runtime.shard_serving_states(),
    };
    let response =
        post_json_with_options_and_headers::<_, ServerHeartbeatResponse>(
            meta_addr,
            "/servers/heartbeat",
            &request,
            &temporalstore_rust::meta::admin_auth_header(),
            HttpRequestOptions::default(),
        )
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
    /// The reason is free text carrying paths and endpoint URLs, so it reaches this label with
    /// quotes and backslashes in it. An unescaped one produces a line Prometheus rejects, which
    /// takes down the WHOLE scrape rather than this one series -- strictly worse than publishing
    /// nothing at all, which is why this is asserted rather than assumed.
    #[test]
    fn storage_backend_reason_is_escaped_for_a_prometheus_label() {
        let mut out = String::new();
        // Raw strings on both sides: exactly one level of escaping to reason about. The input is
        // what the engine would hand over -- a reason containing a quote AND a backslash.
        let reason = r#"auto: probe of "http://s\x" failed, degraded"#;
        append_storage_backend_metric(
            &mut out,
            &temporalstore_rust::StorageBackend::RaftReplication,
            reason,
        );
        let info = out
            .lines()
            .find(|line| line.starts_with("temporalstore_storage_backend_info{"))
            .expect("no info series emitted");
        assert!(info.contains(r#"backend="raft""#), "{info}");
        // Both arrive escaped, so the line stays parseable: " becomes \" and \ becomes \\.
        assert!(info.contains(r#"\"http://s\\x\""#), "{info}");
        // Exactly one closing brace before the value: proof the value did not terminate early.
        assert_eq!(1, info.matches("} 1").count(), "{info}");
    }

    /// The outcome series must keep its shape: the dashboards and the gateway probe both read it,
    /// and adding a second series next to it is exactly how the first one gets broken.
    #[test]
    fn the_outcome_series_is_unchanged_by_the_reason_series() {
        let mut out = String::new();
        append_storage_backend_metric(
            &mut out,
            &temporalstore_rust::StorageBackend::RaftReplication,
            "auto: nothing shared is reachable",
        );
        assert!(out.contains("temporalstore_storage_backend{backend=\"raft\",replication=\"raft\"} 1"));
    }

    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    use tempfile::tempdir;
    use temporalstore_rust::http::{get_json_with_options, serve, HttpRequestOptions};
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
    fn the_datanode_heartbeat_reports_when_this_process_started() {
        // The metaserver spots an in-place restart by watching this value change: it
        // anchors on the first one a server reports, then treats a different, non-zero one
        // as a restart. A constant can therefore never be a restart -- and this heartbeat
        // sent a literal 0, so the anchor was 0, it matched every time, and no datanode
        // restart was ever detected. Nothing failed; the detector simply never fired, and
        // everything downstream of its verdict (reboot conviction in the failure detector,
        // the reboot grace in the shard check) never ran either, while their own tests
        // passed because they set the flag on a fixture by hand instead of driving a
        // heartbeat. No test called this function at all.
        let engine = TemporalEngine::default();
        let runtime = DataNodeRuntime::new(
            engine.clone(),
            DataNodeRuntimeOptions {
                worker_threads: 1,
                max_queue_depth: 8,
                max_background_queue_depth: 8,
            },
        );

        let seen: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
        let meta_addr = free_local_addr();
        let bind_addr = meta_addr.clone();
        let seen_for_server = Arc::clone(&seen);
        thread::spawn(move || {
            serve(&bind_addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    // wait_for_http below probes /health, so the stub must answer it or
                    // the test times out on its own scaffolding rather than on anything
                    // it set out to check.
                    ("GET", "/health") => json_response(200, &Status::ok()),
                    ("POST", "/servers/heartbeat") => {
                        let beat = parse_json::<ServerHeartbeatRequest>(&request.body).unwrap();
                        seen_for_server.lock().unwrap().push(beat.boot_time_ms);
                        json_response(
                            200,
                            &ServerHeartbeatResponse {
                                status: Status::ok(),
                                forbid_auto_register: false,
                                topology_version: 0,
                                server_state: String::new(),
                            },
                        )
                    }
                    _ => json_response(404, &Status::error("not_found", "not found")),
                }
            })
            .unwrap();
        });
        wait_for_http(&meta_addr);

        let boot_time_ms = 1_723_456_789_000u64;
        let _ = send_heartbeat(
            &engine,
            &runtime,
            &meta_addr,
            "127.0.0.1:19",
            "test",
            boot_time_ms,
        );

        let reported = seen.lock().unwrap().clone();
        assert_eq!(
            reported,
            vec![boot_time_ms],
            "the heartbeat must carry the process start time it was given"
        );
        assert_ne!(
            reported[0], 0,
            "a zero start time reads as 'this build does not report it', which is what \
             disabled restart detection for every datanode"
        );
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
            // The cadence the metaserver already uses; 0 disables the check.
            snapshot_check_interval_ms: env_u64("TS_RAFT_SNAPSHOT_CHECK_INTERVAL_MS", 30_000),
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

    /// The datanode entrypoint must make the write-path tuning knobs reachable: unset -> the
    /// byte-identical defaults (5000 ms deadline / 128 inflight); set -> honored on the
    /// `RaftConfig` that starts the runtime.
    #[test]
    fn raft_config_from_env_honors_deadline_and_inflight_overrides() {
        // Unset: byte-identical defaults.
        std::env::remove_var("TS_RAFT_REPLICATION_DEADLINE_MS");
        std::env::remove_var("TS_RAFT_MAX_INFLIGHTS_REPLICATE");
        let defaults = raft_config_from_env();
        assert_eq!(defaults.replication_deadline_ms, 5000);
        assert_eq!(defaults.max_inflights_replicate, 128);

        // Set: both overrides flow through to the config the runtime is started with.
        std::env::set_var("TS_RAFT_REPLICATION_DEADLINE_MS", "300");
        std::env::set_var("TS_RAFT_MAX_INFLIGHTS_REPLICATE", "1024");
        let tuned = raft_config_from_env();
        assert_eq!(tuned.replication_deadline_ms, 300);
        assert_eq!(tuned.max_inflights_replicate, 1024);
        // Overriding these two knobs must not disturb the rest of the config.
        assert_eq!(tuned.max_segment_bytes, defaults.max_segment_bytes);
        assert_eq!(tuned.min_keep_segment_num, defaults.min_keep_segment_num);

        std::env::remove_var("TS_RAFT_REPLICATION_DEADLINE_MS");
        std::env::remove_var("TS_RAFT_MAX_INFLIGHTS_REPLICATE");
    }
}
