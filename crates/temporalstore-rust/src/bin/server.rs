use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::http::{json_response, parse_json, post_json, serve};
use temporalstore_rust::meta::{
    AckResponse, RegisterServerRequest, RegisterShardRequest, RegisterShardResponse,
    ServerHeartbeatRequest, ServerHeartbeatResponse, ShardLoad,
};
use temporalstore_rust::types::{BatchExecuteRequest, ExecuteRequest, ExecuteResponse, Status};
use temporalstore_rust::{
    CheckedBatchExecuteRequest, CheckedExecuteRequest, CompactionRequest, DataNodeRuntime,
    DataNodeRuntimeOptions, DumpShardRequest, GcRequest, LoadShardRequest, MembershipUpdateRequest,
    RequestController, ScanStreamRequest, SetConfigRequest, StreamReadRequest, UnloadShardRequest,
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
    let cache_memory_bytes = std::env::var("TS_CACHE_MEMORY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16 * 1024 * 1024);
    let engine =
        TemporalEngine::with_local_dirs(cache_memory_bytes, cache_dir, page_store_dir, index_dir);
    engine.load_shard(shard_id);
    let runtime = DataNodeRuntime::new(
        engine.clone(),
        DataNodeRuntimeOptions {
            worker_threads: env_usize("TS_SERVER_WORKER_THREADS", 4),
            max_queue_depth: env_usize("TS_SERVER_MAX_QUEUE_DEPTH", 1024),
        },
    );

    let node_id = std::env::var("TS_SERVER_NODE_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_default();
    let location = std::env::var("TS_SERVER_LOCATION").unwrap_or_default();
    let binary_version = env!("CARGO_PKG_VERSION").to_string();
    let heartbeat_interval_ms = std::env::var("TS_SERVER_HEARTBEAT_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000);

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

    println!("temporalstore server listening on {addr}");
    serve(&addr, move |request| {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", "/metrics") => {
                let mut metrics = engine.prometheus_metrics();
                append_runtime_metrics(&mut metrics, &runtime);
                (200, metrics.into_bytes())
            }
            ("GET", "/server/info") => json_response(200, &engine.loaded_shard_stats()),
            ("GET", "/server/runtime_stats") => json_response(200, &runtime.stats()),
            ("GET", "/server/dirty_objects") => json_response(200, &runtime.dirty_objects()),
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
                Ok(req) => json_response(200, &engine.execute(req)),
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
                    Ok(req) => json_response(200, &engine.update_membership(req)),
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

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn append_runtime_metrics(out: &mut String, runtime: &DataNodeRuntime) {
    let stats = runtime.stats();
    out.push_str("# HELP temporalstore_data_node_runtime_jobs_total Data node runtime job counters by kind.\n");
    out.push_str("# TYPE temporalstore_data_node_runtime_jobs_total counter\n");
    for (kind, value) in [
        ("submitted", stats.submitted_total),
        ("completed", stats.completed_total),
        ("rejected", stats.rejected_total),
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

fn send_heartbeat(
    engine: &TemporalEngine,
    meta_addr: &str,
    server_addr: &str,
    binary_version: &str,
) -> ServerHeartbeatResponse {
    let shard_loads = engine
        .loaded_shard_stats()
        .into_iter()
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
    let request = ServerHeartbeatRequest {
        server_addr: server_addr.to_string(),
        boot_time_ms: 0,
        binary_version: binary_version.to_string(),
        shard_loads,
    };
    post_json::<_, ServerHeartbeatResponse>(meta_addr, "/servers/heartbeat", &request)
        .unwrap_or_else(|err| ServerHeartbeatResponse {
            status: Status::error("heartbeat_failed", err.to_string()),
            forbid_auto_register: false,
        })
}
