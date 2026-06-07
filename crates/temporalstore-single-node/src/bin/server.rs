use temporalstore_single_node::engine::TemporalEngine;
use temporalstore_single_node::http::{json_response, parse_json, post_json, serve};
use temporalstore_single_node::meta::{RegisterShardRequest, RegisterShardResponse};
use temporalstore_single_node::types::{
    BatchExecuteRequest, ExecuteRequest, ExecuteResponse, Status,
};
use temporalstore_single_node::{
    LoadShardRequest, MembershipUpdateRequest, ScanStreamRequest, SetConfigRequest,
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
    let cache_memory_bytes = std::env::var("TS_CACHE_MEMORY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16 * 1024 * 1024);
    let engine =
        TemporalEngine::with_local_dirs(cache_memory_bytes, cache_dir, page_store_dir, index_dir);
    engine.load_shard(shard_id);

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

    println!("temporalstore server listening on {addr}");
    serve(&addr, move |request| {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
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
                        response: temporalstore_single_node::CommandResponse::Empty,
                    },
                ),
            },
            ("POST", "/batch_execute") => match parse_json::<BatchExecuteRequest>(&request.body) {
                Ok(req) => json_response(200, &engine.batch_execute(req)),
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
