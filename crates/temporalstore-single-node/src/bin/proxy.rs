use temporalstore_single_node::http::{get_json, json_response, parse_json, post_json, serve};
use temporalstore_single_node::meta::GetShardResponse;
use temporalstore_single_node::types::{
    BatchExecuteRequest, BatchExecuteResponse, CommandResponse, ExecuteRequest, ExecuteResponse,
    Status,
};

fn main() {
    let addr = std::env::var("TS_PROXY_BIND_ADDR")
        .or_else(|_| std::env::var("TS_PROXY_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17000".to_string());
    let meta_addr = std::env::var("TS_META_ADDR").unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    println!("temporalstore proxy listening on {addr}");
    serve(&addr, move |request| {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("GET", path) if path.starts_with("/shards/") => {
                match get_json::<GetShardResponse>(&meta_addr, path) {
                    Ok(response) => json_response(200, &response),
                    Err(err) => {
                        json_response(502, &Status::error("metaserver_error", err.to_string()))
                    }
                }
            }
            ("POST", "/execute") => match parse_json::<ExecuteRequest>(&request.body) {
                Ok(req) => match route(&meta_addr, req.shard_id) {
                    Ok(server_addr) => {
                        match post_json::<_, ExecuteResponse>(&server_addr, "/execute", &req) {
                            Ok(response) => json_response(200, &response),
                            Err(err) => {
                                json_response(502, &execute_error("server_error", err.to_string()))
                            }
                        }
                    }
                    Err(status) => json_response(404, &execute_error(status.code, status.message)),
                },
                Err(err) => json_response(400, &execute_error("bad_request", err.to_string())),
            },
            ("POST", "/batch_execute") => match parse_json::<BatchExecuteRequest>(&request.body) {
                Ok(req) => match route(&meta_addr, req.shard_id) {
                    Ok(server_addr) => match post_json::<_, BatchExecuteResponse>(
                        &server_addr,
                        "/batch_execute",
                        &req,
                    ) {
                        Ok(response) => json_response(200, &response),
                        Err(err) => {
                            json_response(502, &Status::error("server_error", err.to_string()))
                        }
                    },
                    Err(status) => json_response(404, &status),
                },
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            },
            _ => json_response(404, &Status::error("not_found", "unknown proxy route")),
        }
    })
    .expect("proxy failed");
}

fn route(meta_addr: &str, shard_id: u64) -> Result<String, Status> {
    let response = get_json::<GetShardResponse>(meta_addr, &format!("/shards/{shard_id}"))
        .map_err(|err| Status::error("metaserver_error", err.to_string()))?;
    response
        .location
        .map(|location| location.server_addr)
        .ok_or(response.status)
}

fn execute_error(code: impl Into<String>, message: impl Into<String>) -> ExecuteResponse {
    ExecuteResponse {
        status: Status::error(code, message),
        response: CommandResponse::Empty,
    }
}
