use temporalstore_single_node::http::{json_response, parse_json, serve};
use temporalstore_single_node::meta::{GetShardResponse, RegisterShardRequest, SingleNodeMeta};
use temporalstore_single_node::types::Status;

fn main() {
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let meta = SingleNodeMeta::default();
    println!("temporalstore metaserver listening on {addr}");
    serve(&addr, move |request| {
        match (request.method.as_str(), request.path.as_str()) {
            ("GET", "/health") => json_response(200, &Status::ok()),
            ("POST", "/register_shard") => {
                match parse_json::<RegisterShardRequest>(&request.body) {
                    Ok(req) => json_response(200, &meta.register(req)),
                    Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
                }
            }
            ("GET", path) if path.starts_with("/shards/") => {
                let shard_id = path
                    .trim_start_matches("/shards/")
                    .parse()
                    .unwrap_or_default();
                json_response(200, &meta.get(shard_id))
            }
            _ => json_response(
                404,
                &GetShardResponse {
                    status: Status::error("not_found", "unknown metaserver route"),
                    location: None,
                },
            ),
        }
    })
    .expect("metaserver failed");
}
