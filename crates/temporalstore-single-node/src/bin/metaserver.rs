use temporalstore_single_node::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_single_node::meta::{
    AddNamespaceRequest, AddTableRequest, FreezeStaleServersRequest, GetShardResponse,
    GetTableTopologyRequest, LoadFinishRequest, ProxyHeartbeatRequest, RegisterProxyRequest,
    RegisterServerRequest, RegisterShardRequest, ServerHeartbeatRequest, SingleNodeMeta,
    StateChangeRequest,
};
use temporalstore_single_node::types::Status;

fn main() {
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let meta = SingleNodeMeta::default();
    let stale_after_ms = env_u64("TS_META_STALE_AFTER_MS", 30_000);
    let detector_interval_ms = env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000);
    let _failure_detector = meta.start_failure_detector_loop(stale_after_ms, detector_interval_ms);
    println!("temporalstore metaserver listening on {addr}");
    serve(&addr, move |request| handle(&meta, request)).expect("metaserver failed");
}

fn handle(meta: &SingleNodeMeta, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/meta/info") => json_response(200, &meta.info()),
        ("GET", "/meta/stats") => json_response(200, &meta.stats()),
        ("POST", "/register_shard") => parse_or(&request.body, |req: RegisterShardRequest| {
            meta.register(req)
        }),
        ("GET", path) if path.starts_with("/shards/") => {
            let shard_id = path
                .trim_start_matches("/shards/")
                .parse()
                .unwrap_or_default();
            json_response(200, &meta.get(shard_id))
        }
        ("POST", "/servers/register") => parse_or(&request.body, |req: RegisterServerRequest| {
            meta.register_server(req)
        }),
        ("POST", "/servers/heartbeat") | ("POST", "/server_heartbeat") => {
            parse_or(&request.body, |req: ServerHeartbeatRequest| {
                meta.server_heartbeat(req)
            })
        }
        ("GET", "/servers") => json_response(200, &meta.list_servers()),
        ("POST", "/servers/freeze_stale") => {
            parse_or(&request.body, |req: FreezeStaleServersRequest| {
                meta.freeze_stale_servers(req.stale_after_ms)
            })
        }
        ("POST", "/partitions/finish_load") | ("POST", "/finish_load") => {
            parse_or(&request.body, |req: LoadFinishRequest| {
                meta.finish_load(req)
            })
        }
        ("POST", "/servers/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            meta.freeze_server(req)
        }),
        ("POST", "/servers/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            meta.drop_server(req)
        }),
        ("POST", "/proxies/register") => parse_or(&request.body, |req: RegisterProxyRequest| {
            meta.register_proxy(req)
        }),
        ("POST", "/proxies/heartbeat") | ("POST", "/proxy_heartbeat") => {
            parse_or(&request.body, |req: ProxyHeartbeatRequest| {
                meta.proxy_heartbeat(req)
            })
        }
        ("GET", "/proxies") => json_response(200, &meta.list_proxies()),
        ("POST", "/proxies/freeze") => parse_or(&request.body, |req: StateChangeRequest| {
            meta.freeze_proxy(req)
        }),
        ("POST", "/proxies/drop") => parse_or(&request.body, |req: StateChangeRequest| {
            meta.drop_proxy(req)
        }),
        ("POST", "/namespaces") => parse_or(&request.body, |req: AddNamespaceRequest| {
            meta.add_namespace(req)
        }),
        ("GET", "/namespaces") => json_response(200, &meta.list_namespaces()),
        ("POST", "/tables") => parse_or(&request.body, |req: AddTableRequest| meta.add_table(req)),
        ("GET", "/tables") => json_response(200, &meta.list_tables()),
        ("POST", "/tables/topology") | ("POST", "/table_topology") => {
            parse_or(&request.body, |req: GetTableTopologyRequest| {
                meta.get_table_topology(req)
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

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
