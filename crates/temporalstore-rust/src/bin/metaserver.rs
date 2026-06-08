use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::{
    AddNamespaceRequest, AddTableRequest, FreezeStaleServersRequest, GetShardResponse,
    GetTableTopologyRequest, LoadFinishRequest, ProxyHeartbeatRequest, PublishShardSnapshotRequest,
    RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest, ServerHeartbeatRequest,
    SingleNodeMeta, StateChangeRequest,
};
use temporalstore_rust::raft::{MetaRaftCluster, RaftClusterStatus, RaftNodeId};
use temporalstore_rust::types::Status;

fn main() {
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let backend = MetaBackend::from_env().expect("failed to initialize metaserver backend");
    let stale_after_ms = env_u64("TS_META_STALE_AFTER_MS", 30_000);
    let detector_interval_ms = env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000);
    let _failure_detector = match &backend {
        MetaBackend::Single(meta) => {
            Some(meta.start_failure_detector_loop(stale_after_ms, detector_interval_ms))
        }
        MetaBackend::Raft(_) => None,
    };
    println!("temporalstore metaserver listening on {addr}");
    serve(&addr, move |request| handle(&backend, request)).expect("metaserver failed");
}

#[derive(Clone)]
enum MetaBackend {
    Single(SingleNodeMeta),
    Raft(MetaRaftCluster),
}

impl MetaBackend {
    fn from_env() -> std::io::Result<Self> {
        if env_bool("TS_META_RAFT", false) || std::env::var("TS_META_RAFT_NODES").is_ok() {
            return Ok(Self::Raft(MetaRaftCluster::new(parse_raft_node_ids())));
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
            Self::Raft(meta) => Some(meta.status()),
        }
    }
}

macro_rules! backend_call {
    ($backend:expr, $method:ident $(, $arg:expr)*) => {
        match $backend {
            MetaBackend::Single(meta) => meta.$method($($arg),*),
            MetaBackend::Raft(meta) => meta.$method($($arg),*),
        }
    };
}

fn handle(meta: &MetaBackend, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/meta/info") => json_response(200, &backend_call!(meta, info)),
        ("GET", "/meta/stats") => json_response(200, &backend_call!(meta, stats)),
        ("GET", "/meta/raft/status") => match meta.raft_status() {
            Some(status) => json_response(200, &status),
            None => json_response(
                200,
                &Status::error("raft_disabled", "meta raft is disabled"),
            ),
        },
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
                backend_call!(meta, freeze_stale_servers, req.stale_after_ms)
            })
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

fn parse_raft_node_ids() -> Vec<RaftNodeId> {
    std::env::var("TS_META_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| vec![1, 2, 3])
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
