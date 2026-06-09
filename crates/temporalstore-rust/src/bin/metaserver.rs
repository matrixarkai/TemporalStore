use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::{
    AddNamespaceRequest, AddTableRequest, FreezeStaleServersRequest, GetShardResponse,
    GetTableTopologyRequest, LoadFinishRequest, ProxyHeartbeatRequest, PublishShardSnapshotRequest,
    RegisterProxyRequest, RegisterServerRequest, RegisterShardRequest, ServerHeartbeatRequest,
    SingleNodeMeta, StateChangeRequest,
};
use temporalstore_rust::raft::{
    ProductionMetaRaftRuntime, ProductionMetaRaftRuntimeOptions, ProductionRaftEngineKind,
    ProductionRaftNode, RaftClusterStatus, RaftConfig, RaftNodeId,
};
use temporalstore_rust::types::Status;

fn main() {
    let addr = std::env::var("TS_META_BIND_ADDR")
        .or_else(|_| std::env::var("TS_META_ADDR"))
        .unwrap_or_else(|_| "127.0.0.1:17001".to_string());
    let backend = MetaBackend::from_env().expect("failed to initialize metaserver backend");
    let stale_after_ms = env_u64("TS_META_STALE_AFTER_MS", 30_000);
    let detector_interval_ms = env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000);
    let _failure_detector = match &backend {
        MetaBackend::Single(meta) => Some(MetaBackground::Single(
            meta.start_failure_detector_loop(stale_after_ms, detector_interval_ms),
        )),
        MetaBackend::Raft(runtime) => Some(MetaBackground::Raft(runtime.start_timer_loop())),
    };
    println!("temporalstore metaserver listening on {addr}");
    serve(&addr, move |request| handle(&backend, request)).expect("metaserver failed");
}

#[derive(Clone)]
enum MetaBackend {
    Single(SingleNodeMeta),
    Raft(ProductionMetaRaftRuntime),
}

enum MetaBackground {
    Single(std::thread::JoinHandle<()>),
    Raft(temporalstore_rust::raft::ProductionRaftTimerHandle),
}

impl MetaBackend {
    fn from_env() -> std::io::Result<Self> {
        if env_bool("TS_META_RAFT", false) || std::env::var("TS_META_RAFT_NODES").is_ok() {
            return Ok(Self::Raft(
                ProductionMetaRaftRuntime::start(runtime_options_from_env())
                    .expect("failed to initialize metaserver raft runtime"),
            ));
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
            Self::Raft(runtime) => Some(runtime.status()),
        }
    }

    fn raft_ready(&self) -> Status {
        match self {
            Self::Single(_) => Status::error("raft_disabled", "meta raft is disabled"),
            Self::Raft(runtime) => runtime
                .validate_ready()
                .map(|_| Status::ok())
                .unwrap_or_else(|err| Status::error("raft_not_ready", err.to_string())),
        }
    }
}

macro_rules! backend_call {
    ($backend:expr, $method:ident $(, $arg:expr)*) => {
        match $backend {
            MetaBackend::Single(meta) => meta.$method($($arg),*),
            MetaBackend::Raft(runtime) => runtime.cluster().$method($($arg),*),
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
        ("GET", "/meta/raft/ready") => json_response(200, &meta.raft_ready()),
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

fn runtime_options_from_env() -> ProductionMetaRaftRuntimeOptions {
    ProductionMetaRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        local_node_id: env_u64("TS_META_RAFT_NODE_ID", 1),
        nodes: parse_meta_raft_nodes(),
        config: RaftConfig::default(),
        heartbeat_interval_ms: env_u64("TS_META_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_META_RAFT_ELECTION_TICK_MS", 50),
        failure_detector_interval_ms: env_u64("TS_META_FAILURE_DETECTOR_INTERVAL_MS", 10_000),
        stale_server_after_ms: env_u64("TS_META_STALE_AFTER_MS", 30_000),
    }
}

fn parse_meta_raft_nodes() -> Vec<ProductionRaftNode> {
    std::env::var("TS_META_RAFT_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .enumerate()
                .filter_map(|(index, part)| parse_meta_raft_node(index, part.trim()))
                .collect::<Vec<_>>()
        })
        .filter(|nodes| !nodes.is_empty())
        .unwrap_or_else(|| {
            vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:17101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:17102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:17103".to_string(),
                },
            ]
        })
}

fn parse_meta_raft_node(index: usize, value: &str) -> Option<ProductionRaftNode> {
    if let Some((id, addr)) = value.split_once('=') {
        return Some(ProductionRaftNode {
            node_id: id.trim().parse().ok()?,
            addr: addr.trim().to_string(),
        });
    }
    let node_id = value.parse::<RaftNodeId>().ok()?;
    Some(ProductionRaftNode {
        node_id,
        addr: format!("127.0.0.1:{}", 17101 + index),
    })
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
