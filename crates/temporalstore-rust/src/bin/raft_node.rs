use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::{
    handle_raft_http, CommandResponse, DistributedRaftCommandResponse,
    DistributedRaftProposeRequest, DistributedRaftReadRequest, ProductionRaftEngineKind,
    ProductionRaftNode, ProductionRaftRuntime, ProductionRaftRuntimeOptions,
    ProductionRaftSecurity, RaftConfig, RaftFailoverReport, RaftRpcRuntimeOptions, Status,
};

#[derive(Debug, Deserialize)]
struct RaftAdminLivenessRequest {
    node_id: u64,
    alive: bool,
}

#[derive(Debug, Deserialize)]
struct RaftAdminElectRequest {
    node_id: u64,
}

#[derive(Debug, Serialize)]
struct RaftAdminLivenessResponse {
    status: Status,
}

#[derive(Debug, Serialize)]
struct RaftAdminFailoverResponse {
    status: Status,
    report: Option<RaftFailoverReport>,
}

fn main() {
    let options = runtime_options_from_env();
    let bind_addr = std::env::var("TS_RAFT_BIND_ADDR")
        .ok()
        .or_else(|| {
            options
                .nodes
                .iter()
                .find(|node| node.node_id == options.local_node_id)
                .map(|node| node.addr.clone())
        })
        .unwrap_or_else(|| "127.0.0.1:19001".to_string());
    let runtime = ProductionRaftRuntime::start(options).expect("failed to start raft node runtime");
    let _timer = runtime.start_timer_loop();
    let local_admin_enabled = env_bool("TS_RAFT_ENABLE_LOCAL_ADMIN", false);
    println!(
        "temporalstore raft node {} listening on {bind_addr}",
        runtime.status().leader_id
    );
    serve(&bind_addr, move |request| {
        handle(&runtime, local_admin_enabled, request)
    })
    .expect("raft node failed");
}

fn handle(
    runtime: &ProductionRaftRuntime,
    local_admin_enabled: bool,
    request: HttpRequest,
) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/raft/status") => json_response(200, &runtime.status()),
        ("POST", "/raft/admin/liveness") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminLivenessRequest>(&request.body) {
                Ok(req) => {
                    let status = runtime
                        .cluster()
                        .set_alive(req.node_id, req.alive)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/elect") => match parse_json::<RaftAdminElectRequest>(&request.body) {
            Ok(req) => {
                if !local_admin_enabled {
                    return json_response(403, &Status::error("forbidden", "local admin disabled"));
                }
                let cluster = runtime.cluster();
                let status = cluster
                    .catch_up(req.node_id)
                    .and_then(|_| cluster.elect_leader(req.node_id))
                    .map(|_| Status::ok())
                    .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                json_response(200, &RaftAdminLivenessResponse { status })
            }
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        ("POST", "/raft/admin/failover") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            let response = match runtime.cluster().failover_primary() {
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
        ("POST", "/raft/propose") => {
            match parse_json::<DistributedRaftProposeRequest>(&request.body) {
                Ok(req) => json_response(200, &command_response(runtime.propose(req.command))),
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/read") => match parse_json::<DistributedRaftReadRequest>(&request.body) {
            Ok(req) => json_response(
                200,
                &command_response(runtime.read_local(req.node_id, req.command)),
            ),
            Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
        },
        _ => handle_raft_http(&runtime.cluster(), request),
    }
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

fn runtime_options_from_env() -> ProductionRaftRuntimeOptions {
    let local_node_id = env_u64("TS_RAFT_NODE_ID", 1);
    let shard_id = env_u64("TS_RAFT_SHARD_ID", 1);
    let nodes = parse_nodes();
    let wal_dir = std::env::var("TS_RAFT_WAL_DIR")
        .unwrap_or_else(|_| format!("target/temporalstore-raft/node-{local_node_id}"));
    let auth_token =
        std::env::var("TS_RAFT_AUTH_TOKEN").unwrap_or_else(|_| "local-raft-token".to_string());
    ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        shard_id,
        local_node_id,
        nodes,
        wal_dir,
        config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions {
            max_retries: env_usize("TS_RAFT_RPC_RETRIES", 2),
            deadline_ms: env_u64("TS_RAFT_RPC_DEADLINE_MS", 1_000),
            ..RaftRpcRuntimeOptions::default()
        },
        security: ProductionRaftSecurity::plaintext_for_local_chaos(auth_token),
        heartbeat_interval_ms: env_u64("TS_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_RAFT_ELECTION_TICK_MS", 50),
        max_catchup_entries_per_heartbeat: env_u64(
            "TS_RAFT_MAX_CATCHUP_ENTRIES_PER_HEARTBEAT",
            256,
        ),
        allow_plaintext_for_local_chaos: env_bool("TS_RAFT_ALLOW_PLAINTEXT", true),
    }
}

fn parse_nodes() -> Vec<ProductionRaftNode> {
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
            BTreeMap::from([
                (1, "127.0.0.1:19001"),
                (2, "127.0.0.1:19002"),
                (3, "127.0.0.1:19003"),
            ])
            .into_iter()
            .map(|(node_id, addr)| ProductionRaftNode {
                node_id,
                addr: addr.to_string(),
            })
            .collect()
        })
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
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
