use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use temporalstore_rust::http::{json_response, parse_json, serve, HttpRequest};
use temporalstore_rust::meta::ShardSnapshotRef;
use temporalstore_rust::raft::{
    RaftReplicaBootstrapPlan, RaftSnapshotPublishReport, RaftSnapshotTriggerReport,
    ReadIndexResponse,
};
use temporalstore_rust::{
    handle_authenticated_raft_http, production_raft_security_from_env, CommandResponse,
    DistributedRaftCommandResponse, DistributedRaftProposeRequest, DistributedRaftReadRequest,
    ProductionRaftEngineKind, ProductionRaftNode, ProductionRaftRuntime,
    ProductionRaftRuntimeOptions, RaftConfig, RaftControlLeadershipRequest, RaftFailoverReport,
    RaftMembershipChangeReport, RaftNodeId, RaftRpcRuntimeOptions, RaftTransport, Status,
};
use temporalstore_snapshot::{FileObjectStore, S3SnapshotStore};

#[derive(Debug, Deserialize)]
struct RaftAdminLivenessRequest {
    node_id: u64,
    alive: bool,
}

#[derive(Debug, Deserialize)]
struct RaftAdminElectRequest {
    node_id: u64,
}

#[derive(Debug, Deserialize)]
struct RaftAdminPeerBlockRequest {
    peer_id: u64,
    blocked: bool,
}

#[derive(Debug, Deserialize)]
struct RaftAdminCatchUpRequest {
    node_id: u64,
}

#[derive(Debug, Deserialize)]
struct RaftAdminWaitAppliedRequest {
    node_id: u64,
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
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotRequest {
    object_root: String,
    local_root: String,
    #[serde(default = "default_snapshot_cluster_id")]
    cluster_id: String,
    #[serde(default = "default_snapshot_bucket")]
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

#[derive(Debug, Deserialize, Serialize)]
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
    let blocked_peers = Arc::new(Mutex::new(BTreeSet::new()));
    println!(
        "temporalstore raft node {} listening on {bind_addr}",
        runtime.status().leader_id
    );
    serve(&bind_addr, move |request| {
        handle(&runtime, local_admin_enabled, &blocked_peers, request)
    })
    .expect("raft node failed");
}

fn handle(
    runtime: &ProductionRaftRuntime,
    local_admin_enabled: bool,
    blocked_peers: &Arc<Mutex<BTreeSet<u64>>>,
    request: HttpRequest,
) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/metrics") => (200, runtime.cluster().prometheus_metrics().into_bytes()),
        ("GET", "/raft/status") => json_response(200, &runtime.status()),
        ("GET", "/raft/control/byteraft_runtime_admin")
        | ("POST", "/raft/control/byteraft_runtime_admin") => {
            json_response(200, &runtime.cluster().byteraft_runtime_admin_report())
        }
        ("GET", "/raft/control/byteraft_local_status")
        | ("POST", "/raft/control/byteraft_local_status") => {
            json_response(200, &runtime.cluster().byteraft_local_status_report())
        }
        ("POST", "/raft/apply_health") => {
            match parse_json::<RaftApplyHealthRequest>(&request.body) {
                Ok(req) => {
                    json_response(200, &runtime.local_apply_health(req.max_allowed_apply_lag))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/membership/apply") => {
            match parse_json::<RaftMembershipApplyRequest>(&request.body) {
                Ok(req) => {
                    let response = match runtime.apply_membership_change_safely(req.voters) {
                        Ok(report) => RaftMembershipApplyResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftMembershipApplyResponse {
                            status: Status::error("raft_error", err.to_string()),
                            report: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("GET", "/raft/control/list_membership") | ("POST", "/raft/control/list_membership") => {
            json_response(200, &membership_response(runtime))
        }
        ("POST", "/raft/control/add_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let mut voters = runtime.cluster().membership().voters;
                    if !voters.contains(&req.node_id) {
                        voters.push(req.node_id);
                        voters.sort_unstable();
                    }
                    json_response(200, &apply_membership_response(runtime, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/remove_node") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let voters = runtime
                        .cluster()
                        .membership()
                        .voters
                        .into_iter()
                        .filter(|node_id| *node_id != req.node_id)
                        .collect::<Vec<_>>();
                    json_response(200, &apply_membership_response(runtime, voters))
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/trigger_snapshot") => {
            let response = match runtime.cluster().maybe_trigger_snapshot() {
                Ok(report) => RaftControlSnapshotResponse {
                    status: Status::ok(),
                    report: Some(report),
                },
                Err(err) => RaftControlSnapshotResponse {
                    status: Status::error("raft_error", err.to_string()),
                    report: None,
                },
            };
            json_response(200, &response)
        }
        ("POST", "/raft/control/read_index") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let response = match runtime.cluster().read_index(req.node_id) {
                        Ok(read_index) => RaftControlReadIndexResponse {
                            status: Status::ok(),
                            response: Some(read_index),
                        },
                        Err(err) => RaftControlReadIndexResponse {
                            status: Status::error("raft_error", err.to_string()),
                            response: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/transfer_leader") => {
            match parse_json::<RaftControlNodeRequest>(&request.body) {
                Ok(req) => {
                    let status = runtime
                        .transfer_leader(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/control/accept_leadership") => {
            match parse_json::<RaftControlLeadershipRequest>(&request.body) {
                Ok(req) => {
                    let status = if req.node_id != runtime.local_node_id() {
                        Status::error(
                            "bad_request",
                            format!(
                                "node {} cannot accept leadership for node {}",
                                runtime.local_node_id(),
                                req.node_id
                            ),
                        )
                    } else {
                        runtime
                            .cluster()
                            .catch_up(req.node_id)
                            .and_then(|_| runtime.cluster().transfer_leader(req.node_id))
                            .map(|_| Status::ok())
                            .unwrap_or_else(|err| Status::error("raft_error", err.to_string()))
                    };
                    json_response(200, &status)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
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
        ("POST", "/raft/admin/catch_up") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminCatchUpRequest>(&request.body) {
                Ok(req) => {
                    let cluster = runtime.cluster();
                    let status = cluster
                        .build_install_snapshot_request(req.node_id)
                        .and_then(|snapshot| {
                            let response = match runtime.transport().install_snapshot(snapshot) {
                                Ok(response) => response,
                                Err(err) => {
                                    let _ = cluster.finish_snapshot_send(req.node_id, false);
                                    return Err(err);
                                }
                            };
                            if response.success {
                                cluster.finish_snapshot_send(req.node_id, true)?;
                                cluster.catch_up(req.node_id)
                            } else {
                                cluster.finish_snapshot_send(req.node_id, false)?;
                                Err(temporalstore_rust::RaftError::Transport(format!(
                                    "snapshot install rejected by node {}: {:?}",
                                    req.node_id, response.reject_reason
                                )))
                            }
                        })
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/bootstrap_external_snapshot") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminBootstrapExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        uri_scheme(&req.snapshot.uri),
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    runtime.cluster().bootstrap_replica_from_external_snapshot(
                                        req.target_id,
                                        &snapshot_store,
                                        &req.snapshot,
                                        PathBuf::from(&req.local_root)
                                            .join(format!("restore-node-{}", req.target_id)),
                                    ),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(plan) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::ok(),
                            plan: Some(plan),
                        },
                        Err(err) => RaftAdminBootstrapExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            plan: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/publish_external_snapshot") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminPublishExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = Arc::new(FileObjectStore::with_uri_scheme(
                        PathBuf::from(&req.object_root),
                        "s3",
                    ));
                    let snapshot_store = S3SnapshotStore::new(
                        req.cluster_id,
                        req.bucket,
                        PathBuf::from(&req.local_root),
                        store,
                    );
                    let response = match tokio::runtime::Runtime::new()
                        .map_err(|err| err.to_string())
                        .and_then(|tokio_runtime| {
                            tokio_runtime
                                .block_on(
                                    runtime
                                        .cluster()
                                        .publish_leader_snapshot_to_store(&snapshot_store),
                                )
                                .map_err(|err| err.to_string())
                        }) {
                        Ok(report) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::ok(),
                            report: Some(report),
                        },
                        Err(err) => RaftAdminPublishExternalSnapshotResponse {
                            status: Status::error("raft_error", err),
                            report: None,
                        },
                    };
                    json_response(200, &response)
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/local_catch_up") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminCatchUpRequest>(&request.body) {
                Ok(req) => {
                    let status = runtime
                        .cluster()
                        .catch_up(req.node_id)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/wait_applied") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminWaitAppliedRequest>(&request.body) {
                Ok(req) => {
                    let status = runtime
                        .wait_for_applied_index(req.node_id, req.index, req.timeout_ms)
                        .map(|_| Status::ok())
                        .unwrap_or_else(|err| Status::error("raft_error", err.to_string()));
                    json_response(200, &RaftAdminLivenessResponse { status })
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
        }
        ("POST", "/raft/admin/block_peer") => {
            if !local_admin_enabled {
                return json_response(403, &Status::error("forbidden", "local admin disabled"));
            }
            match parse_json::<RaftAdminPeerBlockRequest>(&request.body) {
                Ok(req) => {
                    let mut blocked = blocked_peers.lock().expect("blocked peer lock poisoned");
                    if req.blocked {
                        blocked.insert(req.peer_id);
                    } else {
                        blocked.remove(&req.peer_id);
                    }
                    json_response(
                        200,
                        &RaftAdminLivenessResponse {
                            status: Status::ok(),
                        },
                    )
                }
                Err(err) => json_response(400, &Status::error("bad_request", err.to_string())),
            }
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
        _ => {
            if let Some(peer_id) = incoming_peer_id(&request) {
                if blocked_peers
                    .lock()
                    .expect("blocked peer lock poisoned")
                    .contains(&peer_id)
                {
                    return json_response(
                        503,
                        &Status::error("raft_peer_blocked", "local chaos peer block active"),
                    );
                }
            }
            handle_authenticated_raft_http(
                &runtime.cluster(),
                request,
                runtime.peer_auth_token().unwrap_or_default(),
            )
        }
    }
}

fn membership_response(runtime: &ProductionRaftRuntime) -> RaftControlMembershipResponse {
    let membership = runtime.cluster().membership();
    RaftControlMembershipResponse {
        status: Status::ok(),
        shard_id: membership.shard_id,
        leader_id: membership.leader_id,
        voters: membership.voters,
    }
}

fn apply_membership_response(
    runtime: &ProductionRaftRuntime,
    voters: Vec<RaftNodeId>,
) -> RaftMembershipApplyResponse {
    match runtime.apply_membership_change_safely(voters) {
        Ok(report) => RaftMembershipApplyResponse {
            status: Status::ok(),
            report: Some(report),
        },
        Err(err) => RaftMembershipApplyResponse {
            status: Status::error("raft_error", err.to_string()),
            report: None,
        },
    }
}

fn incoming_peer_id(request: &HttpRequest) -> Option<u64> {
    match request.path.as_str() {
        "/raft/append_entries" | "/raft/install_snapshot" | "/raft/install_snapshot_chunk" => {
            serde_json::from_slice::<serde_json::Value>(&request.body)
                .ok()?
                .get("leader_id")?
                .as_u64()
        }
        "/raft/request_vote" => serde_json::from_slice::<serde_json::Value>(&request.body)
            .ok()?
            .get("candidate_id")?
            .as_u64(),
        _ => None,
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
    // Reads TS_RAFT_AUTH_TOKEN plus TS_RAFT_SECURITY_MODE/cert paths for process auth.
    let security = production_raft_security_from_env(
        "local-raft-token",
        env_bool("TS_RAFT_ALLOW_PLAINTEXT", true),
    );
    ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::TemporalRaft,
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
        security: security.security,
        heartbeat_interval_ms: env_u64("TS_RAFT_HEARTBEAT_INTERVAL_MS", 100),
        election_tick_ms: env_u64("TS_RAFT_ELECTION_TICK_MS", 50),
        max_catchup_entries_per_heartbeat: env_u64(
            "TS_RAFT_MAX_CATCHUP_ENTRIES_PER_HEARTBEAT",
            256,
        ),
        allow_plaintext_for_local_chaos: security.allow_plaintext_for_local_chaos,
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

#[cfg(test)]
mod tests {
    use super::*;
    use temporalstore_rust::Command;

    fn test_runtime() -> ProductionRaftRuntime {
        test_runtime_for_node(1)
    }

    fn test_runtime_for_node(local_node_id: RaftNodeId) -> ProductionRaftRuntime {
        let dir = tempfile::tempdir().unwrap().into_path();
        ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
            engine: ProductionRaftEngineKind::TemporalRaft,
            shard_id: 55,
            local_node_id,
            nodes: vec![
                ProductionRaftNode {
                    node_id: 1,
                    addr: "127.0.0.1:19101".to_string(),
                },
                ProductionRaftNode {
                    node_id: 2,
                    addr: "127.0.0.1:19102".to_string(),
                },
                ProductionRaftNode {
                    node_id: 3,
                    addr: "127.0.0.1:19103".to_string(),
                },
                ProductionRaftNode {
                    node_id: 4,
                    addr: "127.0.0.1:19104".to_string(),
                },
            ],
            wal_dir: dir.display().to_string(),
            config: RaftConfig {
                enable_pre_vote: true,
                lease_duration_ms: 1_000,
                max_segment_bytes: 512,
                min_keep_segment_num: 1,
                ..RaftConfig::default()
            },
            rpc: RaftRpcRuntimeOptions::default(),
            security: temporalstore_rust::ProductionRaftSecurity::plaintext_for_local_chaos(
                "test-token",
            ),
            heartbeat_interval_ms: 100,
            election_tick_ms: 50,
            max_catchup_entries_per_heartbeat: 256,
            allow_plaintext_for_local_chaos: true,
        })
        .unwrap()
    }

    fn route(runtime: &ProductionRaftRuntime, method: &str, path: &str, body: Vec<u8>) -> Vec<u8> {
        route_with_admin(runtime, false, method, path, body)
    }

    fn route_with_admin(
        runtime: &ProductionRaftRuntime,
        local_admin_enabled: bool,
        method: &str,
        path: &str,
        body: Vec<u8>,
    ) -> Vec<u8> {
        let blocked = Arc::new(Mutex::new(BTreeSet::new()));
        let (code, body) = handle(
            runtime,
            local_admin_enabled,
            &blocked,
            HttpRequest {
                method: method.to_string(),
                path: path.to_string(),
                body,
            },
        );
        assert_eq!(code, 200);
        body
    }

    fn wait_for_http(addr: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while std::time::Instant::now() < deadline {
            if std::net::TcpStream::connect(addr).is_ok() {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        panic!("raft node test server {addr} did not start");
    }

    #[test]
    fn raft_control_routes_list_add_remove_and_trigger_snapshot() {
        let runtime = test_runtime();
        for node_id in [2, 3, 4] {
            runtime.cluster().set_alive(node_id, true).unwrap();
            runtime.cluster().catch_up(node_id).unwrap();
        }

        let listed: RaftControlMembershipResponse = serde_json::from_slice(&route(
            &runtime,
            "GET",
            "/raft/control/list_membership",
            Vec::new(),
        ))
        .unwrap();
        assert!(listed.status.ok);
        assert_eq!(listed.shard_id, 55);
        assert_eq!(listed.voters, vec![1, 2, 3, 4]);

        let removed: RaftMembershipApplyResponse = serde_json::from_slice(&route(
            &runtime,
            "POST",
            "/raft/control/remove_node",
            serde_json::to_vec(&RaftControlNodeRequest { node_id: 4 }).unwrap(),
        ))
        .unwrap();
        assert!(removed.status.ok);
        assert_eq!(runtime.cluster().membership().voters, vec![1, 2, 3]);
        for node_id in [2, 3] {
            runtime.cluster().set_alive(node_id, true).unwrap();
            runtime.cluster().catch_up(node_id).unwrap();
        }

        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "route-scale-down".to_string(),
                value: b"after-scale-down".to_vec(),
            })
            .unwrap();
        for node_id in [1, 2, 3] {
            let read = runtime
                .cluster()
                .read_from_replica(
                    node_id,
                    Command::StringGet {
                        key: "route-scale-down".to_string(),
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

        let added: RaftMembershipApplyResponse = serde_json::from_slice(&route(
            &runtime,
            "POST",
            "/raft/control/add_node",
            serde_json::to_vec(&RaftControlNodeRequest { node_id: 4 }).unwrap(),
        ))
        .unwrap();
        assert!(added.status.ok);
        assert_eq!(runtime.cluster().membership().voters, vec![1, 2, 3, 4]);
        for node_id in [2, 3, 4] {
            runtime.cluster().set_alive(node_id, true).unwrap();
            runtime.cluster().catch_up(node_id).unwrap();
        }

        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "route-scale-up".to_string(),
                value: b"after-scale-up".to_vec(),
            })
            .unwrap();
        for node_id in [1, 2, 3, 4] {
            let read = runtime
                .cluster()
                .read_from_replica(
                    node_id,
                    Command::StringGet {
                        key: "route-scale-up".to_string(),
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

        let snapshot: RaftControlSnapshotResponse = serde_json::from_slice(&route(
            &runtime,
            "POST",
            "/raft/control/trigger_snapshot",
            Vec::new(),
        ))
        .unwrap();
        assert!(snapshot.status.ok);
        assert_eq!(
            snapshot.report.unwrap().leader_id,
            runtime.cluster().leader_id()
        );

        let read_index: RaftControlReadIndexResponse = serde_json::from_slice(&route(
            &runtime,
            "POST",
            "/raft/control/read_index",
            serde_json::to_vec(&RaftControlNodeRequest { node_id: 2 }).unwrap(),
        ))
        .unwrap();
        assert!(read_index.status.ok);
        assert_eq!(read_index.response.unwrap().node_id, 2);

        let target_addr = "127.0.0.1:19102".to_string();
        let target_runtime = test_runtime_for_node(2);
        let target_blocked = Arc::new(Mutex::new(BTreeSet::new()));
        let target_blocked_for_server = Arc::clone(&target_blocked);
        std::thread::spawn(move || {
            serve(&target_addr, move |request| {
                handle(&target_runtime, false, &target_blocked_for_server, request)
            })
            .unwrap()
        });
        wait_for_http("127.0.0.1:19102");

        let transfer: RaftAdminLivenessResponse = serde_json::from_slice(&route(
            &runtime,
            "POST",
            "/raft/control/transfer_leader",
            serde_json::to_vec(&RaftControlNodeRequest { node_id: 2 }).unwrap(),
        ))
        .unwrap();
        assert!(transfer.status.ok, "{:?}", transfer.status);
        assert_eq!(runtime.cluster().leader_id(), 2);
    }

    // shared-corpus: raft_byteraft_metrics_admin_pipeline_status server_raft_byteraft_runtime_admin_route
    #[test]
    fn raft_control_exposes_byteraft_runtime_admin_report() {
        let runtime = test_runtime();
        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "raft-node-byteraft-admin-snapshot".to_string(),
                value: b"seed".to_vec(),
            })
            .unwrap();
        runtime.cluster().maybe_trigger_snapshot().unwrap();
        let snapshot = runtime.cluster().build_install_snapshot_request(2).unwrap();
        runtime
            .cluster()
            .receive_install_snapshot(snapshot)
            .unwrap();
        runtime.cluster().set_alive(3, false).unwrap();
        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "raft-node-byteraft-admin-lag".to_string(),
                value: b"lag".to_vec(),
            })
            .unwrap();
        runtime.cluster().set_alive(3, true).unwrap();
        let _ = runtime.cluster().check_write_authority(3);

        let report: temporalstore_rust::raft::ByteRaftRuntimeAdminReport =
            serde_json::from_slice(&route(
                &runtime,
                "GET",
                "/raft/control/byteraft_runtime_admin",
                Vec::new(),
            ))
            .unwrap();
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
        let local: temporalstore_rust::raft::ByteRaftLocalStatusReport =
            serde_json::from_slice(&route(
                &runtime,
                "GET",
                "/raft/control/byteraft_local_status",
                Vec::new(),
            ))
            .unwrap();
        assert_eq!(local.leader_id, report.leader_id);
        assert!(!local.peers.is_empty());
        assert!(local
            .peers
            .iter()
            .any(|peer| peer.pipeline_state.peer_id == peer.status.node_id));

        let metrics = String::from_utf8(route(&runtime, "GET", "/metrics", Vec::new())).unwrap();
        assert!(metrics.contains("temporalstore_raft_byteraft_ready"));
        assert!(metrics.contains("temporalstore_raft_byteraft_capability_ready"));
        assert!(metrics.contains("capability=\"wal_segment_lifecycle\""));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_queue_depth"));
        assert!(metrics.contains("replica_role=\"voter\""));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_queue_depth"));
        assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_installed_index"));
        assert!(metrics.contains("temporalstore_raft_byteraft_wal_segment_count"));
    }

    #[test]
    fn raft_admin_bootstrap_external_snapshot_installs_downloaded_snapshot() {
        let runtime = test_runtime();
        runtime.cluster().set_alive(3, false).unwrap();
        runtime
            .cluster()
            .propose(Command::StringSet {
                key: "external-snapshot-key".to_string(),
                value: b"external-snapshot-value".to_vec(),
            })
            .unwrap();
        runtime.cluster().set_alive(3, true).unwrap();
        assert_eq!(runtime.cluster().local_status(3).unwrap().commit_index, 0);

        let tmp = tempfile::tempdir().unwrap();
        let object_root = tmp.path().join("objects");
        let publish_local_root = tmp.path().join("publish-local");
        let restore_local_root = tmp.path().join("restore-local");
        let published: RaftAdminPublishExternalSnapshotResponse =
            serde_json::from_slice(&route_with_admin(
                &runtime,
                true,
                "POST",
                "/raft/admin/publish_external_snapshot",
                serde_json::to_vec(&RaftAdminPublishExternalSnapshotRequest {
                    object_root: object_root.display().to_string(),
                    local_root: publish_local_root.display().to_string(),
                    cluster_id: "cluster-a".to_string(),
                    bucket: "test".to_string(),
                })
                .unwrap(),
            ))
            .unwrap();
        assert!(published.status.ok, "{published:?}");
        let snapshot_ref = published.report.unwrap().meta_ref;

        let response: RaftAdminBootstrapExternalSnapshotResponse =
            serde_json::from_slice(&route_with_admin(
                &runtime,
                true,
                "POST",
                "/raft/admin/bootstrap_external_snapshot",
                serde_json::to_vec(&RaftAdminBootstrapExternalSnapshotRequest {
                    target_id: 3,
                    snapshot: snapshot_ref,
                    object_root: object_root.display().to_string(),
                    local_root: restore_local_root.display().to_string(),
                    cluster_id: "cluster-a".to_string(),
                    bucket: "test".to_string(),
                })
                .unwrap(),
            ))
            .unwrap();
        assert!(response.status.ok, "{response:?}");
        let plan = response.plan.unwrap();
        assert_eq!(plan.target_id, 3);
        assert_eq!(
            plan.transfer.mode,
            temporalstore_rust::raft::RaftSnapshotTransferMode::ExternalStore
        );

        assert_eq!(runtime.cluster().local_status(3).unwrap().commit_index, 1);
        let read = runtime
            .cluster()
            .read_local(
                3,
                Command::StringGet {
                    key: "external-snapshot-key".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            read,
            CommandResponse::Bytes {
                value: Some(b"external-snapshot-value".to_vec())
            }
        );
    }
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}
