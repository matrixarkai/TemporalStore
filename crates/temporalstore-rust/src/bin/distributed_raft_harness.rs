use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json_with_options, serve, HttpRequest,
    HttpRequestOptions,
};
use temporalstore_rust::meta::ShardSnapshotRef;
use temporalstore_rust::raft::{RaftReplicaBootstrapPlan, RaftSnapshotPublishReport};
use temporalstore_rust::{
    handle_authenticated_raft_http, Command, CommandResponse, DistributedRaftCommandResponse,
    DistributedRaftProposeRequest, DistributedRaftReadRequest, ProductionRaftEngineKind,
    ProductionRaftNode, ProductionRaftRuntime, ProductionRaftRuntimeOptions,
    ProductionRaftSecurity, RaftApplyHealth, RaftClusterStatus, RaftConfig,
    RaftMembershipChangeReport, RaftNodeId, RaftRpcRuntimeOptions, Status,
};
use temporalstore_snapshot::{FileObjectStore, S3SnapshotStore};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
    shard_id: u64,
    auth_token: String,
    key: String,
    value: Vec<u8>,
}

#[derive(Debug, Serialize)]
struct DistributedRaftSummary {
    root: String,
    shard_id: u64,
    nodes: Vec<NodeSummary>,
    proposal_status: Status,
    replica_reads: Vec<ReplicaReadSummary>,
    follower_write_rejection: Status,
    transfer_leader_to_node: RaftNodeId,
    post_transfer_write: Status,
    scale_down: Vec<MembershipSummary>,
    post_scale_down_write: Status,
    scale_down_reads: Vec<ReplicaReadSummary>,
    scale_up: Vec<MembershipSummary>,
    post_scale_up_write: Status,
    scale_up_reads: Vec<ReplicaReadSummary>,
    external_snapshot_publish: Status,
    external_snapshot_bootstrap: Status,
    external_snapshot_read: ReplicaReadSummary,
}

#[derive(Debug, Serialize)]
struct NodeSummary {
    node_id: RaftNodeId,
    addr: String,
    wal_dir: String,
    status: RaftClusterStatus,
    apply_health: RaftApplyHealth,
    wal_files: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ReplicaReadSummary {
    node_id: RaftNodeId,
    status: Status,
    value: Option<String>,
}

#[derive(Debug, Serialize)]
struct MembershipSummary {
    node_id: RaftNodeId,
    status: Status,
    voters: Vec<RaftNodeId>,
    leader_id: RaftNodeId,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotRequest {
    object_root: String,
    local_root: String,
    cluster_id: String,
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminPublishExternalSnapshotResponse {
    status: Status,
    report: Option<RaftSnapshotPublishReport>,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminBootstrapExternalSnapshotRequest {
    target_id: RaftNodeId,
    snapshot: ShardSnapshotRef,
    object_root: String,
    local_root: String,
    cluster_id: String,
    bucket: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct RaftAdminBootstrapExternalSnapshotResponse {
    status: Status,
    plan: Option<RaftReplicaBootstrapPlan>,
}

fn main() {
    let options = parse_options();
    fs::create_dir_all(&options.root).expect("failed to create harness root");
    let nodes = vec![
        ProductionRaftNode {
            node_id: 1,
            addr: free_local_addr(),
        },
        ProductionRaftNode {
            node_id: 2,
            addr: free_local_addr(),
        },
        ProductionRaftNode {
            node_id: 3,
            addr: free_local_addr(),
        },
        ProductionRaftNode {
            node_id: 4,
            addr: free_local_addr(),
        },
    ];
    let mut runtimes = Vec::new();
    for node in &nodes {
        let runtime = ProductionRaftRuntime::start(runtime_options(&options, &nodes, node.node_id))
            .expect("failed to start data-node raft runtime");
        let _timer = runtime.start_timer_loop();
        let runtime_for_server = runtime.clone();
        let addr = node.addr.clone();
        thread::spawn(move || {
            serve(&addr, move |request| handle(&runtime_for_server, request))
                .expect("data-node raft server failed");
        });
        runtimes.push(runtime);
    }
    for node in &nodes {
        wait_for_http(&node.addr);
    }

    let proposal: DistributedRaftCommandResponse = post_json_with_options(
        &nodes[0].addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: options.key.clone(),
                value: options.value.clone(),
            },
        },
        request_options(),
    )
    .expect("raft proposal request failed");
    assert!(
        proposal.status.ok,
        "raft proposal failed: {:?}",
        proposal.status
    );
    wait_for_distributed_majority(&runtimes, &nodes);

    let replica_reads = nodes
        .iter()
        .map(|node| wait_for_replica_read(node, &options))
        .collect::<Vec<_>>();
    let follower_write_rejection = reject_direct_follower_write(&nodes[1], &options);

    wait_for_distributed_majority(&runtimes, &nodes);
    for runtime in &runtimes {
        runtime
            .cluster()
            .transfer_leader(2)
            .expect("leader transfer to node 2 should pass");
    }
    wait_for_distributed_majority(&runtimes, &nodes);
    let post_transfer_write = propose_key(
        &nodes[1],
        "distributed-transfer-leader-key",
        b"after-transfer",
    );
    assert!(
        post_transfer_write.ok,
        "post-transfer write failed: {:?}",
        post_transfer_write
    );
    wait_for_distributed_majority(&runtimes, &nodes);

    let scale_down = apply_membership_on_all(&runtimes, &nodes, &[1, 2, 3]);
    wait_for_distributed_majority(&runtimes, &nodes[..3]);
    let post_scale_down_write =
        propose_key(&nodes[1], "distributed-scale-down-key", b"after-scale-down");
    assert!(
        post_scale_down_write.ok,
        "post-scale-down write failed: {:?}",
        post_scale_down_write
    );
    wait_for_distributed_majority(&runtimes, &nodes[..3]);
    let scale_down_reads = nodes
        .iter()
        .take(3)
        .map(|node| wait_for_key(node, "distributed-scale-down-key", b"after-scale-down"))
        .collect::<Vec<_>>();

    let scale_up = apply_membership_on_all(&runtimes, &nodes, &[1, 2, 3, 4]);
    wait_for_distributed_majority(&runtimes, &nodes);
    let post_scale_up_write = propose_key(&nodes[1], "distributed-scale-up-key", b"after-scale-up");
    assert!(
        post_scale_up_write.ok,
        "post-scale-up write failed: {:?}",
        post_scale_up_write
    );
    wait_for_distributed_majority(&runtimes, &nodes);
    let scale_up_reads = nodes
        .iter()
        .map(|node| wait_for_key(node, "distributed-scale-up-key", b"after-scale-up"))
        .collect::<Vec<_>>();

    let snapshot_target_id = 3;
    for runtime in &runtimes {
        runtime
            .cluster()
            .set_alive(snapshot_target_id, false)
            .expect("snapshot target should exist");
    }
    let external_snapshot_write = propose_key(
        &nodes[1],
        "distributed-external-snapshot-key",
        b"from-external-snapshot",
    );
    assert!(
        external_snapshot_write.ok,
        "external-snapshot source write failed: {:?}",
        external_snapshot_write
    );
    wait_for_distributed_majority(&runtimes, &nodes[..3]);
    for runtime in &runtimes {
        runtime
            .cluster()
            .set_alive(snapshot_target_id, true)
            .expect("snapshot target should exist");
    }
    let object_root = options.root.join("snapshot-objects");
    let publish_local_root = options.root.join("snapshot-publish-local");
    let restore_local_root = options.root.join("snapshot-restore-local");
    let published: RaftAdminPublishExternalSnapshotResponse = post_json_with_options(
        &nodes[1].addr,
        "/raft/admin/publish_external_snapshot",
        &RaftAdminPublishExternalSnapshotRequest {
            object_root: object_root.display().to_string(),
            local_root: publish_local_root.display().to_string(),
            cluster_id: "cluster-a".to_string(),
            bucket: "test".to_string(),
        },
        request_options(),
    )
    .expect("external snapshot publish request failed");
    assert!(
        published.status.ok,
        "external snapshot publish failed: {:?}",
        published
    );
    let bootstrapped: RaftAdminBootstrapExternalSnapshotResponse = post_json_with_options(
        &nodes[2].addr,
        "/raft/admin/bootstrap_external_snapshot",
        &RaftAdminBootstrapExternalSnapshotRequest {
            target_id: snapshot_target_id,
            snapshot: published
                .report
                .as_ref()
                .expect("publish report should be present")
                .meta_ref
                .clone(),
            object_root: object_root.display().to_string(),
            local_root: restore_local_root.display().to_string(),
            cluster_id: "cluster-a".to_string(),
            bucket: "test".to_string(),
        },
        request_options(),
    )
    .expect("external snapshot bootstrap request failed");
    assert!(
        bootstrapped.status.ok,
        "external snapshot bootstrap failed: {:?}",
        bootstrapped
    );
    let external_snapshot_read = wait_for_key(
        &nodes[2],
        "distributed-external-snapshot-key",
        b"from-external-snapshot",
    );

    wait_for_distributed_apply_health(&runtimes, &nodes, 0);

    let node_summaries = nodes
        .iter()
        .map(|node| {
            let wal_dir = wal_dir(&options.root, node.node_id);
            NodeSummary {
                node_id: node.node_id,
                addr: node.addr.clone(),
                wal_dir: wal_dir.display().to_string(),
                status: get_json_with_options(&node.addr, "/raft/status", request_options())
                    .expect("status request failed"),
                apply_health: post_json_with_options(
                    &node.addr,
                    "/raft/apply_health",
                    &RaftApplyHealthRequest {
                        max_allowed_apply_lag: 0,
                    },
                    request_options(),
                )
                .expect("apply health request failed"),
                wal_files: list_files(&wal_dir),
            }
        })
        .collect::<Vec<_>>();

    println!(
        "{}",
        serde_json::to_string_pretty(&DistributedRaftSummary {
            root: options.root.display().to_string(),
            shard_id: options.shard_id,
            nodes: node_summaries,
            proposal_status: proposal.status,
            replica_reads,
            follower_write_rejection,
            transfer_leader_to_node: 2,
            post_transfer_write,
            scale_down,
            post_scale_down_write,
            scale_down_reads,
            scale_up,
            post_scale_up_write,
            scale_up_reads,
            external_snapshot_publish: published.status,
            external_snapshot_bootstrap: bootstrapped.status,
            external_snapshot_read,
        })
        .expect("summary should serialize")
    );
}

fn handle(runtime: &ProductionRaftRuntime, request: HttpRequest) -> (u16, Vec<u8>) {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/health") => json_response(200, &Status::ok()),
        ("GET", "/raft/status") => json_response(200, &runtime.status()),
        ("POST", "/raft/apply_health") => {
            match parse_json::<RaftApplyHealthRequest>(&request.body) {
                Ok(req) => json_response(
                    200,
                    &runtime.cluster().apply_health(req.max_allowed_apply_lag),
                ),
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
        ("POST", "/raft/admin/publish_external_snapshot") => {
            match parse_json::<RaftAdminPublishExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = std::sync::Arc::new(FileObjectStore::with_uri_scheme(
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
        ("POST", "/raft/admin/bootstrap_external_snapshot") => {
            match parse_json::<RaftAdminBootstrapExternalSnapshotRequest>(&request.body) {
                Ok(req) => {
                    let store = std::sync::Arc::new(FileObjectStore::with_uri_scheme(
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
        _ => handle_authenticated_raft_http(
            &runtime.cluster(),
            request,
            runtime.peer_auth_token().unwrap_or_default(),
        ),
    }
}

#[derive(Debug, Serialize, serde::Deserialize)]
struct RaftApplyHealthRequest {
    #[serde(default)]
    max_allowed_apply_lag: u64,
}

fn propose_key(node: &ProductionRaftNode, key: &str, value: &[u8]) -> Status {
    let response: DistributedRaftCommandResponse = post_json_with_options(
        &node.addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: key.to_string(),
                value: value.to_vec(),
            },
        },
        request_options(),
    )
    .expect("raft proposal request failed");
    response.status
}

fn reject_direct_follower_write(node: &ProductionRaftNode, options: &HarnessOptions) -> Status {
    let response: DistributedRaftCommandResponse = post_json_with_options(
        &node.addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: format!("{}-follower-reject", options.key),
                value: b"must-not-commit-from-follower".to_vec(),
            },
        },
        request_options(),
    )
    .expect("follower proposal request failed");
    assert!(
        !response.status.ok,
        "direct follower write should be rejected: {:?}",
        response.status
    );
    response.status
}

fn apply_membership_on_all(
    runtimes: &[ProductionRaftRuntime],
    nodes: &[ProductionRaftNode],
    voters: &[RaftNodeId],
) -> Vec<MembershipSummary> {
    runtimes
        .iter()
        .zip(nodes.iter())
        .map(|(runtime, node)| {
            runtime
                .cluster()
                .catch_up_live_followers()
                .expect("followers should catch up before membership change");
            let report = runtime.apply_membership_change_safely(voters.iter().copied());
            membership_summary(node.node_id, report)
        })
        .collect()
}

fn wait_for_distributed_majority(
    runtimes: &[ProductionRaftRuntime],
    live_nodes: &[ProductionRaftNode],
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for runtime in runtimes {
            for node in live_nodes {
                runtime
                    .cluster()
                    .set_alive(node.node_id, true)
                    .expect("harness node should exist in every raft view");
                runtime
                    .cluster()
                    .catch_up(node.node_id)
                    .expect("harness node should catch up in every raft view");
            }
        }
        let statuses = runtimes
            .iter()
            .map(|runtime| runtime.cluster().status())
            .collect::<Vec<_>>();
        if statuses
            .iter()
            .all(|status| status.has_majority && status.leader_lease_valid)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "distributed raft majority did not converge: {:?}",
            statuses
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn wait_for_distributed_apply_health(
    runtimes: &[ProductionRaftRuntime],
    nodes: &[ProductionRaftNode],
    max_allowed_apply_lag: u64,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for runtime in runtimes {
            for node in nodes {
                runtime
                    .cluster()
                    .catch_up(node.node_id)
                    .expect("live node should catch up before final distributed summary");
            }
        }
        let health = nodes
            .iter()
            .map(|node| {
                post_json_with_options::<_, RaftApplyHealth>(
                    &node.addr,
                    "/raft/apply_health",
                    &RaftApplyHealthRequest {
                        max_allowed_apply_lag,
                    },
                    request_options(),
                )
                .expect("apply health request failed")
            })
            .collect::<Vec<_>>();
        if health.iter().all(|health| health.healthy) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "distributed raft apply health did not converge: {:?}",
            health
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn membership_summary(
    node_id: RaftNodeId,
    report: Result<RaftMembershipChangeReport, temporalstore_rust::RaftError>,
) -> MembershipSummary {
    match report {
        Ok(report) => MembershipSummary {
            node_id,
            status: Status::ok(),
            voters: report.committed_membership.voters,
            leader_id: report.leader_id,
        },
        Err(err) => MembershipSummary {
            node_id,
            status: Status::error("raft_error", err.to_string()),
            voters: Vec::new(),
            leader_id: 0,
        },
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

fn uri_scheme(uri: &str) -> String {
    uri.split_once("://")
        .map(|(scheme, _)| scheme.to_string())
        .unwrap_or_else(|| "file".to_string())
}

fn wait_for_replica_read(
    node: &ProductionRaftNode,
    options: &HarnessOptions,
) -> ReplicaReadSummary {
    wait_for_key(node, &options.key, &options.value)
}

fn wait_for_key(node: &ProductionRaftNode, key: &str, expected: &[u8]) -> ReplicaReadSummary {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response: DistributedRaftCommandResponse = post_json_with_options(
            &node.addr,
            "/raft/read",
            &DistributedRaftReadRequest {
                node_id: node.node_id,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            },
            request_options(),
        )
        .expect("raft read request failed");
        let value = match &response.response {
            CommandResponse::Bytes { value: Some(bytes) } => {
                Some(String::from_utf8_lossy(bytes).to_string())
            }
            _ => None,
        };
        if value.as_deref() == Some(String::from_utf8_lossy(expected).as_ref()) {
            return ReplicaReadSummary {
                node_id: node.node_id,
                status: response.status,
                value,
            };
        }
        assert!(
            Instant::now() < deadline,
            "replica {} did not catch up; last response: {:?}",
            node.node_id,
            response
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn runtime_options(
    options: &HarnessOptions,
    nodes: &[ProductionRaftNode],
    local_node_id: RaftNodeId,
) -> ProductionRaftRuntimeOptions {
    ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
        shard_id: options.shard_id,
        local_node_id,
        nodes: nodes.to_vec(),
        wal_dir: wal_dir(&options.root, local_node_id).display().to_string(),
        config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions {
            max_retries: 3,
            deadline_ms: 1_000,
            ..RaftRpcRuntimeOptions::default()
        },
        security: ProductionRaftSecurity::plaintext_for_local_chaos(options.auth_token.clone()),
        heartbeat_interval_ms: 50,
        election_tick_ms: 10,
        max_catchup_entries_per_heartbeat: 256,
        allow_plaintext_for_local_chaos: true,
    }
}

fn request_options() -> HttpRequestOptions {
    HttpRequestOptions {
        connect_timeout_ms: 1_000,
        io_timeout_ms: 5_000,
        max_retries: 3,
    }
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if get_json_with_options::<Status>(addr, "/health", request_options()).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "raft node {addr} did not start");
        thread::sleep(Duration::from_millis(25));
    }
}

fn free_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
    listener
        .local_addr()
        .expect("failed to read ephemeral port")
        .to_string()
}

fn wal_dir(root: &Path, node_id: RaftNodeId) -> PathBuf {
    root.join(format!("node-{node_id}"))
}

fn list_files(root: &Path) -> Vec<String> {
    let mut out = Vec::new();
    collect_files(root, root, &mut out);
    out.sort();
    out
}

fn collect_files(root: &Path, current: &Path, out: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(current) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out);
        } else if let Ok(relative) = path.strip_prefix(root) {
            out.push(relative.display().to_string());
        }
    }
}

fn parse_options() -> HarnessOptions {
    let mut root =
        std::env::temp_dir().join(format!("temporalstore-distributed-raft-{}", now_ms()));
    let mut shard_id = 1u64;
    let mut auth_token = "local-raft-token".to_string();
    let mut key = "distributed-raft-key".to_string();
    let mut value = b"replicated-value".to_vec();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let Some(raw) = args.next() else {
            usage_and_exit();
        };
        match flag.as_str() {
            "--root" => root = PathBuf::from(raw),
            "--shard-id" => shard_id = parse(&raw, &flag),
            "--auth-token" => auth_token = raw,
            "--key" => key = raw,
            "--value" => value = raw.into_bytes(),
            _ => usage_and_exit(),
        }
    }
    HarnessOptions {
        root,
        shard_id,
        auth_token,
        key,
        value,
    }
}

fn parse<T: std::str::FromStr>(value: &str, flag: &str) -> T {
    value.parse().unwrap_or_else(|_| {
        eprintln!("invalid value for {flag}: {value}");
        std::process::exit(2);
    })
}

fn usage_and_exit() -> ! {
    eprintln!(
        "usage: distributed_raft_harness [--root <path>] [--shard-id <id>] [--auth-token <token>] [--key <key>] [--value <value>]"
    );
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
