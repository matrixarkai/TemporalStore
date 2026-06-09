use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::http::{get_json_with_options, post_json_with_options, HttpRequestOptions};
use temporalstore_rust::{
    Command, CommandResponse, DistributedRaftCommandResponse, DistributedRaftProposeRequest,
    DistributedRaftReadRequest, ProductionRaftNode, RaftClusterStatus, RaftFailoverReport,
    RaftNodeId, Status,
};

#[derive(Debug, Clone)]
struct HarnessOptions {
    root: PathBuf,
    shard_id: u64,
    auth_token: String,
    heartbeat_ms: u64,
}

#[derive(Debug, Serialize)]
struct SecondaryReplicationSummary {
    root: String,
    shard_id: u64,
    nodes: Vec<NodeSummary>,
    writes: Vec<WriteSummary>,
    reads_after_restart: Vec<ReadSummary>,
    restarted_secondary: RaftNodeId,
    partition: PartitionSummary,
    crashed_leader: RaftNodeId,
    failover: AdminFailoverResponse,
    reads_after_leader_crash: Vec<ReadSummary>,
}

#[derive(Debug, Serialize)]
struct NodeSummary {
    node_id: RaftNodeId,
    addr: String,
    wal_dir: String,
    status: RaftClusterStatus,
    wal_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WriteSummary {
    key: String,
    status: Status,
}

#[derive(Debug, Serialize)]
struct ReadSummary {
    node_id: RaftNodeId,
    key: String,
    value: Option<String>,
    status: Status,
}

#[derive(Debug, Serialize)]
struct PartitionSummary {
    isolated_node: RaftNodeId,
    majority_write: WriteSummary,
    isolated_read_status: Status,
    healed_read: ReadSummary,
}

#[derive(Debug, Serialize)]
struct AdminLivenessRequest {
    node_id: RaftNodeId,
    alive: bool,
}

#[derive(Debug, Serialize)]
struct AdminElectRequest {
    node_id: RaftNodeId,
}

#[derive(Debug, Serialize)]
struct AdminPeerBlockRequest {
    peer_id: RaftNodeId,
    blocked: bool,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct AdminLivenessResponse {
    status: Status,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct AdminFailoverResponse {
    status: Status,
    report: Option<RaftFailoverReport>,
}

struct ChildNode {
    node: ProductionRaftNode,
    child: Child,
}

impl Drop for ChildNode {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn main() {
    let options = parse_options();
    fs::create_dir_all(&options.root).expect("failed to create harness root");
    let raft_node_bin = raft_node_binary();
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
    ];
    let nodes_env = nodes
        .iter()
        .map(|node| format!("{}={}", node.node_id, node.addr))
        .collect::<Vec<_>>()
        .join(",");
    let mut children = nodes
        .iter()
        .map(|node| spawn_node(&raft_node_bin, &options, &nodes_env, node))
        .collect::<Vec<_>>();
    for node in &nodes {
        wait_for_http(&node.addr);
    }
    initialize_liveness(&nodes);

    let writes = vec![propose(&nodes[0], "secondary-before-restart", "v1")];
    for node in &nodes {
        wait_for_value(node, node.node_id, "secondary-before-restart", "v1");
    }
    let mut writes = writes;
    writes.push(propose(&nodes[0], "secondary-before-stop", "v2"));
    wait_for_value(&nodes[1], 2, "secondary-before-stop", "v2");
    wait_for_value(&nodes[2], 3, "secondary-before-stop", "v2");

    let restarted_secondary = 3;
    let stopped_index = children
        .iter()
        .position(|child| child.node.node_id == restarted_secondary)
        .expect("secondary child should exist");
    let mut stopped = children.remove(stopped_index);
    stopped.child.kill().expect("failed to kill secondary");
    let _ = stopped.child.wait();
    drop(stopped);

    writes.push(propose(&nodes[0], "secondary-while-down", "v3"));
    wait_for_value(&nodes[1], 2, "secondary-while-down", "v3");

    let restarted_node = nodes
        .iter()
        .find(|node| node.node_id == restarted_secondary)
        .expect("secondary node should exist");
    children.push(spawn_node(
        &raft_node_bin,
        &options,
        &nodes_env,
        restarted_node,
    ));
    wait_for_http(&restarted_node.addr);
    initialize_liveness(&nodes);

    wait_for_value(
        restarted_node,
        restarted_secondary,
        "secondary-while-down",
        "v3",
    );
    writes.push(propose(&nodes[0], "secondary-after-restart", "v4"));

    let mut reads_after_restart = Vec::new();
    for node in &nodes {
        reads_after_restart.push(wait_for_value(
            node,
            node.node_id,
            "secondary-before-restart",
            "v1",
        ));
        reads_after_restart.push(wait_for_value(
            node,
            node.node_id,
            "secondary-while-down",
            "v3",
        ));
        reads_after_restart.push(wait_for_value(
            node,
            node.node_id,
            "secondary-after-restart",
            "v4",
        ));
    }

    let partition = run_partition_phase(&nodes);
    writes.push(partition.majority_write.clone());

    let crashed_leader = 1;
    let leader_index = children
        .iter()
        .position(|child| child.node.node_id == crashed_leader)
        .expect("leader child should exist");
    let mut crashed = children.remove(leader_index);
    crashed.child.kill().expect("failed to kill leader");
    let _ = crashed.child.wait();
    drop(crashed);

    for survivor in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        mark_liveness(survivor, crashed_leader, false);
        for peer in nodes.iter().filter(|node| node.node_id != crashed_leader) {
            mark_liveness(survivor, peer.node_id, true);
        }
    }
    let new_leader = nodes
        .iter()
        .find(|node| node.node_id == 2)
        .expect("surviving node should exist");
    for survivor in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        elect_leader(survivor, new_leader.node_id);
    }
    let failover = trigger_failover(new_leader);
    assert!(
        failover.status.ok,
        "leader failover failed: {:?}",
        failover.status
    );
    writes.push(propose(new_leader, "after-leader-crash", "v5"));

    let mut reads_after_leader_crash = Vec::new();
    for node in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        reads_after_leader_crash.push(wait_for_value(
            node,
            node.node_id,
            "after-leader-crash",
            "v5",
        ));
    }
    for node in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        wait_for_cluster_commit(node, writes.len() as u64);
    }

    let node_summaries = nodes
        .iter()
        .filter(|node| node.node_id != crashed_leader)
        .map(|node| {
            let wal_dir = wal_dir(&options.root, node.node_id);
            NodeSummary {
                node_id: node.node_id,
                addr: node.addr.clone(),
                wal_dir: wal_dir.display().to_string(),
                status: get_json_with_options(&node.addr, "/raft/status", request_options())
                    .expect("status request failed"),
                wal_files: list_files(&wal_dir),
            }
        })
        .collect::<Vec<_>>();
    validate_surviving_cluster(
        &node_summaries,
        crashed_leader,
        new_leader.node_id,
        writes.len() as u64,
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&SecondaryReplicationSummary {
            root: options.root.display().to_string(),
            shard_id: options.shard_id,
            nodes: node_summaries,
            writes,
            reads_after_restart,
            restarted_secondary,
            partition,
            crashed_leader,
            failover,
            reads_after_leader_crash,
        })
        .expect("summary should serialize")
    );
}

fn run_partition_phase(nodes: &[ProductionRaftNode]) -> PartitionSummary {
    let isolated_node = 3;
    for majority in nodes.iter().filter(|node| node.node_id != isolated_node) {
        mark_liveness(majority, isolated_node, false);
        for peer in nodes.iter().filter(|node| node.node_id != isolated_node) {
            mark_liveness(majority, peer.node_id, true);
        }
    }
    let isolated = nodes
        .iter()
        .find(|node| node.node_id == isolated_node)
        .expect("isolated node should exist");
    for peer in nodes.iter().filter(|node| node.node_id != isolated_node) {
        block_peer(isolated, peer.node_id, true);
    }
    for peer in nodes.iter().filter(|node| node.node_id != isolated_node) {
        mark_liveness(isolated, peer.node_id, false);
    }
    mark_liveness(isolated, isolated_node, true);

    let leader = nodes
        .iter()
        .find(|node| node.node_id == 1)
        .expect("leader should exist");
    let majority_write = propose(leader, "partition-majority", "v-partition");
    for majority in nodes.iter().filter(|node| node.node_id != isolated_node) {
        wait_for_value(
            majority,
            majority.node_id,
            "partition-majority",
            "v-partition",
        );
    }

    let isolated_read = read_value(isolated, isolated_node, "partition-majority");
    assert!(
        !isolated_read.status.ok,
        "isolated follower unexpectedly served a read during partition: {:?}",
        isolated_read
    );

    for peer in nodes.iter().filter(|node| node.node_id != isolated_node) {
        block_peer(isolated, peer.node_id, false);
    }
    initialize_liveness(nodes);
    let _heal_trigger = propose(leader, "partition-heal-trigger", "v-heal");
    let healed_read = wait_for_value(isolated, isolated_node, "partition-majority", "v-partition");
    PartitionSummary {
        isolated_node,
        majority_write,
        isolated_read_status: isolated_read.status,
        healed_read,
    }
}

fn validate_surviving_cluster(
    nodes: &[NodeSummary],
    crashed_leader: RaftNodeId,
    expected_leader: RaftNodeId,
    min_commit_index: u64,
) {
    assert!(
        nodes.len() >= 2,
        "leader-crash phase must leave at least two surviving nodes"
    );
    for node in nodes {
        assert!(
            node.status.has_majority,
            "node {} does not report majority after leader crash: {:?}",
            node.node_id, node.status
        );
        assert_eq!(
            node.status.leader_id, expected_leader,
            "node {} does not agree on the post-crash leader",
            node.node_id
        );
        assert!(
            node.status.commit_index >= min_commit_index,
            "node {} commit index {} is behind expected {}",
            node.node_id,
            node.status.commit_index,
            min_commit_index
        );
        let crashed = node
            .status
            .nodes
            .iter()
            .find(|status| status.node_id == crashed_leader)
            .expect("status should include crashed leader voter");
        assert!(
            !crashed.alive,
            "node {} still reports crashed leader {} alive",
            node.node_id, crashed_leader
        );
        for survivor in node
            .status
            .nodes
            .iter()
            .filter(|status| status.node_id != crashed_leader)
        {
            assert!(
                survivor.alive && survivor.lag == 0,
                "node {} sees survivor {} unhealthy after leader crash: {:?}",
                node.node_id,
                survivor.node_id,
                survivor
            );
        }
    }
}

fn spawn_node(
    raft_node_bin: &Path,
    options: &HarnessOptions,
    nodes_env: &str,
    node: &ProductionRaftNode,
) -> ChildNode {
    let child = ProcessCommand::new(raft_node_bin)
        .env("TS_RAFT_NODE_ID", node.node_id.to_string())
        .env("TS_RAFT_SHARD_ID", options.shard_id.to_string())
        .env("TS_RAFT_BIND_ADDR", &node.addr)
        .env("TS_RAFT_NODES", nodes_env)
        .env("TS_RAFT_WAL_DIR", wal_dir(&options.root, node.node_id))
        .env("TS_RAFT_AUTH_TOKEN", &options.auth_token)
        .env(
            "TS_RAFT_HEARTBEAT_INTERVAL_MS",
            options.heartbeat_ms.to_string(),
        )
        .env("TS_RAFT_ELECTION_TICK_MS", "10")
        .env("TS_RAFT_RPC_DEADLINE_MS", "1000")
        .env("TS_RAFT_RPC_RETRIES", "3")
        .env("TS_RAFT_ALLOW_PLAINTEXT", "true")
        .env("TS_RAFT_ENABLE_LOCAL_ADMIN", "true")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn raft_node");
    ChildNode {
        node: node.clone(),
        child,
    }
}

fn propose(node: &ProductionRaftNode, key: &str, value: &str) -> WriteSummary {
    let response: DistributedRaftCommandResponse = post_json_with_options(
        &node.addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: key.to_string(),
                value: value.as_bytes().to_vec(),
            },
        },
        request_options(),
    )
    .expect("proposal request failed");
    if !response.status.ok {
        let status: RaftClusterStatus =
            get_json_with_options(&node.addr, "/raft/status", request_options())
                .expect("status request after failed proposal failed");
        panic!(
            "proposal for key {key} on node {} failed: {:?}; local status: {:?}",
            node.node_id, response.status, status
        );
    }
    WriteSummary {
        key: key.to_string(),
        status: response.status,
    }
}

fn mark_liveness(node: &ProductionRaftNode, target_id: RaftNodeId, alive: bool) {
    let response: AdminLivenessResponse = post_json_with_options(
        &node.addr,
        "/raft/admin/liveness",
        &AdminLivenessRequest {
            node_id: target_id,
            alive,
        },
        request_options(),
    )
    .expect("liveness admin request failed");
    assert!(
        response.status.ok,
        "liveness admin request failed: {:?}",
        response.status
    );
}

fn initialize_liveness(nodes: &[ProductionRaftNode]) {
    for node in nodes {
        for peer in nodes {
            mark_liveness(node, peer.node_id, true);
        }
    }
}

fn block_peer(node: &ProductionRaftNode, peer_id: RaftNodeId, blocked: bool) {
    let response: AdminLivenessResponse = post_json_with_options(
        &node.addr,
        "/raft/admin/block_peer",
        &AdminPeerBlockRequest { peer_id, blocked },
        request_options(),
    )
    .expect("block peer admin request failed");
    assert!(
        response.status.ok,
        "block peer admin request failed: {:?}",
        response.status
    );
}

fn elect_leader(node: &ProductionRaftNode, leader_id: RaftNodeId) {
    let response: AdminLivenessResponse = post_json_with_options(
        &node.addr,
        "/raft/admin/elect",
        &AdminElectRequest { node_id: leader_id },
        request_options(),
    )
    .expect("elect admin request failed");
    assert!(
        response.status.ok,
        "elect admin request failed: {:?}",
        response.status
    );
}

fn trigger_failover(node: &ProductionRaftNode) -> AdminFailoverResponse {
    post_json_with_options(
        &node.addr,
        "/raft/admin/failover",
        &serde_json::json!({}),
        request_options(),
    )
    .expect("failover admin request failed")
}

fn wait_for_value(
    node: &ProductionRaftNode,
    node_id: RaftNodeId,
    key: &str,
    expected: &str,
) -> ReadSummary {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let read = read_value(node, node_id, key);
        if read.value.as_deref() == Some(expected) {
            return read;
        }
        assert!(
            Instant::now() < deadline,
            "node {node_id} did not return {key}={expected}; last response: {:?}",
            read
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn read_value(node: &ProductionRaftNode, node_id: RaftNodeId, key: &str) -> ReadSummary {
    let response: DistributedRaftCommandResponse = post_json_with_options(
        &node.addr,
        "/raft/read",
        &DistributedRaftReadRequest {
            node_id,
            command: Command::StringGet {
                key: key.to_string(),
            },
        },
        request_options(),
    )
    .expect("read request failed");
    let value = match &response.response {
        CommandResponse::Bytes { value: Some(bytes) } => {
            Some(String::from_utf8_lossy(bytes).to_string())
        }
        _ => None,
    };
    ReadSummary {
        node_id,
        key: key.to_string(),
        value,
        status: response.status,
    }
}

fn wait_for_cluster_commit(node: &ProductionRaftNode, min_commit_index: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status: RaftClusterStatus =
            get_json_with_options(&node.addr, "/raft/status", request_options())
                .expect("status request failed");
        let live_caught_up = status
            .nodes
            .iter()
            .filter(|replica| replica.alive)
            .all(|replica| replica.lag == 0 && replica.commit_index >= min_commit_index);
        if status.has_majority && status.commit_index >= min_commit_index && live_caught_up {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "node {} did not reach commit {}; last status: {:?}",
            node.node_id,
            min_commit_index,
            status
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if get_json_with_options::<Status>(addr, "/health", request_options()).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "raft node {addr} did not start");
        thread::sleep(Duration::from_millis(50));
    }
}

fn request_options() -> HttpRequestOptions {
    HttpRequestOptions {
        connect_timeout_ms: 1_000,
        io_timeout_ms: 5_000,
        max_retries: 3,
    }
}

fn raft_node_binary() -> PathBuf {
    let current = std::env::current_exe().expect("failed to resolve current executable");
    let candidate = current
        .parent()
        .expect("current executable should have a parent")
        .join(format!("raft_node{}", std::env::consts::EXE_SUFFIX));
    if !candidate.exists() {
        eprintln!(
            "missing raft_node binary at {}; run `cargo build -p temporalstore-rust --bins` first",
            candidate.display()
        );
        std::process::exit(2);
    }
    candidate
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
    let mut root = std::env::temp_dir().join(format!("temporalstore-secondary-raft-{}", now_ms()));
    let mut shard_id = 1u64;
    let mut auth_token = "local-raft-token".to_string();
    let mut heartbeat_ms = 50u64;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let Some(raw) = args.next() else {
            usage_and_exit();
        };
        match flag.as_str() {
            "--root" => root = PathBuf::from(raw),
            "--shard-id" => shard_id = parse(&raw, &flag),
            "--auth-token" => auth_token = raw,
            "--heartbeat-ms" => heartbeat_ms = parse(&raw, &flag),
            _ => usage_and_exit(),
        }
    }
    HarnessOptions {
        root,
        shard_id,
        auth_token,
        heartbeat_ms,
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
        "usage: raft_secondary_replication_harness [--root <path>] [--shard-id <id>] [--auth-token <token>] [--heartbeat-ms <ms>]"
    );
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
