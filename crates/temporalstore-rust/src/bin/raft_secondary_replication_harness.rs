use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::http::{get_json_with_options, post_json_with_options, HttpRequestOptions};
use temporalstore_rust::meta::ShardSnapshotRef;
use temporalstore_rust::raft::{
    RaftReplicaBootstrapPlan, RaftRpcMetadata, RaftSnapshotPublishReport,
};
use temporalstore_rust::{
    Command, CommandResponse, DistributedRaftCommandResponse, DistributedRaftProposeRequest,
    DistributedRaftReadRequest, OpenRaftDataNodeProcessRolloutReport, OpenRaftProcessNodeEvidence,
    ProductionRaftNode, RaftApplyHealth, RaftClusterStatus, RaftFailoverReport,
    RaftMembershipChangeReport, RaftNodeId, Status, VoteRequest, VoteResponse,
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
    membership_scale_down: Vec<MembershipApplySummary>,
    membership_scale_up: Vec<MembershipApplySummary>,
    restarted_secondary: RaftNodeId,
    partition: PartitionSummary,
    lagging_follower: LaggingFollowerSummary,
    network_vote: NetworkVoteSummary,
    rolling_restart: RollingRestartSummary,
    crashed_leader: RaftNodeId,
    failover: AdminFailoverResponse,
    reads_after_leader_crash: Vec<ReadSummary>,
    openraft_process_rollout: OpenRaftDataNodeProcessRolloutReport,
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
struct LaggingFollowerSummary {
    follower_node: RaftNodeId,
    majority_writes: Vec<WriteSummary>,
    observed_lag: u64,
    catchup_reads: Vec<ReadSummary>,
}

#[derive(Debug, Serialize)]
struct NetworkVoteSummary {
    unauthorized_rejection: String,
    stale_candidate: RaftNodeId,
    stale_target: RaftNodeId,
    stale_response: VoteResponse,
    valid_candidate: RaftNodeId,
    valid_target: RaftNodeId,
    valid_response: VoteResponse,
}

#[derive(Debug, Serialize)]
struct RollingRestartSummary {
    restarted_nodes: Vec<RaftNodeId>,
    writes_after_each_restart: Vec<WriteSummary>,
    reads_after_each_restart: Vec<ReadSummary>,
}

#[derive(Debug, Serialize)]
struct MembershipApplySummary {
    node_id: RaftNodeId,
    status: Status,
    voters: Vec<RaftNodeId>,
    leader_id: RaftNodeId,
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

#[derive(Debug, Serialize)]
struct AdminCatchUpRequest {
    node_id: RaftNodeId,
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

#[derive(Debug, Serialize)]
struct RaftApplyHealthRequest {
    #[serde(default)]
    max_allowed_apply_lag: u64,
}

#[derive(Debug, Serialize)]
struct RaftMembershipApplyRequest {
    voters: Vec<RaftNodeId>,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct RaftMembershipApplyResponse {
    status: Status,
    report: Option<RaftMembershipChangeReport>,
}

#[derive(Debug, Serialize)]
struct PublishExternalSnapshotRequest {
    object_root: String,
    local_root: String,
    cluster_id: String,
    bucket: String,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct PublishExternalSnapshotResponse {
    status: Status,
    report: Option<RaftSnapshotPublishReport>,
}

#[derive(Debug, Serialize)]
struct BootstrapExternalSnapshotRequest {
    target_id: RaftNodeId,
    snapshot: ShardSnapshotRef,
    object_root: String,
    local_root: String,
    cluster_id: String,
    bucket: String,
}

#[derive(Debug, serde::Deserialize, Serialize)]
struct BootstrapExternalSnapshotResponse {
    status: Status,
    plan: Option<RaftReplicaBootstrapPlan>,
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
    wait_for_value(&nodes[0], 1, "secondary-before-stop", "v2");
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

    elect_leader(&nodes[0], nodes[0].node_id);
    catch_up_peer(&nodes[0], restarted_secondary);
    local_catch_up(restarted_node, restarted_secondary);
    let _restart_catchup_trigger =
        propose(&nodes[0], "secondary-restart-catchup-trigger", "v-catchup");
    wait_for_value(
        restarted_node,
        restarted_secondary,
        "secondary-while-down",
        "v3",
    );
    writes.push(propose(&nodes[0], "secondary-after-restart", "v4"));
    elect_leader(&nodes[0], nodes[0].node_id);
    catch_up_peer(&nodes[0], restarted_secondary);
    local_catch_up(restarted_node, restarted_secondary);

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

    let membership_scale_down = apply_membership_to_nodes(&nodes, &[1, 2]);
    initialize_liveness(&nodes[..2]);
    writes.push(propose(&nodes[0], "membership-scale-down", "v-scale-down"));
    wait_for_value(&nodes[0], 1, "membership-scale-down", "v-scale-down");
    wait_for_value(&nodes[1], 2, "membership-scale-down", "v-scale-down");

    let membership_scale_up = apply_membership_to_nodes(&nodes, &[1, 2, 3]);
    initialize_liveness(&nodes);
    bootstrap_external_snapshot(&nodes[0], &nodes[2], &options);
    wait_for_value(&nodes[2], 3, "secondary-before-restart", "v1");
    wait_for_value(&nodes[2], 3, "secondary-before-stop", "v2");
    wait_for_value(&nodes[2], 3, "membership-scale-down", "v-scale-down");
    writes.push(propose(&nodes[0], "membership-scale-up", "v-scale-up"));
    catch_up_peer(&nodes[0], 3);
    local_catch_up(&nodes[2], 3);
    for node in &nodes {
        wait_for_value(node, node.node_id, "membership-scale-up", "v-scale-up");
    }

    let rolling_restart = run_rolling_restart_phase(
        &mut children,
        &raft_node_bin,
        &options,
        &nodes_env,
        &nodes,
        1,
    );
    writes.extend(rolling_restart.writes_after_each_restart.iter().cloned());

    let partition = run_partition_phase(&nodes);
    writes.push(partition.majority_write.clone());
    let lagging_follower = run_lagging_follower_phase(&nodes);
    writes.extend(lagging_follower.majority_writes.iter().cloned());
    let network_vote = run_network_vote_phase(&nodes, &options.auth_token, options.shard_id);

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
    let survivors = nodes
        .iter()
        .filter(|node| node.node_id != crashed_leader)
        .cloned()
        .collect::<Vec<_>>();
    wait_for_surviving_apply_health(new_leader, &survivors);
    writes.push(propose(new_leader, "after-leader-crash", "v5"));

    for node in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        wait_for_cluster_commit(node, writes.len() as u64);
    }
    wait_for_surviving_apply_health(new_leader, &survivors);

    let mut reads_after_leader_crash = Vec::new();
    for node in nodes.iter().filter(|node| node.node_id != crashed_leader) {
        reads_after_leader_crash.push(wait_for_value(
            node,
            node.node_id,
            "after-leader-crash",
            "v5",
        ));
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
    validate_surviving_cluster(
        &node_summaries,
        crashed_leader,
        new_leader.node_id,
        writes.len() as u64,
    );

    let openraft_process_rollout =
        data_node_process_rollout_report(&options, &node_summaries, &rolling_restart);

    println!(
        "{}",
        serde_json::to_string_pretty(&SecondaryReplicationSummary {
            root: options.root.display().to_string(),
            shard_id: options.shard_id,
            nodes: node_summaries,
            writes,
            reads_after_restart,
            membership_scale_down,
            membership_scale_up,
            restarted_secondary,
            partition,
            lagging_follower,
            network_vote,
            rolling_restart,
            crashed_leader,
            failover,
            reads_after_leader_crash,
            openraft_process_rollout,
        })
        .expect("summary should serialize")
    );
}

fn data_node_process_rollout_report(
    options: &HarnessOptions,
    nodes: &[NodeSummary],
    rolling_restart: &RollingRestartSummary,
) -> OpenRaftDataNodeProcessRolloutReport {
    let node_evidence = nodes
        .iter()
        .map(|node| OpenRaftProcessNodeEvidence {
            node_id: node.node_id,
            addr: node.addr.clone(),
            wal_dir: node.wal_dir.clone(),
            commit_index: node.status.commit_index,
            applied_index: node
                .status
                .nodes
                .iter()
                .find(|status| status.node_id == node.node_id)
                .map(|status| status.applied_index)
                .unwrap_or_default(),
            snapshot_id: None,
            restarted: rolling_restart.restarted_nodes.contains(&node.node_id),
            log_store_validated: !node.wal_files.is_empty() && node.apply_health.healthy,
        })
        .collect::<Vec<_>>();
    let mut blockers = Vec::new();
    if node_evidence.len() < 2 {
        blockers.push("not_enough_surviving_processes".to_string());
    }
    if node_evidence
        .iter()
        .any(|node| !node.log_store_validated || node.commit_index == 0)
    {
        blockers.push("log_store_validation_missing".to_string());
    }
    if rolling_restart.restarted_nodes.is_empty() {
        blockers.push("restart_recovery_missing".to_string());
    }
    let write_proposed_through_process_api = true;
    let recovered_after_restart = rolling_restart.restarted_nodes.len() >= 3;
    let snapshot_install_validated = true;
    let applied_fence_validated = node_evidence
        .iter()
        .all(|node| node.applied_index <= node.commit_index && node.applied_index > 0);
    let multi_process_log_store_validated = blockers.is_empty() && applied_fence_validated;
    OpenRaftDataNodeProcessRolloutReport {
        shard_id: options.shard_id,
        nodes: node_evidence,
        write_proposed_through_process_api,
        recovered_after_restart,
        snapshot_install_validated,
        applied_fence_validated,
        multi_process_log_store_validated,
        ready: write_proposed_through_process_api
            && recovered_after_restart
            && snapshot_install_validated
            && applied_fence_validated
            && multi_process_log_store_validated,
        blockers,
    }
}

fn run_partition_phase(nodes: &[ProductionRaftNode]) -> PartitionSummary {
    let isolated_node = 3;
    let leader = nodes
        .iter()
        .find(|node| node.node_id == 1)
        .expect("leader should exist");
    initialize_liveness(nodes);
    for node in nodes {
        elect_leader(node, leader.node_id);
    }
    for follower in nodes
        .iter()
        .filter(|node| node.node_id != leader.node_id && node.node_id != isolated_node)
    {
        catch_up_peer(leader, follower.node_id);
        local_catch_up(follower, follower.node_id);
    }

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
    for node in nodes {
        elect_leader(node, leader.node_id);
    }
    catch_up_peer(leader, isolated_node);
    local_catch_up(isolated, isolated_node);
    let healed_read = wait_for_value(isolated, isolated_node, "partition-majority", "v-partition");
    PartitionSummary {
        isolated_node,
        majority_write,
        isolated_read_status: isolated_read.status,
        healed_read,
    }
}

fn run_lagging_follower_phase(nodes: &[ProductionRaftNode]) -> LaggingFollowerSummary {
    let follower_node = 3;
    let leader = nodes
        .iter()
        .find(|node| node.node_id == 1)
        .expect("leader should exist");
    let follower = nodes
        .iter()
        .find(|node| node.node_id == follower_node)
        .expect("lagging follower should exist");

    for peer in nodes.iter().filter(|node| node.node_id != follower_node) {
        block_peer(follower, peer.node_id, true);
    }

    let majority_writes = (0..3)
        .map(|index| {
            propose(
                leader,
                &format!("lagging-follower-{index}"),
                &format!("v-lag-{index}"),
            )
        })
        .collect::<Vec<_>>();
    for index in 0..3 {
        for majority in nodes.iter().filter(|node| node.node_id != follower_node) {
            wait_for_value(
                majority,
                majority.node_id,
                &format!("lagging-follower-{index}"),
                &format!("v-lag-{index}"),
            );
        }
    }
    let observed_lag = wait_for_lag(leader, follower_node, 3);

    for peer in nodes.iter().filter(|node| node.node_id != follower_node) {
        block_peer(follower, peer.node_id, false);
    }
    initialize_liveness(nodes);
    for node in nodes {
        elect_leader(node, leader.node_id);
    }
    catch_up_peer(leader, follower_node);
    local_catch_up(follower, follower_node);
    let catchup_reads = (0..3)
        .map(|index| {
            wait_for_value(
                follower,
                follower_node,
                &format!("lagging-follower-{index}"),
                &format!("v-lag-{index}"),
            )
        })
        .collect::<Vec<_>>();

    LaggingFollowerSummary {
        follower_node,
        majority_writes,
        observed_lag,
        catchup_reads,
    }
}

fn run_network_vote_phase(
    nodes: &[ProductionRaftNode],
    auth_token: &str,
    shard_id: u64,
) -> NetworkVoteSummary {
    initialize_liveness(nodes);
    let leader = node_by_id(nodes, 1);
    for node in nodes {
        elect_leader(node, leader.node_id);
        if node.node_id != leader.node_id {
            catch_up_peer(leader, node.node_id);
            local_catch_up(node, node.node_id);
        }
    }

    let target = node_by_id(nodes, 2);
    let target_status: RaftClusterStatus =
        get_json_with_options(&target.addr, "/raft/status", request_options())
            .expect("target status request failed before vote phase");
    let target_node_status = target_status
        .nodes
        .iter()
        .find(|node| node.node_id == target.node_id)
        .expect("target status should include local node");

    let stale_candidate = node_by_id(nodes, 3);
    let unauthorized = post_json_with_options::<_, VoteResponse>(
        &target.addr,
        "/raft/request_vote",
        &VoteRequest {
            rpc: Some(raft_rpc("wrong-token", "unauthorized-vote")),
            shard_id,
            term: target_node_status.current_term + 1,
            candidate_id: stale_candidate.node_id,
            target_id: target.node_id,
            last_log_index: target_status.commit_index,
            last_log_term: target_node_status.current_term,
        },
        request_options(),
    )
    .expect_err("unauthorized peer vote should be rejected before vote handling")
    .to_string();
    assert!(
        unauthorized.contains("403") || unauthorized.contains("auth"),
        "unexpected unauthorized vote error: {unauthorized}"
    );

    let stale_response: VoteResponse = post_json_with_options(
        &target.addr,
        "/raft/request_vote",
        &VoteRequest {
            rpc: Some(raft_rpc(auth_token, "stale-vote")),
            shard_id,
            term: target_node_status.current_term + 1,
            candidate_id: stale_candidate.node_id,
            target_id: target.node_id,
            last_log_index: 0,
            last_log_term: 0,
        },
        request_options(),
    )
    .expect("stale vote request failed");
    assert!(
        !stale_response.vote_granted,
        "stale candidate unexpectedly received vote: {:?}",
        stale_response
    );
    assert_eq!(
        stale_response.reject_reason.as_deref(),
        Some("candidate_log_behind")
    );

    let refreshed_target_status: RaftClusterStatus =
        get_json_with_options(&target.addr, "/raft/status", request_options())
            .expect("target status request failed after stale vote");
    let refreshed_target = refreshed_target_status
        .nodes
        .iter()
        .find(|node| node.node_id == target.node_id)
        .expect("target status should include local node");
    let valid_candidate = leader;
    let valid_response: VoteResponse = post_json_with_options(
        &target.addr,
        "/raft/request_vote",
        &VoteRequest {
            rpc: Some(raft_rpc(auth_token, "valid-vote")),
            shard_id,
            term: refreshed_target.current_term + 1,
            candidate_id: valid_candidate.node_id,
            target_id: target.node_id,
            last_log_index: refreshed_target_status.commit_index,
            last_log_term: refreshed_target.current_term + 1,
        },
        request_options(),
    )
    .expect("valid vote request failed");
    assert!(
        valid_response.vote_granted,
        "caught-up candidate did not receive vote: {:?}",
        valid_response
    );

    NetworkVoteSummary {
        unauthorized_rejection: unauthorized,
        stale_candidate: stale_candidate.node_id,
        stale_target: target.node_id,
        stale_response,
        valid_candidate: valid_candidate.node_id,
        valid_target: target.node_id,
        valid_response,
    }
}

fn raft_rpc(auth_token: &str, request_id: &str) -> RaftRpcMetadata {
    RaftRpcMetadata {
        auth_token: Some(auth_token.to_string()),
        deadline_ms: Some(30_000),
        request_id: request_id.to_string(),
    }
}

fn run_rolling_restart_phase(
    children: &mut Vec<ChildNode>,
    raft_node_bin: &Path,
    options: &HarnessOptions,
    nodes_env: &str,
    nodes: &[ProductionRaftNode],
    leader_node_id: RaftNodeId,
) -> RollingRestartSummary {
    let restart_order = vec![3, 2, 1];
    let original_leader = node_by_id(nodes, leader_node_id);
    let mut active_leader_id = leader_node_id;
    let mut writes_after_each_restart = Vec::new();
    let mut reads_after_each_restart = Vec::new();

    for restarted_node_id in restart_order.iter().copied() {
        let restarting_active_leader = restarted_node_id == active_leader_id;
        let child_index = children
            .iter()
            .position(|child| child.node.node_id == restarted_node_id)
            .expect("rolling-restart child should exist");
        let mut stopped = children.remove(child_index);
        stopped
            .child
            .kill()
            .expect("failed to kill rolling-restart node");
        let _ = stopped.child.wait();
        drop(stopped);

        for alive_node in nodes
            .iter()
            .filter(|node| node.node_id != restarted_node_id)
        {
            mark_liveness(alive_node, restarted_node_id, false);
        }
        if restarting_active_leader {
            let promoted_leader_id = nodes
                .iter()
                .find(|node| node.node_id != restarted_node_id)
                .expect("rolling restart should have a surviving leader candidate")
                .node_id;
            active_leader_id = promoted_leader_id;
            for survivor in nodes
                .iter()
                .filter(|node| node.node_id != restarted_node_id)
            {
                elect_leader(survivor, promoted_leader_id);
            }
            let failover = trigger_failover(node_by_id(nodes, promoted_leader_id));
            assert!(
                failover.status.ok,
                "rolling restart failover to node {} failed: {:?}",
                promoted_leader_id, failover.status
            );
        }

        let restarted = node_by_id(nodes, restarted_node_id);
        children.push(spawn_node(raft_node_bin, options, nodes_env, restarted));
        wait_for_http(&restarted.addr);
        initialize_liveness(nodes);
        let active_leader = node_by_id(nodes, active_leader_id);
        for node in nodes {
            elect_leader(node, active_leader_id);
        }
        if restarted_node_id != active_leader_id {
            elect_leader(active_leader, active_leader_id);
            catch_up_peer(active_leader, restarted_node_id);
            local_catch_up(restarted, restarted_node_id);
        }

        let key = format!("rolling-restart-{restarted_node_id}");
        let value = format!("v-restart-{restarted_node_id}");
        let write = propose(active_leader, &key, &value);
        for follower in nodes.iter().filter(|node| node.node_id != active_leader_id) {
            elect_leader(active_leader, active_leader_id);
            catch_up_peer(active_leader, follower.node_id);
            local_catch_up(follower, follower.node_id);
        }
        for node in nodes {
            reads_after_each_restart.push(wait_for_value(node, node.node_id, &key, &value));
        }
        writes_after_each_restart.push(write);

        if restarting_active_leader && active_leader_id != leader_node_id {
            catch_up_peer(active_leader, leader_node_id);
            local_catch_up(original_leader, leader_node_id);
            for node in nodes {
                elect_leader(node, leader_node_id);
            }
            active_leader_id = leader_node_id;
        }
    }

    RollingRestartSummary {
        restarted_nodes: restart_order,
        writes_after_each_restart,
        reads_after_each_restart,
    }
}

fn node_by_id(nodes: &[ProductionRaftNode], node_id: RaftNodeId) -> &ProductionRaftNode {
    nodes
        .iter()
        .find(|node| node.node_id == node_id)
        .unwrap_or_else(|| panic!("node {node_id} should exist"))
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
    fs::create_dir_all(&options.root).expect("failed to create raft harness root");
    let stdout = fs::File::create(
        options
            .root
            .join(format!("raft-node-{}-stdout.log", node.node_id)),
    )
    .expect("failed to create raft_node stdout log");
    let stderr = fs::File::create(
        options
            .root
            .join(format!("raft-node-{}-stderr.log", node.node_id)),
    )
    .expect("failed to create raft_node stderr log");
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
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .expect("failed to spawn raft_node");
    ChildNode {
        node: node.clone(),
        child,
    }
}

fn propose(node: &ProductionRaftNode, key: &str, value: &str) -> WriteSummary {
    let deadline = Instant::now() + Duration::from_secs(60);
    let response: DistributedRaftCommandResponse = loop {
        let response: DistributedRaftCommandResponse = match post_json_with_options(
            &node.addr,
            "/raft/propose",
            &DistributedRaftProposeRequest {
                command: Command::StringSet {
                    key: key.to_string(),
                    value: value.as_bytes().to_vec(),
                },
            },
            request_options(),
        ) {
            Ok(response) => response,
            Err(err) => {
                assert!(
                    Instant::now() < deadline,
                    "proposal request for key {key} on node {} failed: {:?}",
                    node.node_id,
                    err
                );
                thread::sleep(Duration::from_millis(50));
                continue;
            }
        };
        if response.status.ok || !is_transient_proposal_error(&response.status) {
            break response;
        }
        assert!(
            Instant::now() < deadline,
            "proposal for key {key} on node {} stayed transiently unavailable: {:?}",
            node.node_id,
            response.status
        );
        thread::sleep(Duration::from_millis(50));
    };
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

fn is_transient_proposal_error(status: &Status) -> bool {
    status.code == "raft_error"
        && (status.message.contains("not enough live replicas")
            || status.message.contains("behind leader commit index"))
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

fn catch_up_peer(leader: &ProductionRaftNode, node_id: RaftNodeId) {
    let mut last_status = None;
    let deadline = Instant::now() + Duration::from_secs(60);
    for _ in 0..8 {
        let response: AdminLivenessResponse = loop {
            match post_json_with_options(
                &leader.addr,
                "/raft/admin/catch_up",
                &AdminCatchUpRequest { node_id },
                request_options(),
            ) {
                Ok(response) => break response,
                Err(err) => {
                    assert!(
                        Instant::now() < deadline,
                        "catch-up admin request to node {} for peer {} failed: {:?}",
                        leader.node_id,
                        node_id,
                        err
                    );
                    thread::sleep(Duration::from_millis(50));
                }
            }
        };
        if response.status.ok {
            return;
        }
        let stale_term = response.status.message.contains("stale_term");
        let transient_transport = response.status.message.contains("raft transport error")
            || response
                .status
                .message
                .contains("Resource temporarily unavailable")
            || response.status.message.contains("timed out")
            || response.status.message.contains("connection refused");
        last_status = Some(response.status);
        if !(stale_term || transient_transport) {
            break;
        }
        elect_leader(leader, leader.node_id);
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "catch-up admin request failed: {:?}",
        last_status.expect("catch-up should have produced a status")
    );
}

fn local_catch_up(node: &ProductionRaftNode, node_id: RaftNodeId) {
    let response: AdminLivenessResponse = post_json_with_options(
        &node.addr,
        "/raft/admin/local_catch_up",
        &AdminCatchUpRequest { node_id },
        request_options(),
    )
    .expect("local catch-up admin request failed");
    assert!(
        response.status.ok,
        "local catch-up admin request failed: {:?}",
        response.status
    );
}

fn apply_membership_to_nodes(
    nodes: &[ProductionRaftNode],
    voters: &[RaftNodeId],
) -> Vec<MembershipApplySummary> {
    nodes
        .iter()
        .map(|node| {
            let response: RaftMembershipApplyResponse = post_json_with_options(
                &node.addr,
                "/raft/membership/apply",
                &RaftMembershipApplyRequest {
                    voters: voters.to_vec(),
                },
                request_options(),
            )
            .expect("membership apply request failed");
            assert!(
                response.status.ok,
                "membership apply on node {} failed: {:?}",
                node.node_id, response.status
            );
            let report = response
                .report
                .expect("successful membership apply should include report");
            assert_eq!(
                report.committed_membership.voters, voters,
                "membership apply on node {} committed unexpected voters",
                node.node_id
            );
            MembershipApplySummary {
                node_id: node.node_id,
                status: response.status,
                voters: report.committed_membership.voters,
                leader_id: report.leader_id,
            }
        })
        .collect()
}

fn wait_for_surviving_apply_health(leader: &ProductionRaftNode, survivors: &[ProductionRaftNode]) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        for node in survivors {
            if node.node_id != leader.node_id {
                catch_up_peer(leader, node.node_id);
            }
        }
        for observer in survivors {
            for subject in survivors {
                local_catch_up(observer, subject.node_id);
            }
        }
        let health = survivors
            .iter()
            .map(|node| {
                post_json_with_options::<_, RaftApplyHealth>(
                    &node.addr,
                    "/raft/apply_health",
                    &RaftApplyHealthRequest {
                        max_allowed_apply_lag: 0,
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
            "surviving raft apply health did not converge: {:?}",
            health
        );
        thread::sleep(Duration::from_millis(50));
    }
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

fn bootstrap_external_snapshot(
    leader: &ProductionRaftNode,
    target: &ProductionRaftNode,
    options: &HarnessOptions,
) {
    let object_root = options.root.join("external-snapshot-objects");
    let publish_local_root = options.root.join("external-snapshot-publish-local");
    let restore_local_root = options
        .root
        .join(format!("external-snapshot-restore-node-{}", target.node_id));
    let cluster_id = "raft-secondary-harness".to_string();
    let bucket = "raft".to_string();

    let published: PublishExternalSnapshotResponse = post_json_with_options(
        &leader.addr,
        "/raft/admin/publish_external_snapshot",
        &PublishExternalSnapshotRequest {
            object_root: object_root.display().to_string(),
            local_root: publish_local_root.display().to_string(),
            cluster_id: cluster_id.clone(),
            bucket: bucket.clone(),
        },
        request_options(),
    )
    .expect("publish external snapshot admin request failed");
    assert!(
        published.status.ok,
        "publish external snapshot failed: {:?}",
        published.status
    );
    let snapshot = published
        .report
        .expect("published snapshot response should include report")
        .meta_ref;

    let bootstrapped: BootstrapExternalSnapshotResponse = post_json_with_options(
        &target.addr,
        "/raft/admin/bootstrap_external_snapshot",
        &BootstrapExternalSnapshotRequest {
            target_id: target.node_id,
            snapshot,
            object_root: object_root.display().to_string(),
            local_root: restore_local_root.display().to_string(),
            cluster_id,
            bucket,
        },
        request_options(),
    )
    .expect("bootstrap external snapshot admin request failed");
    assert!(
        bootstrapped.status.ok,
        "bootstrap external snapshot failed: {:?}",
        bootstrapped.status
    );
    assert!(
        bootstrapped.plan.is_some(),
        "bootstrap external snapshot response should include plan"
    );
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

fn wait_for_lag(node: &ProductionRaftNode, lagging_node_id: RaftNodeId, min_lag: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status: RaftClusterStatus =
            get_json_with_options(&node.addr, "/raft/status", request_options())
                .expect("status request failed");
        let lag = status
            .nodes
            .iter()
            .find(|replica| replica.node_id == lagging_node_id)
            .map(|replica| replica.lag)
            .unwrap_or_default();
        if lag >= min_lag {
            return lag;
        }
        assert!(
            Instant::now() < deadline,
            "node {} did not observe lag {} for follower {}; last status: {:?}",
            node.node_id,
            min_lag,
            lagging_node_id,
            status
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
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
        io_timeout_ms: 30_000,
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
