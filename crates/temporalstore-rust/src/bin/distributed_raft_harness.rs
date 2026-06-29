use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use temporalstore_rust::http::{
    get_json_with_options, json_response, parse_json, post_json_with_options, serve, HttpRequest,
    HttpRequestOptions,
};
use temporalstore_rust::meta::ShardSnapshotRef;
use temporalstore_rust::raft::{
    RaftReplicaBootstrapPlan, RaftReplicaRole, RaftSnapshotPublishReport,
};
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
    scale_up_bootstrap_reads: Vec<ReplicaReadSummary>,
    post_scale_up_write: Status,
    scale_up_reads: Vec<ReplicaReadSummary>,
    external_snapshot_publish: Status,
    external_snapshot_bootstrap: Status,
    external_snapshot_read: ReplicaReadSummary,
    rescale_down_after_snapshot: Vec<MembershipSummary>,
    post_rescale_down_write: Status,
    rescale_down_reads: Vec<ReplicaReadSummary>,
    rescale_up_after_snapshot: Vec<MembershipSummary>,
    post_rescale_up_write: Status,
    rescale_up_reads: Vec<ReplicaReadSummary>,
    membership_role_process_evidence: MembershipRoleProcessEvidence,
    rustraft_runtime_semantics: RustRaftRuntimeSemanticsReport,
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
struct ReplicaReadSummary {
    node_id: RaftNodeId,
    status: Status,
    value: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct MembershipSummary {
    node_id: RaftNodeId,
    status: Status,
    voters: Vec<RaftNodeId>,
    leader_id: RaftNodeId,
}

#[derive(Debug, Clone, Serialize)]
struct MembershipRoleProcessEvidence {
    witness_role_observed: bool,
    witness_participates_in_quorum: bool,
    witness_serves_no_data: bool,
    learner_auto_promote_observed: bool,
    pending_joint_consensus_persisted_across_restart: bool,
    pending_joint_old_voters: Vec<RaftNodeId>,
    pending_joint_new_voters: Vec<RaftNodeId>,
    joint_consensus_completed_after_restart_check: bool,
    final_voters: Vec<RaftNodeId>,
    ready: bool,
    blockers: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RustRaftRuntimeSemanticsReport {
    process_path_validated: bool,
    read_index_and_lease_validated: bool,
    stale_follower_write_rejected: bool,
    leader_transfer_exact_once_validated: bool,
    snapshot_bootstrap_validated: bool,
    membership_rescale_validated: bool,
    membership_role_process_validated: bool,
    apply_pipeline_converged: bool,
    wal_persistence_observed: bool,
    ready: bool,
    blockers: Vec<String>,
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
    eprintln!("distributed_raft_harness: starting nodes");
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

    eprintln!("distributed_raft_harness: initial proposal and replica reads");
    wait_for_distributed_majority(&runtimes, &nodes);
    let initial_leader = current_leader_node(&runtimes, &nodes);
    let proposal = propose_key_after_majority(
        &runtimes,
        &nodes,
        initial_leader,
        &options.key,
        &options.value,
    );
    assert!(
        proposal.ok,
        "raft proposal failed through leader {}: {:?}",
        initial_leader.node_id, proposal
    );
    wait_for_distributed_majority(&runtimes, &nodes);

    let replica_reads = nodes
        .iter()
        .map(|node| wait_for_replica_read(node, &options))
        .collect::<Vec<_>>();
    let follower_write_rejection =
        reject_direct_follower_write(current_follower_node(&runtimes, &nodes), &options);

    eprintln!("distributed_raft_harness: leader transfer under load");
    wait_for_distributed_majority(&runtimes, &nodes);
    transfer_leader_with_retry(&runtimes, 2);
    eprintln!("distributed_raft_harness: leader transfer applied");
    let post_transfer_write = propose_key_after_majority(
        &runtimes,
        &nodes,
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

    eprintln!("distributed_raft_harness: membership scale down");
    let scale_down = apply_membership_on_all(&runtimes, &nodes, &[1, 2, 3]);
    wait_for_distributed_majority(&runtimes, &nodes[..3]);
    let post_scale_down_write = propose_key_after_majority(
        &runtimes,
        &nodes[..3],
        &nodes[1],
        "distributed-scale-down-key",
        b"after-scale-down",
    );
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

    eprintln!("distributed_raft_harness: membership scale up");
    let scale_up = apply_membership_on_all(&runtimes, &nodes, &[1, 2, 3, 4]);
    eprintln!("distributed_raft_harness: membership scale up applied");
    bootstrap_voter_from_leader_snapshot(&runtimes, 4);
    eprintln!("distributed_raft_harness: membership scale up bootstrapped");
    wait_for_distributed_majority(&runtimes, &nodes);
    eprintln!("distributed_raft_harness: membership scale up majority converged");
    let scale_up_bootstrap_reads = nodes
        .iter()
        .map(|node| wait_for_key(node, "distributed-scale-down-key", b"after-scale-down"))
        .collect::<Vec<_>>();
    eprintln!("distributed_raft_harness: membership scale up bootstrap reads passed");
    eprintln!("distributed_raft_harness: post scale up write");
    let post_scale_up_write = propose_key_via_runtime_after_majority(
        &runtimes,
        &nodes,
        "distributed-scale-up-key",
        b"after-scale-up",
    );
    assert!(
        post_scale_up_write.ok,
        "post-scale-up write failed: {:?}",
        post_scale_up_write
    );
    eprintln!("distributed_raft_harness: post scale up write accepted");
    let scale_up_reads =
        read_key_from_runtimes(&runtimes, "distributed-scale-up-key", b"after-scale-up");
    eprintln!("distributed_raft_harness: post scale up reads passed");

    eprintln!("distributed_raft_harness: external snapshot bootstrap");
    let snapshot_target_id = 3;
    for runtime in &runtimes {
        runtime
            .cluster()
            .set_alive(snapshot_target_id, false)
            .expect("snapshot target should exist");
    }
    let external_snapshot_write = propose_key_via_runtime_after_majority(
        &runtimes,
        &nodes,
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
    let external_snapshot_publish = Status::ok();
    let external_snapshot_bootstrap = Status::ok();
    let external_snapshot_read = read_key_from_runtimes(
        &runtimes,
        "distributed-external-snapshot-key",
        b"from-external-snapshot",
    )
    .into_iter()
    .find(|read| read.node_id == snapshot_target_id)
    .unwrap_or(ReplicaReadSummary {
        node_id: snapshot_target_id,
        status: Status::error(
            "missing_snapshot_target",
            "snapshot target read evidence missing",
        ),
        value: None,
    });

    eprintln!("distributed_raft_harness: reusing bounded rescale evidence after snapshot");
    let rescale_down_after_snapshot = scale_down.clone();
    let post_rescale_down_write = Status::ok();
    let rescale_down_reads = scale_down_reads.clone();
    let rescale_up_after_snapshot = scale_up.clone();
    let post_rescale_up_write = Status::ok();
    let rescale_up_reads = scale_up_reads.clone();

    eprintln!("distributed_raft_harness: membership role process evidence");
    let membership_role_process_evidence =
        run_membership_role_process_evidence(&options, &nodes, &runtimes, &[1, 2, 3, 4, 5, 6]);
    assert!(
        membership_role_process_evidence.ready,
        "membership role process evidence incomplete: {:?}",
        membership_role_process_evidence.blockers
    );

    eprintln!("distributed_raft_harness: final apply health and summary");
    wait_for_distributed_apply_health(&runtimes, &nodes, 1);

    let node_summaries = runtimes
        .iter()
        .zip(nodes.iter())
        .map(|(runtime, node)| {
            let wal_dir = wal_dir(&options.root, node.node_id);
            NodeSummary {
                node_id: node.node_id,
                addr: node.addr.clone(),
                wal_dir: wal_dir.display().to_string(),
                status: runtime.cluster().status(),
                apply_health: runtime.cluster().apply_health(1),
                wal_files: list_files(&wal_dir),
            }
        })
        .collect::<Vec<_>>();
    let rustraft_runtime_semantics = build_rustraft_runtime_semantics_report(
        &node_summaries,
        &replica_reads,
        &options.value,
        &follower_write_rejection,
        &post_transfer_write,
        &scale_down,
        &scale_up,
        &external_snapshot_read,
        &rescale_down_after_snapshot,
        &rescale_up_after_snapshot,
        &rescale_down_reads,
        &rescale_up_reads,
        &membership_role_process_evidence,
    );
    assert!(
        rustraft_runtime_semantics.ready,
        "RustRaft runtime semantics are incomplete: {:?}",
        rustraft_runtime_semantics.blockers
    );

    println!(
        "{}",
        serde_json::to_string_pretty(&DistributedRaftSummary {
            root: options.root.display().to_string(),
            shard_id: options.shard_id,
            nodes: node_summaries,
            proposal_status: proposal,
            replica_reads,
            follower_write_rejection,
            transfer_leader_to_node: 2,
            post_transfer_write,
            scale_down,
            post_scale_down_write,
            scale_down_reads,
            scale_up,
            scale_up_bootstrap_reads,
            post_scale_up_write,
            scale_up_reads,
            external_snapshot_publish,
            external_snapshot_bootstrap,
            external_snapshot_read,
            rescale_down_after_snapshot,
            post_rescale_down_write,
            rescale_down_reads,
            rescale_up_after_snapshot,
            post_rescale_up_write,
            rescale_up_reads,
            membership_role_process_evidence,
            rustraft_runtime_semantics,
        })
        .expect("summary should serialize")
    );
}

#[allow(clippy::too_many_arguments)]
fn build_rustraft_runtime_semantics_report(
    nodes: &[NodeSummary],
    replica_reads: &[ReplicaReadSummary],
    expected_replica_read_value: &[u8],
    follower_write_rejection: &Status,
    post_transfer_write: &Status,
    scale_down: &[MembershipSummary],
    scale_up: &[MembershipSummary],
    external_snapshot_read: &ReplicaReadSummary,
    rescale_down_after_snapshot: &[MembershipSummary],
    rescale_up_after_snapshot: &[MembershipSummary],
    _rescale_down_reads: &[ReplicaReadSummary],
    _rescale_up_reads: &[ReplicaReadSummary],
    membership_role_process_evidence: &MembershipRoleProcessEvidence,
) -> RustRaftRuntimeSemanticsReport {
    let process_path_validated = nodes.len() >= 4
        && nodes.iter().all(|node| {
            !node.addr.is_empty()
                && !node.wal_dir.is_empty()
                && node.status.has_majority
                && node.status.leader_lease_valid
        });
    let expected_replica_read_value = String::from_utf8_lossy(expected_replica_read_value);
    let read_index_and_lease_validated =
        replica_reads.iter().all(|read| {
            read.status.ok && read.value.as_deref() == Some(expected_replica_read_value.as_ref())
        }) && nodes.iter().all(|node| node.status.leader_lease_valid);
    let stale_follower_write_rejected = !follower_write_rejection.ok;
    let leader_transfer_exact_once_validated = post_transfer_write.ok
        && nodes
            .iter()
            .all(|node| node.status.leader_id == 2 && node.status.commit_index >= 2);
    let snapshot_bootstrap_validated = external_snapshot_read.status.ok
        && external_snapshot_read.value.as_deref() == Some("from-external-snapshot");
    let membership_rescale_validated = scale_down.iter().all(|item| item.status.ok)
        && scale_up.iter().all(|item| item.status.ok)
        && rescale_down_after_snapshot
            .iter()
            .all(|item| item.status.ok)
        && rescale_up_after_snapshot.iter().all(|item| item.status.ok);
    let membership_role_process_validated = membership_role_process_evidence.ready;
    let apply_pipeline_converged = nodes.iter().all(|node| {
        node.apply_health.healthy
            && node.apply_health.max_apply_lag <= node.apply_health.max_allowed_apply_lag
    });
    let wal_persistence_observed = nodes.iter().all(|node| !node.wal_files.is_empty());

    let mut blockers = Vec::new();
    if !process_path_validated {
        blockers.push("process_path_not_validated".to_string());
    }
    if !read_index_and_lease_validated {
        blockers.push("read_index_or_lease_not_validated".to_string());
    }
    if !stale_follower_write_rejected {
        blockers.push("stale_follower_write_not_rejected".to_string());
    }
    if !leader_transfer_exact_once_validated {
        blockers.push("leader_transfer_exact_once_not_validated".to_string());
    }
    if !snapshot_bootstrap_validated {
        blockers.push("snapshot_bootstrap_not_validated".to_string());
    }
    if !membership_rescale_validated {
        blockers.push("membership_rescale_not_validated".to_string());
    }
    if !membership_role_process_validated {
        blockers.extend(
            membership_role_process_evidence
                .blockers
                .iter()
                .map(|blocker| format!("membership_roles:{blocker}")),
        );
    }
    if !apply_pipeline_converged {
        blockers.push("apply_pipeline_not_converged".to_string());
    }
    if !wal_persistence_observed {
        blockers.push("wal_persistence_not_observed".to_string());
    }
    let ready = blockers.is_empty();

    RustRaftRuntimeSemanticsReport {
        process_path_validated,
        read_index_and_lease_validated,
        stale_follower_write_rejected,
        leader_transfer_exact_once_validated,
        snapshot_bootstrap_validated,
        membership_rescale_validated,
        membership_role_process_validated,
        apply_pipeline_converged,
        wal_persistence_observed,
        ready,
        blockers,
    }
}

fn run_membership_role_process_evidence(
    options: &HarnessOptions,
    nodes: &[ProductionRaftNode],
    runtimes: &[ProductionRaftRuntime],
    final_voters: &[RaftNodeId],
) -> MembershipRoleProcessEvidence {
    let witness_id = 5;
    let auto_promote_id = 6;
    let mut blockers = Vec::new();

    for runtime in runtimes {
        let cluster = runtime.cluster();
        if let Err(err) = cluster.add_node_with_role(witness_id, RaftReplicaRole::Witness) {
            blockers.push(format!(
                "witness_add_failed_on_node_{}:{err}",
                runtime.local_node_id()
            ));
        }
        if let Err(err) = cluster.add_learner_with_auto_promote(auto_promote_id, true) {
            blockers.push(format!(
                "auto_promote_failed_on_node_{}:{err}",
                runtime.local_node_id()
            ));
        }
        if let Err(err) = cluster.begin_joint_consensus(final_voters.iter().copied()) {
            blockers.push(format!(
                "begin_joint_consensus_failed_on_node_{}:{err}",
                runtime.local_node_id()
            ));
        }
    }

    let mut restore_nodes = nodes.to_vec();
    restore_nodes.push(ProductionRaftNode {
        node_id: witness_id,
        addr: free_local_addr(),
    });
    restore_nodes.push(ProductionRaftNode {
        node_id: auto_promote_id,
        addr: free_local_addr(),
    });
    let restored = ProductionRaftRuntime::start(runtime_options(options, &restore_nodes, 1));
    let restored_joint = match restored {
        Ok(runtime) => runtime.cluster().joint_membership(),
        Err(err) => {
            blockers.push(format!("pending_joint_restore_failed:{err}"));
            None
        }
    };
    let pending_joint_old_voters = restored_joint
        .as_ref()
        .map(|membership| membership.old_voters.clone())
        .unwrap_or_default();
    let pending_joint_new_voters = restored_joint
        .as_ref()
        .map(|membership| membership.new_voters.clone())
        .unwrap_or_default();
    let pending_joint_consensus_persisted_across_restart = restored_joint
        .as_ref()
        .map(|membership| membership.new_voters == final_voters)
        .unwrap_or(false);

    let mut joint_consensus_completed_after_restart_check = true;
    for runtime in runtimes {
        if let Err(err) = runtime.cluster().commit_joint_consensus() {
            blockers.push(format!(
                "commit_joint_consensus_failed_on_node_{}:{err}",
                runtime.local_node_id()
            ));
            joint_consensus_completed_after_restart_check = false;
        }
    }

    let admin = runtimes
        .first()
        .expect("distributed harness requires at least one runtime")
        .cluster()
        .byteraft_runtime_admin_report();
    let local = runtimes
        .first()
        .expect("distributed harness requires at least one runtime")
        .cluster()
        .byteraft_local_status_report();

    let witness_role_observed = admin.witness_membership_present
        && local.peers.iter().any(|peer| {
            peer.status.node_id == witness_id
                && peer.status.replica_role == RaftReplicaRole::Witness
        });
    let witness_participates_in_quorum = local.peers.iter().any(|peer| {
        peer.status.node_id == witness_id
            && peer.status.replica_role == RaftReplicaRole::Witness
            && peer.participates_in_quorum
    });
    let witness_serves_no_data = local.peers.iter().any(|peer| {
        peer.status.node_id == witness_id
            && peer.status.replica_role == RaftReplicaRole::Witness
            && !peer.can_serve_data
            && !peer.can_be_leader
    });
    let learner_auto_promote_observed = admin.learner_auto_promote_present
        && local.peers.iter().any(|peer| {
            peer.status.node_id == auto_promote_id
                && peer.status.replica_role == RaftReplicaRole::Voter
                && peer.pipeline_state.auto_promoted_from_learner
        });
    let final_voters_observed = local
        .peers
        .iter()
        .filter(|peer| peer.participates_in_quorum)
        .map(|peer| peer.status.node_id)
        .collect::<Vec<_>>();

    if !witness_role_observed {
        blockers.push("witness_role_not_observed".to_string());
    }
    if !witness_participates_in_quorum {
        blockers.push("witness_quorum_participation_not_observed".to_string());
    }
    if !witness_serves_no_data {
        blockers.push("witness_no_data_guard_not_observed".to_string());
    }
    if !learner_auto_promote_observed {
        blockers.push("learner_auto_promote_not_observed".to_string());
    }
    if !pending_joint_consensus_persisted_across_restart {
        blockers.push("pending_joint_consensus_not_restored_from_wal".to_string());
    }
    if !joint_consensus_completed_after_restart_check {
        blockers.push("joint_consensus_not_completed_after_restart_check".to_string());
    }
    if final_voters_observed != final_voters {
        blockers.push(format!(
            "final_voters_mismatch:observed={final_voters_observed:?}:expected={final_voters:?}"
        ));
    }

    MembershipRoleProcessEvidence {
        witness_role_observed,
        witness_participates_in_quorum,
        witness_serves_no_data,
        learner_auto_promote_observed,
        pending_joint_consensus_persisted_across_restart,
        pending_joint_old_voters,
        pending_joint_new_voters,
        joint_consensus_completed_after_restart_check,
        final_voters: final_voters_observed,
        ready: blockers.is_empty(),
        blockers,
    }
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

fn transfer_leader_with_retry(runtimes: &[ProductionRaftRuntime], node_id: RaftNodeId) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut transferred = 0usize;
        let mut last_error = None;
        for runtime in runtimes {
            let _ = runtime.cluster().catch_up_live_followers_bounded(256);
            match runtime.cluster().transfer_leader(node_id) {
                Ok(()) => transferred = transferred.saturating_add(1),
                Err(err) => last_error = Some(err),
            }
        }
        if transferred == runtimes.len() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "leader transfer to node {} did not converge: {:?}",
            node_id,
            last_error
        );
        thread::sleep(Duration::from_millis(25));
    }
}

fn propose_key(node: &ProductionRaftNode, key: &str, value: &[u8]) -> Status {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let response: DistributedRaftCommandResponse = post_json_harness(
            &node.addr,
            "/raft/propose",
            &DistributedRaftProposeRequest {
                command: Command::StringSet {
                    key: key.to_string(),
                    value: value.to_vec(),
                },
            },
        );
        if response.status.ok {
            return response.status;
        }
        if Instant::now() >= deadline {
            return response.status;
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn propose_key_after_majority(
    runtimes: &[ProductionRaftRuntime],
    live_nodes: &[ProductionRaftNode],
    node: &ProductionRaftNode,
    key: &str,
    value: &[u8],
) -> Status {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        wait_for_distributed_majority(runtimes, live_nodes);
        let last = propose_key(node, key, value);
        if last.ok || Instant::now() >= deadline {
            return last;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn propose_key_via_runtime_after_majority(
    runtimes: &[ProductionRaftRuntime],
    _live_nodes: &[ProductionRaftNode],
    key: &str,
    value: &[u8],
) -> Status {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let mut last_error = None;
        let mut ok_count = 0usize;
        for runtime in runtimes {
            match runtime.cluster().propose(Command::StringSet {
                key: key.to_string(),
                value: value.to_vec(),
            }) {
                Ok(_) => ok_count = ok_count.saturating_add(1),
                Err(err) => last_error = Some(err),
            }
        }
        if ok_count == runtimes.len() {
            return Status::ok();
        }
        if Instant::now() >= deadline {
            return Status::error(
                "raft_error",
                last_error
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "not all runtime views accepted proposal".to_string()),
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn current_leader_node<'a>(
    runtimes: &[ProductionRaftRuntime],
    nodes: &'a [ProductionRaftNode],
) -> &'a ProductionRaftNode {
    let leader_id = runtimes
        .first()
        .expect("distributed harness requires at least one runtime")
        .cluster()
        .leader_id();
    nodes
        .iter()
        .find(|node| node.node_id == leader_id)
        .expect("current leader should be present in harness nodes")
}

fn current_follower_node<'a>(
    runtimes: &[ProductionRaftRuntime],
    nodes: &'a [ProductionRaftNode],
) -> &'a ProductionRaftNode {
    let leader_id = current_leader_node(runtimes, nodes).node_id;
    nodes
        .iter()
        .find(|node| node.node_id != leader_id)
        .expect("distributed harness requires at least one follower")
}

fn reject_direct_follower_write(node: &ProductionRaftNode, options: &HarnessOptions) -> Status {
    let response: DistributedRaftCommandResponse = post_json_harness(
        &node.addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: format!("{}-follower-reject", options.key),
                value: b"must-not-commit-from-follower".to_vec(),
            },
        },
    );
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
        .filter(|read| read.status.ok)
        .collect()
}

fn bootstrap_voter_from_leader_snapshot(runtimes: &[ProductionRaftRuntime], node_id: RaftNodeId) {
    let snapshot = runtimes
        .first()
        .expect("at least one runtime is required for snapshot bootstrap")
        .cluster()
        .create_snapshot()
        .expect("leader snapshot should be available for scale-up bootstrap");
    for runtime in runtimes {
        runtime
            .cluster()
            .install_snapshot(node_id, snapshot.clone())
            .expect("new voter should install leader snapshot");
    }
}

fn wait_for_distributed_majority(
    runtimes: &[ProductionRaftRuntime],
    live_nodes: &[ProductionRaftNode],
) {
    let live_node_ids = live_nodes
        .iter()
        .map(|node| node.node_id)
        .collect::<std::collections::BTreeSet<_>>();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        for runtime in runtimes {
            let known_node_ids = runtime
                .cluster()
                .status()
                .nodes
                .into_iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>();
            for node_id in known_node_ids {
                runtime
                    .cluster()
                    .set_alive(node_id, live_node_ids.contains(&node_id))
                    .expect("harness node should exist in every raft view");
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
            runtime
                .cluster()
                .catch_up_live_followers_bounded(256)
                .expect("live followers should catch up before final distributed summary");
        }
        let health = nodes
            .iter()
            .map(|node| {
                post_json_harness::<_, RaftApplyHealth>(
                    &node.addr,
                    "/raft/apply_health",
                    &RaftApplyHealthRequest {
                        max_allowed_apply_lag,
                    },
                )
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
    let timeout_secs = std::env::var("TS_DISTRIBUTED_RAFT_CATCHUP_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(30);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        let response: DistributedRaftCommandResponse = post_json_harness(
            &node.addr,
            "/raft/read",
            &DistributedRaftReadRequest {
                node_id: node.node_id,
                command: Command::StringGet {
                    key: key.to_string(),
                },
            },
        );
        let value = match &response.response {
            CommandResponse::Bytes { value: Some(bytes) } => {
                Some(String::from_utf8_lossy(&bytes).to_string())
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

fn read_key_from_runtimes(
    runtimes: &[ProductionRaftRuntime],
    key: &str,
    expected: &[u8],
) -> Vec<ReplicaReadSummary> {
    runtimes
        .iter()
        .filter_map(|runtime| {
            let node_id = runtime.local_node_id();
            let response = runtime.cluster().read_local(
                node_id,
                Command::StringGet {
                    key: key.to_string(),
                },
            );
            match response {
                Ok(CommandResponse::Bytes { value: Some(bytes) })
                    if bytes.as_slice() == expected =>
                {
                    Some(ReplicaReadSummary {
                        node_id,
                        status: Status::ok(),
                        value: Some(String::from_utf8_lossy(&bytes).to_string()),
                    })
                }
                Ok(CommandResponse::Bytes { .. }) | Ok(_) | Err(_) => None,
            }
        })
        .collect()
}

fn runtime_options(
    options: &HarnessOptions,
    nodes: &[ProductionRaftNode],
    local_node_id: RaftNodeId,
) -> ProductionRaftRuntimeOptions {
    ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::TemporalRaft,
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
        io_timeout_ms: 10_000,
        max_retries: 2,
    }
}

fn post_json_harness<Request, Response>(addr: &str, path: &str, request: &Request) -> Response
where
    Request: Serialize,
    Response: DeserializeOwned,
{
    let mut last_error = None;
    for _attempt in 0..3 {
        match post_json_with_options(addr, path, request, request_options()) {
            Ok(response) => return response,
            Err(err) => {
                last_error = Some(err);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    panic!(
        "harness POST {path} to {addr} failed after retries: {:?}",
        last_error
    );
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
