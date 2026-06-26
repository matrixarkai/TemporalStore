use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::meta::{MetaEntityState, ServerMetaInfo, TableTopologyResponse};
use crate::types::ShardId;

use super::{
    JointConsensusMembership, RaftCluster, RaftError, RaftMembership, RaftNodeId, RaftReplicaLag,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftCatchUpReport {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_entries_per_follower: u64,
    pub replayed_log_entries: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub lagging_voters: Vec<RaftReplicaLag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftScaleChangeReport {
    pub leader_id: RaftNodeId,
    pub voters: Vec<RaftNodeId>,
    pub live_voters: usize,
    pub majority: usize,
    pub caught_up_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftMembershipChangeKind {
    AddVoter,
    RemoveVoter,
    ReplaceVoter,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangePlan {
    pub shard_id: ShardId,
    pub kind: RaftMembershipChangeKind,
    pub old_voters: Vec<RaftNodeId>,
    pub new_voters: Vec<RaftNodeId>,
    pub add_voters: Vec<RaftNodeId>,
    pub remove_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangeReport {
    pub plan: RaftMembershipChangePlan,
    pub joint_membership: JointConsensusMembership,
    pub committed_membership: RaftMembership,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub leader_id: RaftNodeId,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaDataRaftMembershipWorkflowReport {
    pub shard_id: ShardId,
    pub learner_id: RaftNodeId,
    pub removed_voter_id: Option<RaftNodeId>,
    pub requested_leader_id: Option<RaftNodeId>,
    #[serde(default)]
    pub initial_voters: Vec<RaftNodeId>,
    pub learner_added: bool,
    pub catch_up_verified: bool,
    #[serde(default)]
    pub learner_catch_up_index: u64,
    #[serde(default)]
    pub required_catch_up_index: u64,
    pub promoted_to_voter: bool,
    pub membership_committed: bool,
    #[serde(default)]
    pub voters_after_promote: Vec<RaftNodeId>,
    pub leader_transferred: bool,
    pub voter_removed: bool,
    pub final_leader_id: RaftNodeId,
    pub final_voters: Vec<RaftNodeId>,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenRaftProcessNodeEvidence {
    pub node_id: RaftNodeId,
    pub addr: String,
    pub wal_dir: String,
    #[serde(default)]
    pub snapshot_dir: String,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_id: Option<String>,
    pub restarted: bool,
    pub log_store_validated: bool,
    #[serde(default)]
    pub wal_segments_inspected: u64,
    #[serde(default)]
    pub wal_retained_segment_count: u64,
    #[serde(default)]
    pub wal_first_sequence: u64,
    #[serde(default)]
    pub wal_last_sequence: u64,
    #[serde(default)]
    pub wal_release_floor: u64,
    #[serde(default)]
    pub wal_slow_fsync_backpressure_observed: bool,
    #[serde(default)]
    pub restart_log_store_comparison_observed: bool,
    #[serde(default)]
    pub storage_mutation_recovered_after_restart: bool,
    #[serde(default)]
    pub wal_persisted_apply_fence_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_observed: bool,
    #[serde(default)]
    pub deterministic_crash_recovery_observed: bool,
    #[serde(default)]
    pub snapshot_files_inspected: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftProcessPathSemanticsEvidence {
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub read_index_and_lease_evidence_observed: bool,
    #[serde(default)]
    pub stale_leader_lease_rejected: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub bounded_stale_reads_observed: bool,
    #[serde(default)]
    pub bounded_stale_partition_reads_observed: bool,
    #[serde(default)]
    pub follower_lease_expiration_observed: bool,
    #[serde(default)]
    pub minority_partition_rejected: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub per_peer_pipeline_state_observed: bool,
    #[serde(default)]
    pub append_pipeline_state_observed: bool,
    #[serde(default)]
    pub replicate_inflight_limits_observed: bool,
    #[serde(default)]
    pub max_replicate_bytes_observed: bool,
    #[serde(default)]
    pub oversized_log_rejection_observed: bool,
    #[serde(default)]
    pub apply_batch_backpressure_observed: bool,
    #[serde(default)]
    pub append_queue_depth_observed: bool,
    #[serde(default)]
    pub replication_pressure_counters_observed: bool,
    #[serde(default)]
    pub max_disk_replicate_log_num_observed: bool,
    #[serde(default)]
    pub snapshot_lifecycle_observed: bool,
    #[serde(default)]
    pub snapshot_chunk_retry_backpressure_observed: bool,
    #[serde(default)]
    pub snapshot_send_timeout_observed: bool,
    #[serde(default)]
    pub snapshot_install_progress_observed: bool,
    #[serde(default)]
    pub snapshot_install_rollback_observed: bool,
    #[serde(default)]
    pub snapshot_membership_change_observed: bool,
    #[serde(default)]
    pub snapshot_rejoin_after_compacted_log_observed: bool,
    #[serde(default)]
    pub wal_segment_lifecycle_observed: bool,
    #[serde(default)]
    pub wal_segment_release_rules_observed: bool,
    #[serde(default)]
    pub wal_first_last_index_status_observed: bool,
    #[serde(default)]
    pub wal_slow_fsync_backpressure_observed: bool,
    #[serde(default)]
    pub restart_log_store_comparison_observed: bool,
    #[serde(default)]
    pub fsm_apply_atomicity_observed: bool,
    #[serde(default)]
    pub apply_fence_recovery_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_recovery_observed: bool,
    #[serde(default)]
    pub storage_wal_snapshot_crash_recovery_observed: bool,
    #[serde(default)]
    pub restart_recovery_observed: bool,
    #[serde(default)]
    pub failover_observed: bool,
    #[serde(default)]
    pub membership_change_observed: bool,
    #[serde(default)]
    pub secondary_lag_observed: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenRaftDataNodeProcessRolloutReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub voters: Vec<RaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RaftNodeId>,
    pub nodes: Vec<OpenRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: usize,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: usize,
    #[serde(default)]
    pub per_node_log_store_inspection_count: usize,
    pub write_proposed_through_process_api: bool,
    #[serde(default)]
    pub leader_transfer_validated: bool,
    #[serde(default)]
    pub leader_transfer_under_load_observed: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_observed: bool,
    #[serde(default)]
    pub leader_transfer_write_ids_observed: Vec<String>,
    #[serde(default)]
    pub leader_transfer_commit_indexes_observed: Vec<u64>,
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub secondary_lag_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejection_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejection_observed: bool,
    #[serde(default)]
    pub catchup_read_eligibility_observed: bool,
    #[serde(default)]
    pub minority_partition_rejection_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_eligibility_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub lagging_follower_observed_lag: u64,
    pub recovered_after_restart: bool,
    #[serde(default)]
    pub restart_recovery_validated: bool,
    pub snapshot_install_validated: bool,
    pub applied_fence_validated: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub byteraft_process_semantics: ByteRaftProcessPathSemanticsEvidence,
    #[serde(default)]
    pub real_process_path_evidence_validated: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenRaftMetaProcessRolloutReport {
    #[serde(default)]
    pub voters: Vec<RaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RaftNodeId>,
    pub nodes: Vec<OpenRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: usize,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: usize,
    #[serde(default)]
    pub per_node_log_store_inspection_count: usize,
    pub mutation_proposed_through_process_api: bool,
    #[serde(default)]
    pub applied_raft_mutations: u64,
    #[serde(default)]
    pub generated_scheduler_tasks: u64,
    #[serde(default)]
    pub scheduler_retries: u64,
    #[serde(default)]
    pub stale_scheduler_token_rejected: bool,
    #[serde(default)]
    pub data_node_membership_results_ready: bool,
    #[serde(default)]
    pub scheduler_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub scheduler_task_replay_from_raft_log_observed: bool,
    #[serde(default)]
    pub membership_mutations_proposed_through_process_api: bool,
    #[serde(default)]
    pub data_node_membership_workflow_report_attached: bool,
    #[serde(default)]
    pub data_node_raft_group_results_observed: bool,
    pub read_index_validated: bool,
    pub snapshot_install_validated: bool,
    pub recovered_after_restart: bool,
    pub scheduler_task_replay_validated: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub byteraft_process_semantics: ByteRaftProcessPathSemanticsEvidence,
    #[serde(default)]
    pub real_process_path_evidence_validated: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaOwnedDataRaftMembershipReport {
    pub scheduler_task_id: u64,
    pub scheduler_generation: u64,
    pub stale_scheduler_token_rejected: bool,
    pub workflow: MetaDataRaftMembershipWorkflowReport,
    #[serde(default)]
    pub executed_steps: Vec<String>,
    #[serde(default)]
    pub final_node_evidence: Vec<OpenRaftProcessNodeEvidence>,
    #[serde(default)]
    pub final_secondary_replica_lag: u64,
    pub follower_lag_validated: bool,
    pub failover_validated: bool,
    pub scale_up_validated: bool,
    pub scale_down_validated: bool,
    pub secondary_replication_validated: bool,
    pub networked_process_api_used: bool,
    #[serde(default)]
    pub scheduler_process_api_calls_observed: u64,
    #[serde(default)]
    pub data_node_membership_apply_process_api_calls_observed: u64,
    #[serde(default)]
    pub data_node_raft_group_process_nodes_observed: usize,
    #[serde(default)]
    pub data_node_raft_group_commit_indexes_observed: Vec<u64>,
    #[serde(default)]
    pub learner_add_process_api_observed: bool,
    #[serde(default)]
    pub catchup_verification_process_api_observed: bool,
    #[serde(default)]
    pub promote_process_api_observed: bool,
    #[serde(default)]
    pub leader_transfer_process_api_observed: bool,
    #[serde(default)]
    pub voter_remove_process_api_observed: bool,
    pub persisted_through_meta_raft_replay: bool,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftTopologyMembershipPlan {
    pub shard_id: ShardId,
    pub target_voters: Vec<RaftNodeId>,
    pub target_servers: Vec<String>,
    pub no_change: bool,
    pub membership_change: Option<RaftMembershipChangePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataRaftTopologyApplyReport {
    pub plan: DataRaftTopologyMembershipPlan,
    pub applied: bool,
    pub membership_report: Option<RaftMembershipChangeReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftFailoverReport {
    pub old_leader_id: RaftNodeId,
    pub new_leader_id: RaftNodeId,
    pub term: u64,
    pub commit_index: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReadIndexResponse {
    pub leader_id: RaftNodeId,
    pub node_id: RaftNodeId,
    pub term: u64,
    pub read_index: u64,
}

pub fn plan_data_raft_membership_from_topology(
    cluster: &RaftCluster,
    topology: &TableTopologyResponse,
    servers: &[ServerMetaInfo],
    shard_id: ShardId,
) -> Result<DataRaftTopologyMembershipPlan, RaftError> {
    let partition = topology
        .partitions
        .iter()
        .find(|partition| partition.shard_id == shard_id)
        .ok_or_else(|| {
            RaftError::InvalidConfig(format!("topology does not contain shard {shard_id}"))
        })?;
    let servers_by_addr = servers
        .iter()
        .map(|server| (server.server_addr.as_str(), server))
        .collect::<BTreeMap<_, _>>();
    let mut target_servers = Vec::new();
    let mut seen_servers = BTreeSet::new();
    if let Some(primary) = &partition.primary {
        if seen_servers.insert(primary.clone()) {
            target_servers.push(primary.clone());
        }
    }
    for replica in &partition.replicas {
        if seen_servers.insert(replica.clone()) {
            target_servers.push(replica.clone());
        }
    }
    if target_servers.is_empty() {
        return Err(RaftError::InvalidConfig(format!(
            "topology shard {shard_id} has no primary or replicas"
        )));
    }

    let mut target_voters = Vec::new();
    for server_addr in &target_servers {
        let server = servers_by_addr.get(server_addr.as_str()).ok_or_else(|| {
            RaftError::InvalidConfig(format!(
                "topology shard {shard_id} references unknown server {server_addr}"
            ))
        })?;
        if server.state != MetaEntityState::Normal {
            return Err(RaftError::InvalidConfig(format!(
                "topology shard {shard_id} references non-normal server {server_addr}"
            )));
        }
        if server.node_id == 0 {
            return Err(RaftError::InvalidConfig(format!(
                "server {server_addr} has no data raft node id"
            )));
        }
        target_voters.push(server.node_id);
    }
    target_voters.sort_unstable();
    target_voters.dedup();

    let current_voters = cluster
        .status()
        .nodes
        .into_iter()
        .filter(|node| node.replica_role.participates_in_quorum())
        .map(|node| node.node_id)
        .collect::<BTreeSet<_>>();
    let target_set = target_voters.iter().copied().collect::<BTreeSet<_>>();
    if current_voters == target_set {
        return Ok(DataRaftTopologyMembershipPlan {
            shard_id,
            target_voters,
            target_servers,
            no_change: true,
            membership_change: None,
        });
    }
    let membership_change = cluster.plan_membership_change(target_voters.clone())?;
    Ok(DataRaftTopologyMembershipPlan {
        shard_id,
        target_voters,
        target_servers,
        no_change: false,
        membership_change: Some(membership_change),
    })
}

pub fn apply_data_raft_membership_from_topology(
    cluster: &RaftCluster,
    topology: &TableTopologyResponse,
    servers: &[ServerMetaInfo],
    shard_id: ShardId,
) -> Result<DataRaftTopologyApplyReport, RaftError> {
    let plan = plan_data_raft_membership_from_topology(cluster, topology, servers, shard_id)?;
    if plan.no_change {
        return Ok(DataRaftTopologyApplyReport {
            plan,
            applied: false,
            membership_report: None,
        });
    }
    let membership_report = cluster.apply_membership_change_safely(plan.target_voters.clone())?;
    Ok(DataRaftTopologyApplyReport {
        plan,
        applied: true,
        membership_report: Some(membership_report),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
