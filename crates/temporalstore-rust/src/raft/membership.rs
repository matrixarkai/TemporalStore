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
pub struct TemporalRaftProcessNodeEvidence {
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
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalRaftProcessOperationalSemanticsEvidence {
    #[serde(default)]
    pub api_presence_only_rejected: bool,
    #[serde(default)]
    pub process_path_validated: bool,
    #[serde(default)]
    pub read_index_validated: bool,
    #[serde(default)]
    pub leader_lease_validated: bool,
    #[serde(default)]
    pub stale_leader_lease_rejection_observed: bool,
    #[serde(default)]
    pub follower_lease_expiration_observed: bool,
    #[serde(default)]
    pub lagging_follower_read_rejected: bool,
    #[serde(default)]
    pub bounded_stale_read_acceptance_observed: bool,
    #[serde(default)]
    pub bounded_stale_read_rejection_observed: bool,
    #[serde(default)]
    pub minority_partition_read_rejection_observed: bool,
    #[serde(default)]
    pub healed_follower_catchup_observed: bool,
    #[serde(default)]
    pub stale_follower_write_rejected: bool,
    #[serde(default)]
    pub leader_transfer_exact_once_validated: bool,
    #[serde(default)]
    pub leader_transfer_under_load_validated: bool,
    #[serde(default)]
    pub snapshot_bootstrap_validated: bool,
    #[serde(default)]
    pub snapshot_install_restart_validated: bool,
    #[serde(default)]
    pub membership_rescale_validated: bool,
    #[serde(default)]
    pub membership_add_promote_remove_validated: bool,
    #[serde(default)]
    pub follower_rejoin_after_compaction_validated: bool,
    #[serde(default)]
    pub secondary_read_eligibility_validated: bool,
    #[serde(default)]
    pub apply_pipeline_converged: bool,
    #[serde(default)]
    pub wal_persistence_observed: bool,
    #[serde(default)]
    pub fsm_apply_idempotent_replay_observed: bool,
    #[serde(default)]
    pub storage_mutation_wal_fence_atomicity_observed: bool,
    #[serde(default)]
    pub snapshot_install_apply_fence_atomicity_observed: bool,
    #[serde(default)]
    pub process_restart_after_apply_crash_recovered: bool,
    #[serde(default)]
    pub ready: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

impl TemporalRaftProcessOperationalSemanticsEvidence {
    pub fn proves_runtime_semantics(&self) -> bool {
        self.ready
            && self.blockers.is_empty()
            && self.api_presence_only_rejected
            && self.process_path_validated
            && self.read_index_validated
            && self.leader_lease_validated
            && self.stale_leader_lease_rejection_observed
            && self.follower_lease_expiration_observed
            && self.lagging_follower_read_rejected
            && self.bounded_stale_read_acceptance_observed
            && self.bounded_stale_read_rejection_observed
            && self.minority_partition_read_rejection_observed
            && self.healed_follower_catchup_observed
            && self.stale_follower_write_rejected
            && self.leader_transfer_exact_once_validated
            && self.leader_transfer_under_load_validated
            && self.snapshot_bootstrap_validated
            && self.snapshot_install_restart_validated
            && self.membership_rescale_validated
            && self.membership_add_promote_remove_validated
            && self.follower_rejoin_after_compaction_validated
            && self.secondary_read_eligibility_validated
            && self.apply_pipeline_converged
            && self.wal_persistence_observed
            && self.fsm_apply_idempotent_replay_observed
            && self.storage_mutation_wal_fence_atomicity_observed
            && self.snapshot_install_apply_fence_atomicity_observed
            && self.process_restart_after_apply_crash_recovered
    }

    pub fn missing_requirements(&self) -> Vec<String> {
        let mut missing = Vec::new();
        for (present, requirement) in [
            (self.ready, "operational_semantics_ready"),
            (
                self.api_presence_only_rejected,
                "api_presence_only_rejected",
            ),
            (self.process_path_validated, "process_path_validated"),
            (self.read_index_validated, "read_index_validated"),
            (self.leader_lease_validated, "leader_lease_validated"),
            (
                self.stale_leader_lease_rejection_observed,
                "stale_leader_lease_rejection_observed",
            ),
            (
                self.follower_lease_expiration_observed,
                "follower_lease_expiration_observed",
            ),
            (
                self.lagging_follower_read_rejected,
                "lagging_follower_read_rejected",
            ),
            (
                self.bounded_stale_read_acceptance_observed,
                "bounded_stale_read_acceptance_observed",
            ),
            (
                self.bounded_stale_read_rejection_observed,
                "bounded_stale_read_rejection_observed",
            ),
            (
                self.minority_partition_read_rejection_observed,
                "minority_partition_read_rejection_observed",
            ),
            (
                self.healed_follower_catchup_observed,
                "healed_follower_catchup_observed",
            ),
            (
                self.stale_follower_write_rejected,
                "stale_follower_write_rejected",
            ),
            (
                self.leader_transfer_exact_once_validated,
                "leader_transfer_exact_once_validated",
            ),
            (
                self.leader_transfer_under_load_validated,
                "leader_transfer_under_load_validated",
            ),
            (
                self.snapshot_bootstrap_validated,
                "snapshot_bootstrap_validated",
            ),
            (
                self.snapshot_install_restart_validated,
                "snapshot_install_restart_validated",
            ),
            (
                self.membership_rescale_validated,
                "membership_rescale_validated",
            ),
            (
                self.membership_add_promote_remove_validated,
                "membership_add_promote_remove_validated",
            ),
            (
                self.follower_rejoin_after_compaction_validated,
                "follower_rejoin_after_compaction_validated",
            ),
            (
                self.secondary_read_eligibility_validated,
                "secondary_read_eligibility_validated",
            ),
            (self.apply_pipeline_converged, "apply_pipeline_converged"),
            (self.wal_persistence_observed, "wal_persistence_observed"),
            (
                self.fsm_apply_idempotent_replay_observed,
                "fsm_apply_idempotent_replay_observed",
            ),
            (
                self.storage_mutation_wal_fence_atomicity_observed,
                "storage_mutation_wal_fence_atomicity_observed",
            ),
            (
                self.snapshot_install_apply_fence_atomicity_observed,
                "snapshot_install_apply_fence_atomicity_observed",
            ),
            (
                self.process_restart_after_apply_crash_recovered,
                "process_restart_after_apply_crash_recovered",
            ),
        ] {
            if !present {
                missing.push(requirement.to_string());
            }
        }
        missing.extend(
            self.blockers
                .iter()
                .map(|blocker| format!("blocker:{blocker}")),
        );
        missing
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalRaftDataNodeProcessRolloutReport {
    pub shard_id: ShardId,
    #[serde(default)]
    pub voters: Vec<RaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RaftNodeId>,
    pub nodes: Vec<TemporalRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
    pub write_proposed_through_process_api: bool,
    #[serde(default)]
    pub leader_transfer_validated: bool,
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
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub recovered_after_restart: bool,
    #[serde(default)]
    pub restart_recovery_validated: bool,
    pub snapshot_install_validated: bool,
    pub applied_fence_validated: bool,
    #[serde(default)]
    pub crash_after_storage_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_snapshot_install_recovered: bool,
    #[serde(default)]
    pub apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: TemporalRaftProcessOperationalSemanticsEvidence,
    pub ready: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TemporalRaftMetaProcessRolloutReport {
    #[serde(default)]
    pub voters: Vec<RaftNodeId>,
    #[serde(default)]
    pub learners: Vec<RaftNodeId>,
    pub nodes: Vec<TemporalRaftProcessNodeEvidence>,
    #[serde(default)]
    pub spawned_process_count: u64,
    #[serde(default)]
    pub independent_wal_dirs: bool,
    #[serde(default)]
    pub independent_snapshot_dirs: bool,
    #[serde(default)]
    pub observed_process_requests: u64,
    #[serde(default)]
    pub read_index_responses_observed: u64,
    #[serde(default)]
    pub restarted_node_count: u64,
    #[serde(default)]
    pub per_node_log_store_inspection_count: u64,
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
    #[serde(default)]
    pub failover_validated: bool,
    #[serde(default)]
    pub membership_change_validated: bool,
    #[serde(default)]
    pub follower_lag_validated: bool,
    #[serde(default)]
    pub secondary_read_validated: bool,
    pub read_index_validated: bool,
    pub snapshot_install_validated: bool,
    pub recovered_after_restart: bool,
    pub scheduler_task_replay_validated: bool,
    #[serde(default)]
    pub crash_after_meta_mutation_recovered: bool,
    #[serde(default)]
    pub crash_after_meta_wal_persist_recovered: bool,
    #[serde(default)]
    pub crash_during_meta_snapshot_install_recovered: bool,
    #[serde(default)]
    pub meta_apply_fence_recovered_after_restart: bool,
    pub multi_process_log_store_validated: bool,
    #[serde(default)]
    pub operational_semantics: TemporalRaftProcessOperationalSemanticsEvidence,
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
    pub final_node_evidence: Vec<TemporalRaftProcessNodeEvidence>,
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
    #[serde(default)]
    pub scheduler_generation_token_coupling_observed: bool,
    #[serde(default)]
    pub stale_generation_rejection_observed: bool,
    #[serde(default)]
    pub membership_generation_replayed_from_meta_raft: bool,
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
