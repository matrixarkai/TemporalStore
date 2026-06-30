use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::meta::{MetaEntityState, ServerMetaInfo, TableTopologyResponse};
use crate::types::ShardId;

use super::{
    JointConsensusMembership, RaftCluster, RaftError, RaftMembership, RaftNodeId, RaftReplicaLag,
};

pub type TemporalRaftProcessNodeEvidence = rustraft::RustRaftProcessNodeEvidence;
pub type TemporalRaftProcessOperationalSemanticsEvidence =
    rustraft::RustRaftProcessOperationalSemanticsEvidence;
pub type TemporalRaftDataNodeProcessRolloutReport = rustraft::RustRaftDataNodeProcessRolloutReport;
pub type TemporalRaftMetaProcessRolloutReport = rustraft::RustRaftMetaProcessRolloutReport;

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
