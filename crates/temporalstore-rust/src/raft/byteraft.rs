use serde::{Deserialize, Serialize};

use super::{distributed_raft_readiness, RaftDistributedReadiness};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftSemanticRequirement {
    pub id: String,
    pub description: String,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftParityContract {
    pub consensus_backend_boundary: String,
    pub data_node_backend_trait: String,
    pub metaserver_backend_trait: String,
    pub openraft_dependency_removed: bool,
    pub temporal_raft_runtime_available: bool,
    pub requirements: Vec<ByteRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ByteRaftParityReport {
    pub ready: bool,
    pub contract: ByteRaftParityContract,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
}

pub fn byteraft_parity_contract() -> ByteRaftParityContract {
    ByteRaftParityContract {
        consensus_backend_boundary:
            "temporalstore_rust::raft::DataRaftConsensusBackend".to_string(),
        data_node_backend_trait: "DataRaftConsensusBackend".to_string(),
        metaserver_backend_trait: "DataRaftConsensusBackend".to_string(),
        openraft_dependency_removed: true,
        temporal_raft_runtime_available: true,
        requirements: vec![
            requirement(
                "leader_write_authority",
                "Leader-only writes and bounded stale-read authority match ByteRaft semantics.",
                "byteraft_leader_write_authority_present",
            ),
            requirement(
                "operator_observability",
                "Operator-facing status exposes leader, term, commit, apply, and peer state.",
                "byteraft_operator_observability_present",
            ),
            requirement(
                "rpc_transport_contract",
                "AppendEntries, Vote, InstallSnapshot, and ReadIndex transport contracts exist.",
                "byteraft_rpc_transport_contract_present",
            ),
            requirement(
                "snapshot_trigger",
                "Log retention can trigger durable snapshots before unbounded growth.",
                "byteraft_log_retention_snapshot_trigger_present",
            ),
            requirement(
                "apply_snapshot_fence",
                "Snapshot install has an apply fence so stale logs cannot overwrite restored state.",
                "byteraft_apply_snapshot_fence_present",
            ),
            requirement(
                "storage_apply_fence",
                "Storage mutation apply is fenced with durable apply index state.",
                "raft_storage_apply_fence_present",
            ),
            requirement(
                "snapshot_floor_log_matching",
                "Snapshot floor and log matching reject unsafe stale or compacted entries.",
                "byteraft_snapshot_floor_log_matching_present",
            ),
            requirement(
                "snapshot_tail_catchup",
                "Followers can catch up from snapshot plus tail logs.",
                "byteraft_snapshot_tail_catchup_present",
            ),
            requirement(
                "compacted_entry_rejection",
                "Compacted entries are rejected rather than silently replayed.",
                "byteraft_compacted_entry_rejection_present",
            ),
            requirement(
                "metaserver_snapshot_floor_election",
                "Metaserver election/readiness respects snapshot floor safety.",
                "byteraft_metaserver_snapshot_floor_election_present",
            ),
            requirement(
                "learner_catchup_promotion",
                "Learners are promoted only after catch-up and membership workflow checks.",
                "learner_catchup_promotion_present",
            ),
            requirement(
                "metaserver_membership_workflow",
                "Metaserver owns membership workflow and topology placement transitions.",
                "metaserver_membership_workflow_present",
            ),
        ],
    }
}

pub fn byteraft_parity_report(readiness: &RaftDistributedReadiness) -> ByteRaftParityReport {
    let contract = byteraft_parity_contract();
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    for requirement in &contract.requirements {
        if readiness_value(readiness, &requirement.readiness_field) {
            satisfied.push(requirement.id.clone());
        } else {
            missing.push(requirement.id.clone());
        }
    }
    ByteRaftParityReport {
        ready: missing.is_empty() && contract.openraft_dependency_removed,
        contract,
        satisfied,
        missing,
    }
}

pub fn byteraft_parity_report_from_current_readiness() -> ByteRaftParityReport {
    byteraft_parity_report(&distributed_raft_readiness())
}

fn requirement(
    id: &str,
    description: &str,
    readiness_field: &str,
) -> ByteRaftSemanticRequirement {
    ByteRaftSemanticRequirement {
        id: id.to_string(),
        description: description.to_string(),
        readiness_field: readiness_field.to_string(),
        required_for_production: true,
    }
}

fn readiness_value(readiness: &RaftDistributedReadiness, field: &str) -> bool {
    match field {
        "byteraft_leader_write_authority_present" => {
            readiness.byteraft_leader_write_authority_present
        }
        "byteraft_operator_observability_present" => {
            readiness.byteraft_operator_observability_present
        }
        "byteraft_rpc_transport_contract_present" => {
            readiness.byteraft_rpc_transport_contract_present
        }
        "byteraft_log_retention_snapshot_trigger_present" => {
            readiness.byteraft_log_retention_snapshot_trigger_present
        }
        "byteraft_apply_snapshot_fence_present" => {
            readiness.byteraft_apply_snapshot_fence_present
        }
        "raft_storage_apply_fence_present" => readiness.raft_storage_apply_fence_present,
        "byteraft_snapshot_floor_log_matching_present" => {
            readiness.byteraft_snapshot_floor_log_matching_present
        }
        "byteraft_snapshot_tail_catchup_present" => {
            readiness.byteraft_snapshot_tail_catchup_present
        }
        "byteraft_compacted_entry_rejection_present" => {
            readiness.byteraft_compacted_entry_rejection_present
        }
        "byteraft_metaserver_snapshot_floor_election_present" => {
            readiness.byteraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => readiness.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => {
            readiness.metaserver_membership_workflow_present
        }
        _ => false,
    }
}
