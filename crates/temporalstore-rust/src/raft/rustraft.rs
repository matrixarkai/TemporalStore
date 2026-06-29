use serde::{Deserialize, Serialize};

use super::{distributed_raft_readiness, RaftDistributedReadiness};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftSemanticRequirement {
    pub id: String,
    pub description: String,
    pub readiness_field: String,
    pub required_for_production: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityContract {
    pub consensus_backend_boundary: String,
    pub data_node_backend_trait: String,
    pub metaserver_backend_trait: String,
    pub openraft_dependency_removed: bool,
    pub temporal_raft_runtime_available: bool,
    pub requirements: Vec<RustRaftSemanticRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftParityReport {
    pub ready: bool,
    pub contract: RustRaftParityContract,
    pub satisfied: Vec<String>,
    pub missing: Vec<String>,
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    RustRaftParityContract {
        consensus_backend_boundary:
            "temporalstore_rust::raft::DataRaftConsensusBackend".to_string(),
        data_node_backend_trait: "DataRaftConsensusBackend".to_string(),
        metaserver_backend_trait: "DataRaftConsensusBackend".to_string(),
        openraft_dependency_removed: true,
        temporal_raft_runtime_available: true,
        requirements: vec![
            requirement(
                "leader_write_authority",
                "Leader-only writes and bounded stale-read authority match RustRaft semantics.",
                "rustraft_leader_write_authority_present",
            ),
            requirement(
                "operator_observability",
                "Operator-facing status exposes leader, term, commit, apply, and peer state.",
                "rustraft_operator_observability_present",
            ),
            requirement(
                "rpc_transport_contract",
                "AppendEntries, Vote, InstallSnapshot, and ReadIndex transport contracts exist.",
                "rustraft_rpc_transport_contract_present",
            ),
            requirement(
                "snapshot_trigger",
                "Log retention can trigger durable snapshots before unbounded growth.",
                "rustraft_log_retention_snapshot_trigger_present",
            ),
            requirement(
                "apply_snapshot_fence",
                "Snapshot install has an apply fence so stale logs cannot overwrite restored state.",
                "rustraft_apply_snapshot_fence_present",
            ),
            requirement(
                "storage_apply_fence",
                "Storage mutation apply is fenced with durable apply index state.",
                "raft_storage_apply_fence_present",
            ),
            requirement(
                "snapshot_floor_log_matching",
                "Snapshot floor and log matching reject unsafe stale or compacted entries.",
                "rustraft_snapshot_floor_log_matching_present",
            ),
            requirement(
                "snapshot_tail_catchup",
                "Followers can catch up from snapshot plus tail logs.",
                "rustraft_snapshot_tail_catchup_present",
            ),
            requirement(
                "compacted_entry_rejection",
                "Compacted entries are rejected rather than silently replayed.",
                "rustraft_compacted_entry_rejection_present",
            ),
            requirement(
                "metaserver_snapshot_floor_election",
                "Metaserver election/readiness respects snapshot floor safety.",
                "rustraft_metaserver_snapshot_floor_election_present",
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

pub fn rustraft_parity_report(readiness: &RaftDistributedReadiness) -> RustRaftParityReport {
    let contract = rustraft_parity_contract();
    let mut satisfied = Vec::new();
    let mut missing = Vec::new();
    for requirement in &contract.requirements {
        if readiness_value(readiness, &requirement.readiness_field) {
            satisfied.push(requirement.id.clone());
        } else {
            missing.push(requirement.id.clone());
        }
    }
    RustRaftParityReport {
        ready: missing.is_empty() && contract.openraft_dependency_removed,
        contract,
        satisfied,
        missing,
    }
}

pub fn rustraft_parity_report_from_current_readiness() -> RustRaftParityReport {
    rustraft_parity_report(&distributed_raft_readiness())
}

fn requirement(
    id: &str,
    description: &str,
    readiness_field: &str,
) -> RustRaftSemanticRequirement {
    RustRaftSemanticRequirement {
        id: id.to_string(),
        description: description.to_string(),
        readiness_field: readiness_field.to_string(),
        required_for_production: true,
    }
}

fn readiness_value(readiness: &RaftDistributedReadiness, field: &str) -> bool {
    match field {
        "rustraft_leader_write_authority_present" => {
            readiness.rustraft_leader_write_authority_present
        }
        "rustraft_operator_observability_present" => {
            readiness.rustraft_operator_observability_present
        }
        "rustraft_rpc_transport_contract_present" => {
            readiness.rustraft_rpc_transport_contract_present
        }
        "rustraft_log_retention_snapshot_trigger_present" => {
            readiness.rustraft_log_retention_snapshot_trigger_present
        }
        "rustraft_apply_snapshot_fence_present" => {
            readiness.rustraft_apply_snapshot_fence_present
        }
        "raft_storage_apply_fence_present" => readiness.raft_storage_apply_fence_present,
        "rustraft_snapshot_floor_log_matching_present" => {
            readiness.rustraft_snapshot_floor_log_matching_present
        }
        "rustraft_snapshot_tail_catchup_present" => {
            readiness.rustraft_snapshot_tail_catchup_present
        }
        "rustraft_compacted_entry_rejection_present" => {
            readiness.rustraft_compacted_entry_rejection_present
        }
        "rustraft_metaserver_snapshot_floor_election_present" => {
            readiness.rustraft_metaserver_snapshot_floor_election_present
        }
        "learner_catchup_promotion_present" => readiness.learner_catchup_promotion_present,
        "metaserver_membership_workflow_present" => {
            readiness.metaserver_membership_workflow_present
        }
        _ => false,
    }
}
