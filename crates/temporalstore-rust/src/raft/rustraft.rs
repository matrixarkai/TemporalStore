use rustraft::{
    rustraft_parity_contract as library_rustraft_parity_contract,
    rustraft_parity_report as library_rustraft_parity_report,
};

use super::{distributed_raft_readiness, RaftDistributedReadiness};

pub use rustraft::{
    rustraft_byteraft_runtime_capability_prometheus, rustraft_byteraft_runtime_capability_report,
    rustraft_pipeline_evidence, rustraft_production_readiness_report,
    rustraft_read_safety_runtime_decision, rustraft_snapshot_lifecycle_evidence,
    rustraft_wal_lifecycle_evidence, RaftCapabilityEvidence,
    RustRaftByteRaftRuntimeCapabilityReport, RustRaftParityContract, RustRaftParityReport,
    RustRaftPeerPipelineStatus, RustRaftPipelineEvidence, RustRaftPipelineLimits,
    RustRaftProductionReadinessInput, RustRaftProductionReadinessReport,
    RustRaftPrometheusMetricSet, RustRaftReadSafetyOperation, RustRaftReadSafetyRuntimeDecision,
    RustRaftReadSafetyRuntimeInput, RustRaftReadinessEvidence, RustRaftReadinessSnapshot,
    RustRaftSemanticRequirement, RustRaftSnapshotLifecycleEvidence, RustRaftWalLifecycleEvidence,
    RustRaftWalLifecycleStatus,
};

impl From<&RaftDistributedReadiness> for RustRaftReadinessSnapshot {
    fn from(readiness: &RaftDistributedReadiness) -> Self {
        Self {
            rustraft_leader_write_authority_present: readiness
                .rustraft_leader_write_authority_present,
            rustraft_operator_observability_present: readiness
                .rustraft_operator_observability_present,
            rustraft_rpc_transport_contract_present: readiness
                .rustraft_rpc_transport_contract_present,
            rustraft_log_retention_snapshot_trigger_present: readiness
                .rustraft_log_retention_snapshot_trigger_present,
            rustraft_apply_snapshot_fence_present: readiness.rustraft_apply_snapshot_fence_present,
            raft_storage_apply_fence_present: readiness.raft_storage_apply_fence_present,
            rustraft_snapshot_floor_log_matching_present: readiness
                .rustraft_snapshot_floor_log_matching_present,
            rustraft_snapshot_tail_catchup_present: readiness
                .rustraft_snapshot_tail_catchup_present,
            rustraft_compacted_entry_rejection_present: readiness
                .rustraft_compacted_entry_rejection_present,
            rustraft_metaserver_snapshot_floor_election_present: readiness
                .rustraft_metaserver_snapshot_floor_election_present,
            learner_catchup_promotion_present: readiness.learner_catchup_promotion_present,
            metaserver_membership_workflow_present: readiness
                .metaserver_membership_workflow_present,
        }
    }
}

pub fn rustraft_parity_contract() -> RustRaftParityContract {
    library_rustraft_parity_contract()
}

pub fn rustraft_parity_report(readiness: &RaftDistributedReadiness) -> RustRaftParityReport {
    let snapshot = RustRaftReadinessSnapshot::from(readiness);
    library_rustraft_parity_report(&snapshot)
}

pub fn rustraft_parity_report_from_current_readiness() -> RustRaftParityReport {
    rustraft_parity_report(&distributed_raft_readiness())
}
