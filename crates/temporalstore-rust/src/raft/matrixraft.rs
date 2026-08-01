use matrixraft::{
    rustraft_parity_contract as library_matrixraft_parity_contract,
    rustraft_parity_report as library_matrixraft_parity_report,
    rustraft_reference_raft_runtime_capability_prometheus as library_matrixraft_reference_raft_runtime_capability_prometheus,
};

use super::{distributed_raft_readiness, RaftDistributedReadiness};

pub use matrixraft::{
    rustraft_admin_status_surface_evidence as matrixraft_admin_status_surface_evidence,
    rustraft_capability_evidence_from_fields as matrixraft_capability_evidence_from_fields,
    rustraft_cross_plane_process_readiness_blocker_report as matrixraft_cross_plane_process_readiness_blocker_report,
    rustraft_data_node_process_rollout_blockers as matrixraft_data_node_process_rollout_blockers,
    rustraft_data_node_strict_process_rollout_validated as matrixraft_data_node_strict_process_rollout_validated,
    rustraft_meta_process_rollout_blockers as matrixraft_meta_process_rollout_blockers,
    rustraft_meta_strict_process_rollout_validated as matrixraft_meta_strict_process_rollout_validated,
    rustraft_named_readiness_blockers as matrixraft_named_readiness_blockers,
    rustraft_peer_pipeline_status_from_observed as matrixraft_peer_pipeline_status_from_observed,
    rustraft_pipeline_evidence as matrixraft_pipeline_evidence,
    rustraft_production_readiness_report as matrixraft_production_readiness_report,
    rustraft_read_safety_runtime_decision as matrixraft_read_safety_runtime_decision,
    rustraft_reference_raft_runtime_capability_report as matrixraft_reference_raft_runtime_capability_report,
    rustraft_runtime_capability_report_from_evidence as matrixraft_runtime_capability_report_from_evidence,
    rustraft_snapshot_lifecycle_evidence as matrixraft_snapshot_lifecycle_evidence,
    rustraft_validate_deployment_mode as matrixraft_validate_deployment_mode,
    rustraft_validate_deployment_readiness as matrixraft_validate_deployment_readiness,
    rustraft_wal_lifecycle_evidence as matrixraft_wal_lifecycle_evidence, RaftCapabilityEvidence,
    RustRaftAdminStatusSurfaceEvidence as MatrixRaftAdminStatusSurfaceEvidence,
    RustRaftAdminStatusSurfaceInput as MatrixRaftAdminStatusSurfaceInput,
    RustRaftCrossPlaneProcessReadinessBlockerReport as MatrixRaftCrossPlaneProcessReadinessBlockerReport,
    RustRaftDataNodeProcessRolloutReport as MatrixRaftDataNodeProcessRolloutReport,
    RustRaftDeploymentMode as MatrixRaftDeploymentMode,
    RustRaftMetaProcessRolloutReport as MatrixRaftMetaProcessRolloutReport,
    RustRaftObservedPeerPipeline as MatrixRaftObservedPeerPipeline,
    RustRaftParityContract as MatrixRaftParityContract,
    RustRaftParityReport as MatrixRaftParityReport,
    RustRaftPeerPipelineStatus as MatrixRaftPeerPipelineStatus,
    RustRaftPipelineEvidence as MatrixRaftPipelineEvidence,
    RustRaftPipelineLimits as MatrixRaftPipelineLimits,
    RustRaftProcessNodeEvidence as MatrixRaftProcessNodeEvidence,
    RustRaftProcessOperationalSemanticsEvidence as MatrixRaftProcessOperationalSemanticsEvidence,
    RustRaftProcessReadinessBlocker as MatrixRaftProcessReadinessBlocker,
    RustRaftProductionReadinessError as MatrixRaftProductionReadinessError,
    RustRaftProductionReadinessInput as MatrixRaftProductionReadinessInput,
    RustRaftProductionReadinessReport as MatrixRaftProductionReadinessReport,
    RustRaftPrometheusMetricSet as MatrixRaftPrometheusMetricSet,
    RustRaftReadSafetyOperation as MatrixRaftReadSafetyOperation,
    RustRaftReadSafetyRuntimeDecision as MatrixRaftReadSafetyRuntimeDecision,
    RustRaftReadSafetyRuntimeInput as MatrixRaftReadSafetyRuntimeInput,
    RustRaftReadinessEvidence as MatrixRaftReadinessEvidence,
    RustRaftReadinessSnapshot as MatrixRaftReadinessSnapshot,
    RustRaftReferenceRaftRuntimeCapabilityReport as MatrixRaftReferenceRaftRuntimeCapabilityReport,
    RustRaftSemanticRequirement as MatrixRaftSemanticRequirement,
    RustRaftSnapshotLifecycleEvidence as MatrixRaftSnapshotLifecycleEvidence,
    RustRaftWalLifecycleEvidence as MatrixRaftWalLifecycleEvidence,
    RustRaftWalLifecycleStatus as MatrixRaftWalLifecycleStatus,
};

impl From<&RaftDistributedReadiness> for MatrixRaftReadinessSnapshot {
    fn from(readiness: &RaftDistributedReadiness) -> Self {
        Self {
            rustraft_leader_write_authority_present: readiness
                .matrixraft_leader_write_authority_present,
            rustraft_operator_observability_present: readiness
                .matrixraft_operator_observability_present,
            rustraft_rpc_transport_contract_present: readiness
                .matrixraft_rpc_transport_contract_present,
            rustraft_log_retention_snapshot_trigger_present: readiness
                .matrixraft_log_retention_snapshot_trigger_present,
            rustraft_apply_snapshot_fence_present: readiness
                .matrixraft_apply_snapshot_fence_present,
            raft_storage_apply_fence_present: readiness.raft_storage_apply_fence_present,
            rustraft_snapshot_floor_log_matching_present: readiness
                .matrixraft_snapshot_floor_log_matching_present,
            rustraft_snapshot_tail_catchup_present: readiness
                .matrixraft_snapshot_tail_catchup_present,
            rustraft_compacted_entry_rejection_present: readiness
                .matrixraft_compacted_entry_rejection_present,
            rustraft_metaserver_snapshot_floor_election_present: readiness
                .matrixraft_metaserver_snapshot_floor_election_present,
            learner_catchup_promotion_present: readiness.learner_catchup_promotion_present,
            metaserver_membership_workflow_present: readiness
                .metaserver_membership_workflow_present,
        }
    }
}

pub fn matrixraft_parity_contract() -> MatrixRaftParityContract {
    library_matrixraft_parity_contract()
}

pub fn matrixraft_parity_report(readiness: &RaftDistributedReadiness) -> MatrixRaftParityReport {
    let snapshot = MatrixRaftReadinessSnapshot::from(readiness);
    library_matrixraft_parity_report(&snapshot)
}

pub fn matrixraft_parity_report_from_current_readiness() -> MatrixRaftParityReport {
    matrixraft_parity_report(&distributed_raft_readiness())
}

pub fn matrixraft_reference_raft_runtime_capability_prometheus(
    report: &MatrixRaftReferenceRaftRuntimeCapabilityReport,
    labels: &[(&str, &str)],
) -> MatrixRaftPrometheusMetricSet {
    let mut metrics =
        library_matrixraft_reference_raft_runtime_capability_prometheus(report, labels);
    metrics.text = metrics
        .text
        .replace("rustraft", "matrixraft")
        .replace("RustRaft", "MatrixRaft");
    metrics
}
