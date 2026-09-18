// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::{
    matrixraft_parity_contract as library_matrixraft_parity_contract,
    matrixraft_parity_report as library_matrixraft_parity_report,
    matrixraft_baseline_raft_runtime_capability_prometheus as library_matrixraft_reference_raft_runtime_capability_prometheus,
};

use super::{distributed_raft_readiness, RaftDistributedReadiness};

pub use matrixraft::{
    matrixraft_admin_status_surface_evidence,
    matrixraft_capability_evidence_from_fields,
    matrixraft_cross_plane_process_readiness_blocker_report,
    matrixraft_data_node_process_rollout_blockers,
    matrixraft_data_node_strict_process_rollout_validated,
    matrixraft_meta_process_rollout_blockers,
    matrixraft_meta_strict_process_rollout_validated,
    matrixraft_named_readiness_blockers,
    matrixraft_peer_pipeline_status_from_observed,
    matrixraft_pipeline_evidence,
    matrixraft_production_readiness_report,
    matrixraft_read_safety_runtime_decision,
    matrixraft_baseline_raft_runtime_capability_report as matrixraft_reference_raft_runtime_capability_report,
    matrixraft_runtime_capability_report_from_evidence,
    matrixraft_snapshot_lifecycle_evidence,
    matrixraft_validate_deployment_mode,
    matrixraft_validate_deployment_readiness,
    matrixraft_wal_lifecycle_evidence, CapabilityEvidence,
    AdminStatusSurfaceEvidence as MatrixRaftAdminStatusSurfaceEvidence,
    AdminStatusSurfaceInput as MatrixRaftAdminStatusSurfaceInput,
    CrossPlaneProcessReadinessBlockerReport as MatrixRaftCrossPlaneProcessReadinessBlockerReport,
    DataNodeProcessRolloutReport as MatrixRaftDataNodeProcessRolloutReport,
    DeploymentMode as MatrixRaftDeploymentMode,
    MetaProcessRolloutReport as MatrixRaftMetaProcessRolloutReport,
    ObservedPeerPipeline as MatrixRaftObservedPeerPipeline,
    ParityContract as MatrixRaftParityContract,
    ParityReport as MatrixRaftParityReport,
    PeerProgress as MatrixRaftPeerPipelineStatus,
    PipelineEvidence as MatrixRaftPipelineEvidence,
    PipelineLimits as MatrixRaftPipelineLimits,
    ProcessNodeEvidence as MatrixRaftProcessNodeEvidence,
    ProcessOperationalSemanticsEvidence as MatrixRaftProcessOperationalSemanticsEvidence,
    ProcessReadinessBlocker as MatrixRaftProcessReadinessBlocker,
    ProductionReadinessError as MatrixRaftProductionReadinessError,
    ProductionReadinessInput as MatrixRaftProductionReadinessInput,
    ProductionReadinessReport as MatrixRaftProductionReadinessReport,
    PrometheusMetricSet as MatrixRaftPrometheusMetricSet,
    ReadSafetyOperation as MatrixRaftReadSafetyOperation,
    ReadSafetyRuntimeDecision as MatrixRaftReadSafetyRuntimeDecision,
    ReadSafetyRuntimeInput as MatrixRaftReadSafetyRuntimeInput,
    ReadinessEvidence as MatrixRaftReadinessEvidence,
    ReadinessSnapshot as MatrixRaftReadinessSnapshot,
    BaselineRaftRuntimeCapabilityReport as MatrixRaftReferenceRaftRuntimeCapabilityReport,
    SemanticRequirement as MatrixRaftSemanticRequirement,
    SnapshotLifecycleEvidence as MatrixRaftSnapshotLifecycleEvidence,
    WalLifecycleEvidence as MatrixRaftWalLifecycleEvidence,
    WalLifecycleStatus as MatrixRaftWalLifecycleStatus,
};

impl From<&RaftDistributedReadiness> for MatrixRaftReadinessSnapshot {
    fn from(readiness: &RaftDistributedReadiness) -> Self {
        Self {
            matrixraft_leader_write_authority_present: readiness
                .matrixraft_leader_write_authority_present,
            matrixraft_operator_observability_present: readiness
                .matrixraft_operator_observability_present,
            matrixraft_rpc_transport_contract_present: readiness
                .matrixraft_rpc_transport_contract_present,
            matrixraft_log_retention_snapshot_trigger_present: readiness
                .matrixraft_log_retention_snapshot_trigger_present,
            matrixraft_apply_snapshot_fence_present: readiness
                .matrixraft_apply_snapshot_fence_present,
            raft_storage_apply_fence_present: readiness.raft_storage_apply_fence_present,
            matrixraft_snapshot_floor_log_matching_present: readiness
                .matrixraft_snapshot_floor_log_matching_present,
            matrixraft_snapshot_tail_catchup_present: readiness
                .matrixraft_snapshot_tail_catchup_present,
            matrixraft_compacted_entry_rejection_present: readiness
                .matrixraft_compacted_entry_rejection_present,
            matrixraft_metaserver_snapshot_floor_election_present: readiness
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
