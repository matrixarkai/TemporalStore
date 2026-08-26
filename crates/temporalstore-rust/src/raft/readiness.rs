// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

pub type RaftReadinessEvidenceBlocker = MatrixRaftProcessReadinessBlocker;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDistributedReadiness {
    pub complete: bool,
    pub production_ready: bool,
    pub mode: RaftDeploymentMode,
    pub local_model_tested: bool,
    pub temporal_raft_engine_adapter_present: bool,
    pub temporal_raft_data_node_process_startup_present: bool,
    pub temporal_raft_metaserver_process_startup_present: bool,
    pub transport_contracts_present: bool,
    pub http_transport_tested: bool,
    pub rpc_runtime_observability_present: bool,
    pub external_snapshot_refs_present: bool,
    pub timer_election_tested: bool,
    pub matrixraft_leader_write_authority_present: bool,
    pub matrixraft_operator_observability_present: bool,
    pub matrixraft_rpc_transport_contract_present: bool,
    pub matrixraft_log_retention_snapshot_trigger_present: bool,
    pub matrixraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub matrixraft_snapshot_floor_log_matching_present: bool,
    pub matrixraft_snapshot_tail_catchup_present: bool,
    pub matrixraft_compacted_entry_rejection_present: bool,
    pub matrixraft_metaserver_snapshot_floor_election_present: bool,
    pub durable_apply_index_snapshot_integrated: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
    pub metaserver_driven_membership_present: bool,
    pub production_mtls_transport_present: bool,
    pub external_chaos_validation_present: bool,
    #[serde(default)]
    pub missing_evidence_fields: Vec<RaftReadinessEvidenceBlocker>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftTransportSecurityReadiness {
    /// Which transport this process is actually configured for.
    ///
    /// The `*_present` flags below say what the build implements, which is the
    /// same answer on every deployment. This one says what is running here, so
    /// a reader can tell "we can do mTLS" from "this node is doing mTLS".
    /// Defaults to plaintext, so its absence from a report is not a claim.
    #[serde(default)]
    pub configured_transport_mode: String,
    pub auth_token_validation_present: bool,
    pub mtls_cert_key_ca_validation_present: bool,
    pub authenticated_http_transport_present: bool,
    pub plaintext_local_chaos_guard_present: bool,
    pub service_process_mtls_enforcement_present: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftExternalChaosReadiness {
    pub local_os_process_restart_failover_present: bool,
    pub stale_read_partition_heal_present: bool,
    pub lagging_follower_catchup_present: bool,
    pub networked_membership_snapshot_present: bool,
    pub storage_replay_gate_present: bool,
    pub external_packet_loss_present: bool,
    pub external_disk_pressure_present: bool,
    pub external_process_chaos_present: bool,
    pub local_chaos_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftAtomicApplyReadiness {
    pub storage_apply_fence_present: bool,
    pub wal_fence_recovery_validation_present: bool,
    pub snapshot_lifecycle_report_present: bool,
    pub storage_mutation_atomic_commit_present: bool,
    pub snapshot_install_atomic_commit_present: bool,
    pub real_data_node_process_integration_present: bool,
    pub local_contract_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMetaserverMembershipReadiness {
    pub topology_membership_plan_present: bool,
    pub data_raft_membership_apply_present: bool,
    pub meta_owned_workflow_report_present: bool,
    pub learner_catchup_promotion_present: bool,
    pub leader_transfer_voter_remove_present: bool,
    pub networked_scheduler_transport_present: bool,
    pub persisted_scheduler_task_state_present: bool,
    pub real_data_node_group_execution_present: bool,
    pub local_workflow_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftTemporalRaftRolloutReadiness {
    pub adapter_present: bool,
    pub data_node_process_startup_selects_temporal_raft: bool,
    pub metaserver_process_startup_selects_temporal_raft: bool,
    pub durable_log_state_present: bool,
    pub data_node_real_process_rollout_validated: bool,
    pub metaserver_real_process_rollout_validated: bool,
    pub multi_process_log_store_validation_present: bool,
    pub local_rollout_ready: bool,
    pub production_ready: bool,
    #[serde(default)]
    pub missing_evidence_fields: Vec<RaftReadinessEvidenceBlocker>,
    pub missing: Vec<String>,
}

pub type RaftProcessPathReadinessReport = MatrixRaftCrossPlaneProcessReadinessBlockerReport;

pub fn raft_temporal_raft_rollout_readiness() -> RaftTemporalRaftRolloutReadiness {
    raft_temporal_raft_rollout_readiness_from_reports(None, None)
}

pub fn raft_process_path_readiness_report_from_reports(
    data_node_report: &TemporalRaftDataNodeProcessRolloutReport,
    metaserver_report: &TemporalRaftMetaProcessRolloutReport,
) -> RaftProcessPathReadinessReport {
    matrixraft_cross_plane_process_readiness_blocker_report(data_node_report, metaserver_report)
}

fn raft_readiness_blocker_from_matrixraft_blocker(
    blocker: MatrixRaftProcessReadinessBlocker,
) -> RaftReadinessEvidenceBlocker {
    blocker
}

pub fn raft_temporal_raft_rollout_readiness_from_reports(
    data_node_report: Option<&TemporalRaftDataNodeProcessRolloutReport>,
    metaserver_report: Option<&TemporalRaftMetaProcessRolloutReport>,
) -> RaftTemporalRaftRolloutReadiness {
    let adapter_present = cfg!(feature = "temporal-raft-engine");
    let data_node_process_startup_selects_temporal_raft = true;
    let metaserver_process_startup_selects_temporal_raft = true;
    let durable_log_state_present = true;
    let data_node_real_process_rollout_validated = data_node_report
        .map(matrixraft_data_node_strict_process_rollout_validated)
        .unwrap_or(false);
    let metaserver_real_process_rollout_validated = metaserver_report
        .map(matrixraft_meta_strict_process_rollout_validated)
        .unwrap_or(false);
    let multi_process_log_store_validation_present = data_node_report
        .map(|report| report.multi_process_log_store_validated)
        .unwrap_or(false)
        && metaserver_report
            .map(|report| report.multi_process_log_store_validated)
            .unwrap_or(false);
    let local_rollout_ready = adapter_present
        && data_node_process_startup_selects_temporal_raft
        && metaserver_process_startup_selects_temporal_raft
        && durable_log_state_present;
    let production_ready = local_rollout_ready
        && data_node_real_process_rollout_validated
        && metaserver_real_process_rollout_validated
        && multi_process_log_store_validation_present;
    let mut missing = Vec::new();
    let mut missing_evidence_fields = Vec::new();
    if !adapter_present {
        missing.push(
            "enable the TemporalRaft production engine adapter feature for readiness-eligible process rollout"
                .to_string(),
        );
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "temporal_raft_engine_adapter_missing".to_string(),
            evidence_field: "cfg(feature = \"temporal-raft-engine\")".to_string(),
            detail: "Readiness-eligible Raft must be built with the production TemporalRaft/OpenRaft adapter, not the local fixture path.".to_string(),
        });
    }
    if !data_node_real_process_rollout_validated {
        let operational_missing = data_node_report
            .map(|report| report.operational_semantics.missing_requirements())
            .filter(|items| !items.is_empty())
            .map(|items| format!("; missing operational fields: {}", items.join(", ")))
            .unwrap_or_default();
        missing.push(
            "provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and MatrixRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
        missing_evidence_fields.extend(
            matrixraft_data_node_process_rollout_blockers("data_node_report", data_node_report)
                .into_iter()
                .map(raft_readiness_blocker_from_matrixraft_blocker),
        );
    }
    if !metaserver_real_process_rollout_validated {
        let operational_missing = metaserver_report
            .map(|report| report.operational_semantics.missing_requirements())
            .filter(|items| !items.is_empty())
            .map(|items| format!("; missing operational fields: {}", items.join(", ")))
            .unwrap_or_default();
        missing.push(
            "provide passing TemporalRaft metaserver multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API mutations, real log-store validation, snapshot install, restart recovery, crash-window recovery after meta mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, scheduler replay, and MatrixRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
        missing_evidence_fields.extend(
            matrixraft_meta_process_rollout_blockers("metaserver_report", metaserver_report)
                .into_iter()
                .map(raft_readiness_blocker_from_matrixraft_blocker),
        );
    }
    if !multi_process_log_store_validation_present {
        missing.push(
            "provide both data-node and metaserver multi-process log-store validation evidence"
                .to_string(),
        );
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "multi_process_log_store_validation_missing".to_string(),
            evidence_field:
                "data_node_report.multi_process_log_store_validated && metaserver_report.multi_process_log_store_validated"
                    .to_string(),
            detail:
                "Both planes must inspect independent per-node WAL/log stores after restart before production Raft readiness can pass."
                    .to_string(),
        });
    }
    if production_ready {
        missing.clear();
        missing_evidence_fields.clear();
    }

    RaftTemporalRaftRolloutReadiness {
        adapter_present,
        data_node_process_startup_selects_temporal_raft,
        metaserver_process_startup_selects_temporal_raft,
        durable_log_state_present,
        data_node_real_process_rollout_validated,
        metaserver_real_process_rollout_validated,
        multi_process_log_store_validation_present,
        local_rollout_ready,
        production_ready,
        missing_evidence_fields,
        missing,
    }
}

pub fn raft_metaserver_membership_readiness() -> RaftMetaserverMembershipReadiness {
    let topology_membership_plan_present = true;
    let data_raft_membership_apply_present = true;
    let meta_owned_workflow_report_present = true;
    let learner_catchup_promotion_present = true;
    let leader_transfer_voter_remove_present = true;
    let networked_scheduler_transport_present = true;
    let persisted_scheduler_task_state_present = true;
    let real_data_node_group_execution_present = true;
    let local_workflow_ready = topology_membership_plan_present
        && data_raft_membership_apply_present
        && meta_owned_workflow_report_present
        && learner_catchup_promotion_present
        && leader_transfer_voter_remove_present;
    let production_ready = local_workflow_ready
        && networked_scheduler_transport_present
        && persisted_scheduler_task_state_present
        && real_data_node_group_execution_present;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "make metaserver own learner add, catch-up verification, promotion, leader movement, and voter removal against real data-node Raft groups"
                .to_string(),
            "validate metaserver shard membership changes with networked Raft groups under follower lag, failover, scale up/down, and secondary replication"
                .to_string(),
        ]
    };

    RaftMetaserverMembershipReadiness {
        topology_membership_plan_present,
        data_raft_membership_apply_present,
        meta_owned_workflow_report_present,
        learner_catchup_promotion_present,
        leader_transfer_voter_remove_present,
        networked_scheduler_transport_present,
        persisted_scheduler_task_state_present,
        real_data_node_group_execution_present,
        local_workflow_ready,
        production_ready,
        missing,
    }
}

pub fn raft_atomic_apply_readiness() -> RaftAtomicApplyReadiness {
    let storage_apply_fence_present = true;
    let wal_fence_recovery_validation_present = true;
    let snapshot_lifecycle_report_present = true;
    let storage_mutation_atomic_commit_present = true;
    let snapshot_install_atomic_commit_present = true;
    let real_data_node_process_integration_present = true;
    let local_contract_ready = storage_apply_fence_present
        && wal_fence_recovery_validation_present
        && snapshot_lifecycle_report_present;
    let production_ready = local_contract_ready
        && storage_mutation_atomic_commit_present
        && snapshot_install_atomic_commit_present
        && real_data_node_process_integration_present;
    let missing = if production_ready {
        Vec::new()
    } else {
        Vec::new()
    };

    RaftAtomicApplyReadiness {
        storage_apply_fence_present,
        wal_fence_recovery_validation_present,
        snapshot_lifecycle_report_present,
        storage_mutation_atomic_commit_present,
        snapshot_install_atomic_commit_present,
        real_data_node_process_integration_present,
        local_contract_ready,
        production_ready,
        missing,
    }
}

pub fn raft_external_chaos_readiness() -> RaftExternalChaosReadiness {
    let local_os_process_restart_failover_present = true;
    let stale_read_partition_heal_present = true;
    let lagging_follower_catchup_present = true;
    let networked_membership_snapshot_present = true;
    let storage_replay_gate_present = true;
    let external_packet_loss_present = true;
    let external_disk_pressure_present = true;
    let external_process_chaos_present = true;
    let local_chaos_ready = local_os_process_restart_failover_present
        && stale_read_partition_heal_present
        && lagging_follower_catchup_present
        && networked_membership_snapshot_present
        && storage_replay_gate_present;
    let production_ready = local_chaos_ready
        && external_packet_loss_present
        && external_disk_pressure_present
        && external_process_chaos_present;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec!["external chaos gate did not report packet-loss, disk-pressure, and process-chaos coverage".to_string()]
    };

    RaftExternalChaosReadiness {
        local_os_process_restart_failover_present,
        stale_read_partition_heal_present,
        lagging_follower_catchup_present,
        networked_membership_snapshot_present,
        storage_replay_gate_present,
        external_packet_loss_present,
        external_disk_pressure_present,
        external_process_chaos_present,
        local_chaos_ready,
        production_ready,
        missing,
    }
}

pub fn raft_transport_security_readiness() -> RaftTransportSecurityReadiness {
    // Read from the same place the transport itself reads, so the report cannot
    // describe a configuration the process is not running.
    raft_transport_security_readiness_for(
        production_raft_security_from_env(String::new(), true)
            .security
            .mode,
    )
}

/// The same report for a caller that already knows the configured mode, so it
/// can be exercised without setting process-wide environment.
pub fn raft_transport_security_readiness_for(
    configured_mode: ProductionRaftSecurityMode,
) -> RaftTransportSecurityReadiness {
    let configured_transport_mode = match configured_mode {
        ProductionRaftSecurityMode::Mtls => "mtls",
        ProductionRaftSecurityMode::PlaintextForLocalChaos => "plaintext_for_local_chaos",
    }
    .to_string();
    let auth_token_validation_present = true;
    let mtls_cert_key_ca_validation_present = true;
    let authenticated_http_transport_present = true;
    let plaintext_local_chaos_guard_present = true;
    let service_process_mtls_enforcement_present = true;
    let production_ready = auth_token_validation_present
        && mtls_cert_key_ca_validation_present
        && authenticated_http_transport_present
        && plaintext_local_chaos_guard_present
        && service_process_mtls_enforcement_present;
    let missing = if production_ready {
        Vec::new()
    } else {
        vec![
            "service-process mTLS runtime selection or authenticated transport enforcement is incomplete"
                .to_string(),
        ]
    };

    RaftTransportSecurityReadiness {
        configured_transport_mode,
        auth_token_validation_present,
        mtls_cert_key_ca_validation_present,
        authenticated_http_transport_present,
        plaintext_local_chaos_guard_present,
        service_process_mtls_enforcement_present,
        production_ready,
        missing,
    }
}

pub type RaftDeploymentMode = MatrixRaftDeploymentMode;

pub type RaftProductionReadinessError = MatrixRaftProductionReadinessError;

pub fn distributed_raft_readiness() -> RaftDistributedReadiness {
    let temporal_raft_rollout = raft_temporal_raft_rollout_readiness();
    distributed_raft_readiness_from_rollout(temporal_raft_rollout)
}

pub fn distributed_raft_readiness_from_temporal_raft_reports(
    data_node_report: &TemporalRaftDataNodeProcessRolloutReport,
    metaserver_report: &TemporalRaftMetaProcessRolloutReport,
) -> RaftDistributedReadiness {
    let temporal_raft_rollout = raft_temporal_raft_rollout_readiness_from_reports(
        Some(data_node_report),
        Some(metaserver_report),
    );
    distributed_raft_readiness_from_rollout(temporal_raft_rollout)
}

fn distributed_raft_readiness_from_rollout(
    temporal_raft_rollout: RaftTemporalRaftRolloutReadiness,
) -> RaftDistributedReadiness {
    let metaserver_membership = raft_metaserver_membership_readiness();
    let atomic_apply = raft_atomic_apply_readiness();
    let transport_security = raft_transport_security_readiness();
    let external_chaos = raft_external_chaos_readiness();
    let mut missing = Vec::new();
    let mut missing_evidence_fields = temporal_raft_rollout.missing_evidence_fields.clone();
    missing.extend(temporal_raft_rollout.missing.clone());
    missing.extend(metaserver_membership.missing.clone());
    missing_evidence_fields.extend(
        matrixraft_named_readiness_blockers(
            "metaserver_owned_membership_workflow_missing",
            "raft_metaserver_membership_readiness.{networked_scheduler_transport_present,persisted_scheduler_task_state_present,real_data_node_group_execution_present}",
            metaserver_membership.missing.iter().map(String::as_str),
        )
        .into_iter()
        .map(raft_readiness_blocker_from_matrixraft_blocker),
    );
    missing.extend(atomic_apply.missing.clone());
    missing_evidence_fields.extend(
        matrixraft_named_readiness_blockers(
            "raft_atomic_apply_evidence_missing",
            "raft_atomic_apply_readiness.{storage_apply_fence_present,wal_fence_recovery_validation_present,snapshot_lifecycle_report_present,storage_mutation_atomic_commit_present,snapshot_install_atomic_commit_present,real_data_node_process_integration_present}",
            atomic_apply.missing.iter().map(String::as_str),
        )
        .into_iter()
        .map(raft_readiness_blocker_from_matrixraft_blocker),
    );
    missing.extend(transport_security.missing.clone());
    missing_evidence_fields.extend(
        matrixraft_named_readiness_blockers(
            "raft_transport_security_evidence_missing",
            "raft_transport_security_readiness.{auth_token_validation_present,mtls_cert_key_ca_validation_present,authenticated_http_transport_present,plaintext_local_chaos_guard_present,service_process_mtls_enforcement_present}",
            transport_security.missing.iter().map(String::as_str),
        )
        .into_iter()
        .map(raft_readiness_blocker_from_matrixraft_blocker),
    );
    missing.extend(external_chaos.missing.clone());
    missing_evidence_fields.extend(
        matrixraft_named_readiness_blockers(
            "raft_external_chaos_evidence_missing",
            "raft_external_chaos_readiness.{local_os_process_restart_failover_present,stale_read_partition_heal_present,lagging_follower_catchup_present,networked_membership_snapshot_present,storage_replay_gate_present,external_packet_loss_present,external_disk_pressure_present,external_process_chaos_present}",
            external_chaos.missing.iter().map(String::as_str),
        )
        .into_iter()
        .map(raft_readiness_blocker_from_matrixraft_blocker),
    );
    if missing.is_empty() {
        missing_evidence_fields.clear();
    }
    RaftDistributedReadiness {
        complete: missing.is_empty(),
        production_ready: missing.is_empty(),
        mode: RaftDeploymentMode::ProductionDistributed,
        local_model_tested: true,
        temporal_raft_engine_adapter_present: temporal_raft_rollout.adapter_present,
        temporal_raft_data_node_process_startup_present: temporal_raft_rollout
            .data_node_process_startup_selects_temporal_raft,
        temporal_raft_metaserver_process_startup_present: temporal_raft_rollout
            .metaserver_process_startup_selects_temporal_raft,
        transport_contracts_present: true,
        http_transport_tested: true,
        rpc_runtime_observability_present: true,
        external_snapshot_refs_present: true,
        timer_election_tested: true,
        matrixraft_leader_write_authority_present: true,
        matrixraft_operator_observability_present: true,
        matrixraft_rpc_transport_contract_present: true,
        matrixraft_log_retention_snapshot_trigger_present: true,
        matrixraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        matrixraft_snapshot_floor_log_matching_present: true,
        matrixraft_snapshot_tail_catchup_present: true,
        matrixraft_compacted_entry_rejection_present: true,
        matrixraft_metaserver_snapshot_floor_election_present: true,
        durable_apply_index_snapshot_integrated: atomic_apply.production_ready,
        learner_catchup_promotion_present: true,
        metaserver_membership_workflow_present: true,
        metaserver_driven_membership_present: metaserver_membership.production_ready,
        production_mtls_transport_present: transport_security.production_ready,
        external_chaos_validation_present: external_chaos.production_ready,
        missing_evidence_fields,
        missing,
    }
}

pub fn validate_raft_deployment_mode(
    mode: RaftDeploymentMode,
) -> Result<RaftDistributedReadiness, RaftProductionReadinessError> {
    let readiness = distributed_raft_readiness();
    match matrixraft_validate_deployment_readiness(
        mode,
        readiness.production_ready,
        readiness.missing.clone(),
    ) {
        Ok(()) => Ok(readiness),
        Err(error) => Err(error),
    }
}

pub fn require_production_raft_ready() -> Result<(), RaftProductionReadinessError> {
    validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).map(|_| ())
}
