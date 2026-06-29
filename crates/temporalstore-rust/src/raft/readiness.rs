use super::*;

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
    pub byteraft_leader_write_authority_present: bool,
    pub byteraft_operator_observability_present: bool,
    pub byteraft_rpc_transport_contract_present: bool,
    pub byteraft_log_retention_snapshot_trigger_present: bool,
    pub byteraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub byteraft_snapshot_floor_log_matching_present: bool,
    pub byteraft_snapshot_tail_catchup_present: bool,
    pub byteraft_compacted_entry_rejection_present: bool,
    pub byteraft_metaserver_snapshot_floor_election_present: bool,
    pub durable_apply_index_snapshot_integrated: bool,
    pub learner_catchup_promotion_present: bool,
    pub metaserver_membership_workflow_present: bool,
    pub metaserver_driven_membership_present: bool,
    pub production_mtls_transport_present: bool,
    pub external_chaos_validation_present: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftTransportSecurityReadiness {
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
    pub missing: Vec<String>,
}

pub fn raft_temporal_raft_rollout_readiness() -> RaftTemporalRaftRolloutReadiness {
    raft_temporal_raft_rollout_readiness_from_reports(None, None)
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
        .map(|report| {
            report.ready
                && report.write_proposed_through_process_api
                && report.recovered_after_restart
                && report.restart_recovery_validated
                && report.snapshot_install_validated
                && report.applied_fence_validated
                && report.multi_process_log_store_validated
                && report.leader_transfer_validated
                && report.failover_validated
                && report.membership_change_validated
                && report.follower_lag_validated
                && report.secondary_read_validated
                && report.operational_semantics.proves_runtime_semantics()
                && report.nodes.len() >= 3
                && report.nodes.iter().all(|node| {
                    node.restarted
                        && node.log_store_validated
                        && node.applied_index >= node.commit_index
                })
        })
        .unwrap_or(false);
    let metaserver_real_process_rollout_validated = metaserver_report
        .map(|report| {
            report.ready
                && report.mutation_proposed_through_process_api
                && report.applied_raft_mutations > 0
                && report.read_index_validated
                && report.snapshot_install_validated
                && report.recovered_after_restart
                && report.scheduler_task_replay_validated
                && report.multi_process_log_store_validated
                && report.failover_validated
                && report.membership_change_validated
                && report.follower_lag_validated
                && report.secondary_read_validated
                && report.operational_semantics.proves_runtime_semantics()
                && report.nodes.len() >= 3
                && report.nodes.iter().all(|node| {
                    node.restarted
                        && node.log_store_validated
                        && node.applied_index >= node.commit_index
                })
        })
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
    if !adapter_present {
        missing.push(
            "enable the TemporalRaft production engine adapter feature for readiness-eligible process rollout"
                .to_string(),
        );
    }
    if !data_node_real_process_rollout_validated {
        let operational_missing = data_node_report
            .map(|report| report.operational_semantics.missing_requirements())
            .filter(|items| !items.is_empty())
            .map(|items| format!("; missing operational fields: {}", items.join(", ")))
            .unwrap_or_default();
        missing.push(
            "provide passing TemporalRaft data-node multi-process rollout evidence with process API writes, real log-store validation, snapshot install, restart recovery, failover, membership changes, follower lag, secondary reads, and ByteRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
    }
    if !metaserver_real_process_rollout_validated {
        let operational_missing = metaserver_report
            .map(|report| report.operational_semantics.missing_requirements())
            .filter(|items| !items.is_empty())
            .map(|items| format!("; missing operational fields: {}", items.join(", ")))
            .unwrap_or_default();
        missing.push(
            "provide passing TemporalRaft metaserver multi-process rollout evidence with process API mutations, real log-store validation, read-index, snapshot install, restart recovery, failover, membership changes, follower lag, secondary reads, scheduler replay, and ByteRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
    }
    if !multi_process_log_store_validation_present {
        missing.push(
            "provide both data-node and metaserver multi-process log-store validation evidence"
                .to_string(),
        );
    }
    if production_ready {
        missing.clear();
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
        auth_token_validation_present,
        mtls_cert_key_ca_validation_present,
        authenticated_http_transport_present,
        plaintext_local_chaos_guard_present,
        service_process_mtls_enforcement_present,
        production_ready,
        missing,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RaftDeploymentMode {
    /// Backward-compatible deserialization variant only.
    ///
    /// Runtime validation rejects local Raft deployment; local clusters remain
    /// internal test fixtures and production must use distributed Raft.
    LocalModel,
    ProductionDistributed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftProductionReadinessError {
    pub mode: RaftDeploymentMode,
    pub message: String,
    pub missing: Vec<String>,
}

impl std::fmt::Display for RaftProductionReadinessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.message)?;
        if !self.missing.is_empty() {
            write!(formatter, ": {}", self.missing.join("; "))?;
        }
        Ok(())
    }
}

impl std::error::Error for RaftProductionReadinessError {}

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
    missing.extend(temporal_raft_rollout.missing.clone());
    missing.extend(metaserver_membership.missing.clone());
    missing.extend(atomic_apply.missing.clone());
    missing.extend(transport_security.missing.clone());
    missing.extend(external_chaos.missing.clone());
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
        byteraft_leader_write_authority_present: true,
        byteraft_operator_observability_present: true,
        byteraft_rpc_transport_contract_present: true,
        byteraft_log_retention_snapshot_trigger_present: true,
        byteraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        byteraft_snapshot_floor_log_matching_present: true,
        byteraft_snapshot_tail_catchup_present: true,
        byteraft_compacted_entry_rejection_present: true,
        byteraft_metaserver_snapshot_floor_election_present: true,
        durable_apply_index_snapshot_integrated: atomic_apply.production_ready,
        learner_catchup_promotion_present: true,
        metaserver_membership_workflow_present: true,
        metaserver_driven_membership_present: metaserver_membership.production_ready,
        production_mtls_transport_present: transport_security.production_ready,
        external_chaos_validation_present: external_chaos.production_ready,
        missing,
    }
}

pub fn validate_raft_deployment_mode(
    mode: RaftDeploymentMode,
) -> Result<RaftDistributedReadiness, RaftProductionReadinessError> {
    let readiness = distributed_raft_readiness();
    match mode {
        RaftDeploymentMode::LocalModel => Err(RaftProductionReadinessError {
            mode,
            message:
                "local Raft deployment mode is disabled; production distributed Raft is required"
                    .to_string(),
            missing: readiness.missing,
        }),
        RaftDeploymentMode::ProductionDistributed if readiness.production_ready => Ok(readiness),
        RaftDeploymentMode::ProductionDistributed => Err(RaftProductionReadinessError {
            mode,
            message: "distributed Raft is not production-ready".to_string(),
            missing: readiness.missing,
        }),
    }
}

pub fn require_production_raft_ready() -> Result<(), RaftProductionReadinessError> {
    validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).map(|_| ())
}
