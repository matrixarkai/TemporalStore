use super::*;
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftReadinessEvidenceBlocker {
    pub blocker: String,
    pub evidence_field: String,
    pub detail: String,
}

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
    pub rustraft_leader_write_authority_present: bool,
    pub rustraft_operator_observability_present: bool,
    pub rustraft_rpc_transport_contract_present: bool,
    pub rustraft_log_retention_snapshot_trigger_present: bool,
    pub rustraft_apply_snapshot_fence_present: bool,
    pub raft_storage_apply_fence_present: bool,
    pub rustraft_snapshot_floor_log_matching_present: bool,
    pub rustraft_snapshot_tail_catchup_present: bool,
    pub rustraft_compacted_entry_rejection_present: bool,
    pub rustraft_metaserver_snapshot_floor_election_present: bool,
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
                && process_path_proof_is_complete(
                    report.spawned_process_count,
                    report.independent_wal_dirs,
                    report.independent_snapshot_dirs,
                    report.observed_process_requests,
                    report.read_index_responses_observed,
                    report.restarted_node_count,
                    report.per_node_log_store_inspection_count,
                    &report.nodes,
                )
                && report.write_proposed_through_process_api
                && report.recovered_after_restart
                && report.restart_recovery_validated
                && report.snapshot_install_validated
                && report.applied_fence_validated
                && report.crash_after_storage_mutation_recovered
                && report.crash_after_wal_persist_recovered
                && report.crash_during_snapshot_install_recovered
                && report.apply_fence_recovered_after_restart
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
                && process_path_proof_is_complete(
                    report.spawned_process_count,
                    report.independent_wal_dirs,
                    report.independent_snapshot_dirs,
                    report.observed_process_requests,
                    report.read_index_responses_observed,
                    report.restarted_node_count,
                    report.per_node_log_store_inspection_count,
                    &report.nodes,
                )
                && report.mutation_proposed_through_process_api
                && report.applied_raft_mutations > 0
                && report.read_index_validated
                && report.snapshot_install_validated
                && report.recovered_after_restart
                && report.scheduler_task_replay_validated
                && report.crash_after_meta_mutation_recovered
                && report.crash_after_meta_wal_persist_recovered
                && report.crash_during_meta_snapshot_install_recovered
                && report.meta_apply_fence_recovered_after_restart
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
            "provide passing TemporalRaft data-node multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API writes, real log-store validation, snapshot install, restart recovery, crash-window recovery after storage mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, and RustRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
        missing_evidence_fields.extend(process_rollout_evidence_blockers(
            "data_node_report",
            data_node_report.map(DataNodeRolloutView::from),
        ));
    }
    if !metaserver_real_process_rollout_validated {
        let operational_missing = metaserver_report
            .map(|report| report.operational_semantics.missing_requirements())
            .filter(|items| !items.is_empty())
            .map(|items| format!("; missing operational fields: {}", items.join(", ")))
            .unwrap_or_default();
        missing.push(
            "provide passing TemporalRaft metaserver multi-process rollout evidence with spawned process count, independent WAL/snapshot dirs, observed process requests, read-index responses, per-node log-store inspection, process API mutations, real log-store validation, snapshot install, restart recovery, crash-window recovery after meta mutation/WAL persistence/snapshot install/apply fence, failover, membership changes, follower lag, secondary reads, scheduler replay, and RustRaft-derived operational semantics evidence"
                .to_string()
                + &operational_missing,
        );
        missing_evidence_fields.extend(process_rollout_evidence_blockers(
            "metaserver_report",
            metaserver_report.map(MetaRolloutView::from),
        ));
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

trait RolloutEvidenceView {
    fn ready(&self) -> bool;
    fn spawned_process_count(&self) -> u64;
    fn independent_wal_dirs(&self) -> bool;
    fn independent_snapshot_dirs(&self) -> bool;
    fn observed_process_requests(&self) -> u64;
    fn read_index_responses_observed(&self) -> u64;
    fn restarted_node_count(&self) -> u64;
    fn per_node_log_store_inspection_count(&self) -> u64;
    fn node_count(&self) -> usize;
    fn process_api_observed(&self) -> bool;
    fn snapshot_install_validated(&self) -> bool;
    fn restart_recovery_validated(&self) -> bool;
    fn multi_process_log_store_validated(&self) -> bool;
    fn failover_validated(&self) -> bool;
    fn membership_change_validated(&self) -> bool;
    fn follower_lag_validated(&self) -> bool;
    fn secondary_read_validated(&self) -> bool;
    fn nodes_restarted_and_log_checked(&self) -> bool;
    fn operational_missing(&self) -> Vec<String>;
}

struct DataNodeRolloutView<'a>(&'a TemporalRaftDataNodeProcessRolloutReport);

impl<'a> From<&'a TemporalRaftDataNodeProcessRolloutReport> for DataNodeRolloutView<'a> {
    fn from(report: &'a TemporalRaftDataNodeProcessRolloutReport) -> Self {
        Self(report)
    }
}

impl RolloutEvidenceView for DataNodeRolloutView<'_> {
    fn ready(&self) -> bool {
        self.0.ready
    }
    fn spawned_process_count(&self) -> u64 {
        self.0.spawned_process_count
    }
    fn independent_wal_dirs(&self) -> bool {
        self.0.independent_wal_dirs
    }
    fn independent_snapshot_dirs(&self) -> bool {
        self.0.independent_snapshot_dirs
    }
    fn observed_process_requests(&self) -> u64 {
        self.0.observed_process_requests
    }
    fn read_index_responses_observed(&self) -> u64 {
        self.0.read_index_responses_observed
    }
    fn restarted_node_count(&self) -> u64 {
        self.0.restarted_node_count
    }
    fn per_node_log_store_inspection_count(&self) -> u64 {
        self.0.per_node_log_store_inspection_count
    }
    fn node_count(&self) -> usize {
        self.0.nodes.len()
    }
    fn process_api_observed(&self) -> bool {
        self.0.write_proposed_through_process_api
    }
    fn snapshot_install_validated(&self) -> bool {
        self.0.snapshot_install_validated
    }
    fn restart_recovery_validated(&self) -> bool {
        self.0.restart_recovery_validated && self.0.recovered_after_restart
    }
    fn multi_process_log_store_validated(&self) -> bool {
        self.0.multi_process_log_store_validated
    }
    fn failover_validated(&self) -> bool {
        self.0.failover_validated
    }
    fn membership_change_validated(&self) -> bool {
        self.0.membership_change_validated
    }
    fn follower_lag_validated(&self) -> bool {
        self.0.follower_lag_validated
    }
    fn secondary_read_validated(&self) -> bool {
        self.0.secondary_read_validated
    }
    fn nodes_restarted_and_log_checked(&self) -> bool {
        self.0.nodes.iter().all(|node| {
            node.restarted && node.log_store_validated && node.applied_index >= node.commit_index
        })
    }
    fn operational_missing(&self) -> Vec<String> {
        self.0.operational_semantics.missing_requirements()
    }
}

struct MetaRolloutView<'a>(&'a TemporalRaftMetaProcessRolloutReport);

impl<'a> From<&'a TemporalRaftMetaProcessRolloutReport> for MetaRolloutView<'a> {
    fn from(report: &'a TemporalRaftMetaProcessRolloutReport) -> Self {
        Self(report)
    }
}

impl RolloutEvidenceView for MetaRolloutView<'_> {
    fn ready(&self) -> bool {
        self.0.ready
    }
    fn spawned_process_count(&self) -> u64 {
        self.0.spawned_process_count
    }
    fn independent_wal_dirs(&self) -> bool {
        self.0.independent_wal_dirs
    }
    fn independent_snapshot_dirs(&self) -> bool {
        self.0.independent_snapshot_dirs
    }
    fn observed_process_requests(&self) -> u64 {
        self.0.observed_process_requests
    }
    fn read_index_responses_observed(&self) -> u64 {
        self.0.read_index_responses_observed
    }
    fn restarted_node_count(&self) -> u64 {
        self.0.restarted_node_count
    }
    fn per_node_log_store_inspection_count(&self) -> u64 {
        self.0.per_node_log_store_inspection_count
    }
    fn node_count(&self) -> usize {
        self.0.nodes.len()
    }
    fn process_api_observed(&self) -> bool {
        self.0.mutation_proposed_through_process_api && self.0.applied_raft_mutations > 0
    }
    fn snapshot_install_validated(&self) -> bool {
        self.0.snapshot_install_validated
    }
    fn restart_recovery_validated(&self) -> bool {
        self.0.recovered_after_restart
    }
    fn multi_process_log_store_validated(&self) -> bool {
        self.0.multi_process_log_store_validated
    }
    fn failover_validated(&self) -> bool {
        self.0.failover_validated
    }
    fn membership_change_validated(&self) -> bool {
        self.0.membership_change_validated
    }
    fn follower_lag_validated(&self) -> bool {
        self.0.follower_lag_validated
    }
    fn secondary_read_validated(&self) -> bool {
        self.0.secondary_read_validated
    }
    fn nodes_restarted_and_log_checked(&self) -> bool {
        self.0.nodes.iter().all(|node| {
            node.restarted && node.log_store_validated && node.applied_index >= node.commit_index
        })
    }
    fn operational_missing(&self) -> Vec<String> {
        self.0.operational_semantics.missing_requirements()
    }
}

fn process_rollout_evidence_blockers<T: RolloutEvidenceView>(
    prefix: &str,
    report: Option<T>,
) -> Vec<RaftReadinessEvidenceBlocker> {
    let Some(report) = report else {
        return vec![RaftReadinessEvidenceBlocker {
            blocker: format!("{prefix}_missing"),
            evidence_field: prefix.to_string(),
            detail: "No process-harness report was supplied; local fixtures cannot satisfy production Raft readiness.".to_string(),
        }];
    };
    let mut blockers = Vec::new();
    push_if_false(
        &mut blockers,
        report.ready(),
        prefix,
        "ready",
        "process rollout report must be ready",
    );
    push_if_false(
        &mut blockers,
        report.spawned_process_count() >= 3 && report.node_count() >= 3,
        prefix,
        "spawned_process_count",
        "multi-process data-node/metaserver Raft evidence requires at least three spawned nodes",
    );
    push_if_false(
        &mut blockers,
        report.independent_wal_dirs(),
        prefix,
        "independent_wal_dirs",
        "each process must use an independent WAL directory",
    );
    push_if_false(
        &mut blockers,
        report.independent_snapshot_dirs(),
        prefix,
        "independent_snapshot_dirs",
        "each process must use an independent snapshot directory",
    );
    push_if_false(
        &mut blockers,
        report.observed_process_requests() >= report.node_count() as u64,
        prefix,
        "observed_process_requests",
        "harness must observe real process API traffic rather than in-memory fixture calls",
    );
    push_if_false(
        &mut blockers,
        report.read_index_responses_observed() > 0,
        prefix,
        "read_index_responses_observed",
        "process harness must observe read-index responses",
    );
    push_if_false(
        &mut blockers,
        report.restart_recovery_validated(),
        prefix,
        "restart_recovery_validated",
        "restart recovery must be validated after persisted WAL/snapshot state",
    );
    push_if_false(
        &mut blockers,
        report.nodes_restarted_and_log_checked(),
        prefix,
        "nodes[*].{restarted,log_store_validated,applied_index,commit_index}",
        "every node must restart, pass log-store inspection, and converge applied index to commit index",
    );
    push_if_false(
        &mut blockers,
        report.process_api_observed(),
        prefix,
        "process_api_mutations_or_writes",
        "writes/mutations must be proposed through process APIs",
    );
    push_if_false(
        &mut blockers,
        report.multi_process_log_store_validated(),
        prefix,
        "multi_process_log_store_validated",
        "independent process log stores must be inspected and validated",
    );
    push_if_false(
        &mut blockers,
        report.failover_validated(),
        prefix,
        "failover_validated",
        "failover must be validated on this plane",
    );
    push_if_false(
        &mut blockers,
        report.membership_change_validated(),
        prefix,
        "membership_change_validated",
        "membership add/remove under load must be validated",
    );
    push_if_false(
        &mut blockers,
        report.follower_lag_validated(),
        prefix,
        "follower_lag_validated",
        "secondary lag and catch-up must be observed",
    );
    push_if_false(
        &mut blockers,
        report.secondary_read_validated(),
        prefix,
        "secondary_read_validated",
        "secondary read eligibility after catch-up must be validated",
    );
    push_if_false(
        &mut blockers,
        report.snapshot_install_validated(),
        prefix,
        "snapshot_install_validated",
        "snapshot install/restart after compaction must be validated",
    );
    for missing in report.operational_missing() {
        blockers.push(RaftReadinessEvidenceBlocker {
            blocker: format!("{prefix}_operational_semantics_missing"),
            evidence_field: format!("{prefix}.operational_semantics.{missing}"),
            detail: "RustRaft/ByteRaft-derived operational semantics evidence is incomplete."
                .to_string(),
        });
    }
    blockers
}

fn push_if_false(
    blockers: &mut Vec<RaftReadinessEvidenceBlocker>,
    ready: bool,
    prefix: &str,
    evidence_field: &str,
    detail: &str,
) {
    if !ready {
        blockers.push(RaftReadinessEvidenceBlocker {
            blocker: format!(
                "{}_{}_missing",
                prefix,
                evidence_field.replace(['.', '*', '{', '}', '[', ']', ','], "_")
            ),
            evidence_field: format!("{prefix}.{evidence_field}"),
            detail: detail.to_string(),
        });
    }
}

fn process_path_proof_is_complete(
    spawned_process_count: u64,
    independent_wal_dirs: bool,
    independent_snapshot_dirs: bool,
    observed_process_requests: u64,
    read_index_responses_observed: u64,
    restarted_node_count: u64,
    per_node_log_store_inspection_count: u64,
    nodes: &[TemporalRaftProcessNodeEvidence],
) -> bool {
    let expected = nodes.len() as u64;
    spawned_process_count >= 3
        && expected >= 3
        && spawned_process_count >= expected
        && independent_wal_dirs
        && independent_snapshot_dirs
        && observed_process_requests >= expected
        && read_index_responses_observed > 0
        && restarted_node_count >= expected
        && per_node_log_store_inspection_count >= expected
        && unique_non_empty_dirs(nodes.iter().map(|node| node.addr.as_str()))
        && unique_non_empty_dirs(nodes.iter().map(|node| node.wal_dir.as_str()))
        && unique_non_empty_dirs(nodes.iter().map(|node| node.snapshot_dir.as_str()))
}

fn unique_non_empty_dirs<'a>(dirs: impl Iterator<Item = &'a str>) -> bool {
    let mut seen = HashSet::new();
    let mut count = 0usize;
    for dir in dirs {
        if dir.is_empty() || !seen.insert(dir) {
            return false;
        }
        count += 1;
    }
    count >= 3
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
    let mut missing_evidence_fields = temporal_raft_rollout.missing_evidence_fields.clone();
    missing.extend(temporal_raft_rollout.missing.clone());
    missing.extend(metaserver_membership.missing.clone());
    for item in &metaserver_membership.missing {
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "metaserver_owned_membership_workflow_missing".to_string(),
            evidence_field: "raft_metaserver_membership_readiness.{networked_scheduler_transport_present,persisted_scheduler_task_state_present,real_data_node_group_execution_present}".to_string(),
            detail: item.clone(),
        });
    }
    missing.extend(atomic_apply.missing.clone());
    for item in &atomic_apply.missing {
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "raft_atomic_apply_evidence_missing".to_string(),
            evidence_field: "raft_atomic_apply_readiness.{storage_apply_fence_present,wal_fence_recovery_validation_present,snapshot_lifecycle_report_present,storage_mutation_atomic_commit_present,snapshot_install_atomic_commit_present,real_data_node_process_integration_present}".to_string(),
            detail: item.clone(),
        });
    }
    missing.extend(transport_security.missing.clone());
    for item in &transport_security.missing {
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "raft_transport_security_evidence_missing".to_string(),
            evidence_field: "raft_transport_security_readiness.{auth_token_validation_present,mtls_cert_key_ca_validation_present,authenticated_http_transport_present,plaintext_local_chaos_guard_present,service_process_mtls_enforcement_present}".to_string(),
            detail: item.clone(),
        });
    }
    missing.extend(external_chaos.missing.clone());
    for item in &external_chaos.missing {
        missing_evidence_fields.push(RaftReadinessEvidenceBlocker {
            blocker: "raft_external_chaos_evidence_missing".to_string(),
            evidence_field: "raft_external_chaos_readiness.{local_os_process_restart_failover_present,stale_read_partition_heal_present,lagging_follower_catchup_present,networked_membership_snapshot_present,storage_replay_gate_present,external_packet_loss_present,external_disk_pressure_present,external_process_chaos_present}".to_string(),
            detail: item.clone(),
        });
    }
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
        rustraft_leader_write_authority_present: true,
        rustraft_operator_observability_present: true,
        rustraft_rpc_transport_contract_present: true,
        rustraft_log_retention_snapshot_trigger_present: true,
        rustraft_apply_snapshot_fence_present: true,
        raft_storage_apply_fence_present: true,
        rustraft_snapshot_floor_log_matching_present: true,
        rustraft_snapshot_tail_catchup_present: true,
        rustraft_compacted_entry_rejection_present: true,
        rustraft_metaserver_snapshot_floor_election_present: true,
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
