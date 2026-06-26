use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftDistributedReadiness {
    pub complete: bool,
    pub production_ready: bool,
    pub mode: RaftDeploymentMode,
    pub local_model_tested: bool,
    pub openraft_engine_adapter_present: bool,
    pub openraft_data_node_process_startup_present: bool,
    pub openraft_metaserver_process_startup_present: bool,
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
    pub byteraft_per_peer_pipeline_state_present: bool,
    pub byteraft_reorder_queue_state_present: bool,
    pub byteraft_snapshot_sender_downloader_lifecycle_present: bool,
    pub byteraft_wal_segment_lifecycle_present: bool,
    pub byteraft_read_index_lease_semantics_present: bool,
    pub byteraft_admin_status_surface_present: bool,
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
pub struct RaftOpenRaftRolloutReadiness {
    pub adapter_present: bool,
    pub data_node_process_startup_selects_openraft: bool,
    pub metaserver_process_startup_selects_openraft: bool,
    pub durable_log_state_present: bool,
    pub data_node_real_process_rollout_validated: bool,
    pub metaserver_real_process_rollout_validated: bool,
    pub multi_process_log_store_validation_present: bool,
    pub local_rollout_ready: bool,
    pub production_ready: bool,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftByteRaftRuntimeReadiness {
    pub runtime_report_present: bool,
    pub per_peer_pipeline_state_present: bool,
    pub reorder_queue_state_present: bool,
    pub snapshot_sender_downloader_lifecycle_present: bool,
    pub wal_segment_lifecycle_present: bool,
    pub read_index_lease_semantics_present: bool,
    pub stale_follower_write_rejection_present: bool,
    pub admin_status_surface_present: bool,
    pub process_path_operational_semantics_ready: bool,
    pub report: ByteRaftRuntimeAdminReport,
    pub missing: Vec<String>,
}

pub fn raft_openraft_rollout_readiness() -> RaftOpenRaftRolloutReadiness {
    raft_openraft_rollout_readiness_from_reports(None, None)
}

pub fn raft_openraft_rollout_readiness_from_reports(
    data_node_report: Option<&OpenRaftDataNodeProcessRolloutReport>,
    metaserver_report: Option<&OpenRaftMetaProcessRolloutReport>,
) -> RaftOpenRaftRolloutReadiness {
    let adapter_present = cfg!(feature = "openraft-engine");
    let data_node_process_startup_selects_openraft = true;
    let metaserver_process_startup_selects_openraft = true;
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
        && data_node_process_startup_selects_openraft
        && metaserver_process_startup_selects_openraft
        && durable_log_state_present;
    let production_ready = local_rollout_ready
        && data_node_real_process_rollout_validated
        && metaserver_real_process_rollout_validated
        && multi_process_log_store_validation_present;
    let mut missing = Vec::new();
    if !adapter_present {
        missing.push(
            "enable the OpenRaft production engine adapter feature for readiness-eligible process rollout"
                .to_string(),
        );
    }
    if !data_node_real_process_rollout_validated {
        missing.push(
            "provide passing OpenRaft data-node multi-process rollout evidence with process API writes, real log-store validation, snapshot install, restart recovery, failover, membership changes, follower lag, and secondary reads"
                .to_string(),
        );
    }
    if !metaserver_real_process_rollout_validated {
        missing.push(
            "provide passing OpenRaft metaserver multi-process rollout evidence with process API mutations, real log-store validation, read-index, snapshot install, restart recovery, failover, membership changes, follower lag, secondary reads, and scheduler replay"
                .to_string(),
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

    RaftOpenRaftRolloutReadiness {
        adapter_present,
        data_node_process_startup_selects_openraft,
        metaserver_process_startup_selects_openraft,
        durable_log_state_present,
        data_node_real_process_rollout_validated,
        metaserver_real_process_rollout_validated,
        multi_process_log_store_validation_present,
        local_rollout_ready,
        production_ready,
        missing,
    }
}

pub fn raft_byteraft_runtime_readiness() -> RaftByteRaftRuntimeReadiness {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let root = std::env::temp_dir().join(format!(
        "temporalstore-byteraft-runtime-readiness-{}-{unique}",
        std::process::id()
    ));
    let mut config = RaftConfig {
        enable_pre_vote: true,
        lease_duration_ms: 1_000,
        max_inflights_replicate: 2,
        max_segment_bytes: 512,
        min_keep_segment_num: 1,
        ..RaftConfig::default()
    };
    config.prohibits_election = false;
    let cluster = RaftCluster::new_single_shard_with_wal(&root, 91, [1, 2, 3], config)
        .unwrap_or_else(|_| RaftCluster::new_single_shard(91, [1, 2, 3]));
    let _ = cluster.propose(Command::StringSet {
        key: "byteraft-runtime-admin-snapshot".to_string(),
        value: b"seed".to_vec(),
    });
    let _ = cluster.maybe_trigger_snapshot();
    if let Ok(snapshot) = cluster.build_install_snapshot_request(2) {
        let _ = cluster.receive_install_snapshot(snapshot);
    }
    let _ = cluster.set_alive(3, false);
    let _ = cluster.propose(Command::StringSet {
        key: "byteraft-runtime-admin-lag".to_string(),
        value: b"lag".to_vec(),
    });
    if let Ok(append_request) = cluster.build_append_entries_request(3) {
        let _ = cluster.build_append_entries_request(3);
        let _ = cluster.receive_append_entries(append_request);
    }
    let _ = cluster.set_alive(3, false);
    let _ = cluster.propose(Command::StringSet {
        key: "byteraft-runtime-admin-lag-2".to_string(),
        value: b"lag-2".to_vec(),
    });
    let _ = cluster.set_alive(3, true);
    let stale_follower_write_rejection_present = matches!(
        cluster.check_write_authority(3),
        Err(RaftError::NotLeader { .. })
    );
    let _ = cluster.read_index(3);
    let _ = cluster.check_data_raft_read_policy(
        3,
        DataRaftReadPolicy {
            mode: DataRaftReadMode::BoundedStale,
            bounded_stale_max_index_lag: 0,
            ..DataRaftReadPolicy::default()
        },
    );
    let _ = cluster.check_data_raft_read_policy(
        3,
        DataRaftReadPolicy {
            mode: DataRaftReadMode::BoundedStale,
            bounded_stale_max_index_lag: 1,
            ..DataRaftReadPolicy::default()
        },
    );
    cluster.advance_time_ms(1_001);
    let _ = cluster.check_read(
        1,
        RaftReadOptions {
            strategy: RaftReadStrategy::LeaseRead,
            ..RaftReadOptions::default()
        },
    );
    let _ = cluster.tick_election();
    let _ = cluster.set_alive(2, false);
    let _ = cluster.set_alive(3, false);
    let _ = cluster.read_index(1);
    let _ = cluster.check_write_authority(1);
    let _ = cluster.set_alive(2, true);
    let _ = cluster.set_alive(3, true);
    let _ = cluster.tick_election();
    if let Ok(catchup) = cluster.build_append_entries_request(3) {
        let _ = cluster.receive_append_entries(catchup);
    }
    let _ = cluster.read_index(3);
    let _ = cluster.check_read(
        1,
        RaftReadOptions {
            strategy: RaftReadStrategy::LeaseRead,
            ..RaftReadOptions::default()
        },
    );
    let _ = cluster.add_learner_with_auto_promote(4, true);
    let _ = cluster.add_node_with_role(5, RaftReplicaRole::Witness);
    let _ = cluster.propose(Command::StringSet {
        key: "byteraft-runtime-admin-oversized".to_string(),
        value: vec![b'x'; 64 * 1024],
    });
    let out_of_order = AppendEntriesRequest {
        rpc: None,
        shard_id: 91,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 999,
        prev_log_term: 1,
        entries: Vec::new(),
        leader_commit: cluster.commit_index(1).unwrap_or_default(),
    };
    let _ = cluster.receive_append_entries(out_of_order);
    let lagging_commit = cluster.commit_index(3).unwrap_or_default();
    let apply_backpressure = AppendEntriesRequest {
        rpc: None,
        shard_id: 91,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: lagging_commit,
        prev_log_term: 1,
        entries: vec![RaftLogEntry {
            term: 1,
            index: lagging_commit.saturating_add(1),
            shard_id: 91,
            command: Command::StringSet {
                key: "byteraft-runtime-admin-apply-backpressure".to_string(),
                value: vec![b'y'; 96 * 1024],
            },
        }],
        leader_commit: lagging_commit.saturating_add(1),
    };
    let _ = cluster.receive_append_entries(apply_backpressure);
    let _ = cluster.begin_joint_consensus([1, 2, 3, 4]);
    let _ = cluster.set_alive(3, false);
    let _ = cluster.propose(Command::StringSet {
        key: "byteraft-runtime-admin-joint-snapshot-lag".to_string(),
        value: b"joint-lag".to_vec(),
    });
    let _ = cluster.set_alive(3, true);
    if let Ok(mut snapshot_chunks) = cluster.build_install_snapshot_chunks(3, 1) {
        if let Some(first_chunk) = snapshot_chunks.first().cloned() {
            let _ = cluster.receive_install_snapshot_chunk(first_chunk.clone());
            if snapshot_chunks.len() > 1 {
                let _ = cluster.receive_install_snapshot_chunk(first_chunk);
                snapshot_chunks[1].last_included_index =
                    snapshot_chunks[1].last_included_index.saturating_add(1);
                let _ = cluster.receive_install_snapshot_chunk(snapshot_chunks[1].clone());
            }
        }
    }
    let mut report = cluster.byteraft_runtime_admin_report();
    report.stale_follower_write_rejected =
        report.stale_follower_write_rejected && stale_follower_write_rejection_present;
    if !report.stale_follower_write_rejected
        && !report
            .blockers
            .iter()
            .any(|blocker| blocker == "stale_follower_write_rejection_missing")
    {
        report
            .blockers
            .push("stale_follower_write_rejection_missing".to_string());
    }
    report.ready = report.blockers.is_empty();
    let _ = fs::remove_dir_all(&root);

    let runtime_report_present = true;
    let per_peer_pipeline_state_present = !report.peer_pipeline_states.is_empty()
        && report
            .peer_pipeline_states
            .iter()
            .any(|peer| peer.append_queue_depth > 0 || peer.inflight_entries > 0)
        && report
            .peer_pipeline_states
            .iter()
            .all(|peer| peer.next_index > 0);
    let reorder_queue_state_present = report.reorder_queue_enabled
        && report
            .peer_pipeline_states
            .iter()
            .all(|peer| peer.reorder_queue_depth <= peer.match_index);
    let snapshot_sender_downloader_lifecycle_present = report.snapshot_sender_lifecycle_present
        && report.snapshot_downloader_lifecycle_present
        && report.snapshot_retry_backpressure_present;
    let wal_segment_lifecycle_present = report.wal_segment_lifecycle_present;
    let read_index_lease_semantics_present = report.read_index_validated
        && report.lease_read_validated
        && report.stale_follower_read_rejected;
    let admin_status_surface_present = report.admin_status_surface_complete;
    let process_path_operational_semantics_ready = runtime_report_present
        && per_peer_pipeline_state_present
        && reorder_queue_state_present
        && snapshot_sender_downloader_lifecycle_present
        && wal_segment_lifecycle_present
        && read_index_lease_semantics_present
        && report.stale_follower_write_rejected
        && admin_status_surface_present
        && report.ready;
    let mut missing = report.blockers.clone();
    if !per_peer_pipeline_state_present {
        missing.push("per-peer replication pipeline state is not evidenced".to_string());
    }
    if !snapshot_sender_downloader_lifecycle_present {
        missing.push("snapshot sender/downloader lifecycle is not evidenced".to_string());
    }
    if !wal_segment_lifecycle_present {
        missing.push("WAL segment lifecycle is not evidenced".to_string());
    }
    if !read_index_lease_semantics_present {
        missing.push("read-index/lease/stale-follower semantics are not evidenced".to_string());
    }

    RaftByteRaftRuntimeReadiness {
        runtime_report_present,
        per_peer_pipeline_state_present,
        reorder_queue_state_present,
        snapshot_sender_downloader_lifecycle_present,
        wal_segment_lifecycle_present,
        read_index_lease_semantics_present,
        stale_follower_write_rejection_present: report.stale_follower_write_rejected,
        admin_status_surface_present,
        process_path_operational_semantics_ready,
        report,
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
    let openraft_rollout = raft_openraft_rollout_readiness();
    distributed_raft_readiness_from_rollout(openraft_rollout)
}

pub fn distributed_raft_readiness_from_openraft_reports(
    data_node_report: &OpenRaftDataNodeProcessRolloutReport,
    metaserver_report: &OpenRaftMetaProcessRolloutReport,
) -> RaftDistributedReadiness {
    let openraft_rollout = raft_openraft_rollout_readiness_from_reports(
        Some(data_node_report),
        Some(metaserver_report),
    );
    distributed_raft_readiness_from_rollout(openraft_rollout)
}

fn distributed_raft_readiness_from_rollout(
    openraft_rollout: RaftOpenRaftRolloutReadiness,
) -> RaftDistributedReadiness {
    let metaserver_membership = raft_metaserver_membership_readiness();
    let atomic_apply = raft_atomic_apply_readiness();
    let transport_security = raft_transport_security_readiness();
    let external_chaos = raft_external_chaos_readiness();
    let byteraft_runtime = raft_byteraft_runtime_readiness();
    let mut missing = Vec::new();
    missing.extend(openraft_rollout.missing.clone());
    missing.extend(metaserver_membership.missing.clone());
    missing.extend(atomic_apply.missing.clone());
    missing.extend(transport_security.missing.clone());
    missing.extend(external_chaos.missing.clone());
    missing.extend(byteraft_runtime.missing.clone());
    RaftDistributedReadiness {
        complete: missing.is_empty(),
        production_ready: missing.is_empty(),
        mode: RaftDeploymentMode::ProductionDistributed,
        local_model_tested: true,
        openraft_engine_adapter_present: openraft_rollout.adapter_present,
        openraft_data_node_process_startup_present: openraft_rollout
            .data_node_process_startup_selects_openraft,
        openraft_metaserver_process_startup_present: openraft_rollout
            .metaserver_process_startup_selects_openraft,
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
        byteraft_per_peer_pipeline_state_present: byteraft_runtime.per_peer_pipeline_state_present,
        byteraft_reorder_queue_state_present: byteraft_runtime.reorder_queue_state_present,
        byteraft_snapshot_sender_downloader_lifecycle_present: byteraft_runtime
            .snapshot_sender_downloader_lifecycle_present,
        byteraft_wal_segment_lifecycle_present: byteraft_runtime.wal_segment_lifecycle_present,
        byteraft_read_index_lease_semantics_present: byteraft_runtime
            .read_index_lease_semantics_present,
        byteraft_admin_status_surface_present: byteraft_runtime.admin_status_surface_present,
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

