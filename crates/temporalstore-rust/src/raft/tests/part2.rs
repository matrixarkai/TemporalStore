// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 2, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

// shared-corpus: raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn http_raft_transport_sends_append_vote_and_snapshot_over_tcp() {
    let cluster = RaftCluster::new_single_shard(11, [1, 2, 3]);
    let addr = "127.0.0.1:18431".to_string();
    std::thread::spawn({
        let cluster = cluster.clone();
        let addr = addr.clone();
        move || serve(&addr, move |request| handle_raft_http(&cluster, request)).unwrap()
    });
    wait_for_http(&addr);

    let mut peers = BTreeMap::new();
    peers.insert(2, addr.clone());
    peers.insert(3, addr.clone());
    let transport = HttpRaftTransport::with_options(
        peers,
        HttpRequestOptions {
            connect_timeout_ms: 200,
            io_timeout_ms: 500,
            max_retries: 1,
        },
    );

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "network".to_string(),
            value: b"append".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let append = cluster.build_append_entries_request(3).unwrap();
    let append_response = transport.append_entries(append).unwrap();
    assert!(append_response.success);
    assert_eq!(append_response.match_index, 1);
    assert_eq!(
        cluster.read_local(
            3,
            Command::StringGet {
                key: "network".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"append".to_vec())
        })
    );

    let vote = cluster.build_vote_request(2, 3).unwrap();
    let vote_response = transport.request_vote(vote).unwrap();
    assert!(vote_response.vote_granted);

    cluster.elect_leader(2).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot".to_string(),
            value: b"installed".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.build_install_snapshot_request(3).unwrap();
    let snapshot_response = transport.install_snapshot(snapshot).unwrap();
    assert!(snapshot_response.success);
    assert_eq!(snapshot_response.last_included_index, 2);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn streaming_snapshot_chunks_install_only_after_all_chunks_arrive() {
    // This test is about ENTRY chunking; a state image is one small unit and
    // completes in a single chunk, which is not what these assertions probe.
    let _entries = super::part4::EnvFlagGuard::off("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard(21, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    for index in 0..5 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    cluster.set_alive(3, true).unwrap();
    let chunks = cluster.build_install_snapshot_chunks(3, 2).unwrap();
    assert_eq!(chunks.len(), 3);

    let first = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);

    let second = cluster
        .receive_install_snapshot_chunk(chunks[1].clone())
        .unwrap();
    assert!(second.success);
    assert!(!second.snapshot_complete);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);

    let final_chunk = cluster
        .receive_install_snapshot_chunk(chunks[2].clone())
        .unwrap();
    assert!(final_chunk.snapshot_complete);
    assert_eq!(final_chunk.last_included_index, 5);
    assert_eq!(cluster.commit_index(3).unwrap(), 5);
    assert_eq!(
        cluster
            .read_local(
                3,
                Command::StringGet {
                    key: "k4".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(vec![4])
        }
    );
}

// shared-corpus: raft_matrixraft_snapshot_lifecycle_depth
// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn matrixraft_snapshot_chunk_retry_releases_backpressure_and_installs_chunk() {
    // This test is about ENTRY chunking; a state image is one small unit and
    // completes in a single chunk, which is not what these assertions probe.
    let _entries = super::part4::EnvFlagGuard::off("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let transport = FlakyTransport {
        cluster: RaftCluster::new_single_shard(213, [1, 2, 3]),
        failures_left: Arc::new(Mutex::new(1)),
    };
    for index in 0..3 {
        transport
            .cluster
            .propose(Command::StringSet {
                key: format!("snapshot-retry-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    let chunks = transport
        .cluster
        .build_install_snapshot_chunks(3, 2)
        .unwrap();
    let runtime = RaftRpcRuntime::new(
        transport.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 2,
            retry_backoff_ms: 0,
            deadline_ms: 100,
            auth_token_required: false,
        },
    );

    let first = runtime.install_snapshot_chunk(chunks[0].clone()).unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    assert_eq!(first.received_chunks, 1);
    let metrics = runtime.metrics();
    assert_eq!(metrics.attempts, 2);
    assert_eq!(metrics.retries, 1);
    assert_eq!(metrics.successes, 1);
    assert_eq!(metrics.inflight, 0);

    let _permit = runtime.acquire().unwrap();
    assert!(matches!(
        runtime.install_snapshot_chunk(chunks[1].clone()).unwrap_err(),
        RaftError::Transport(message) if message.contains("backpressure")
    ));
    assert_eq!(runtime.metrics().backpressure_rejections, 1);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn matrixraft_snapshot_lifecycle_reports_timeout_rate_limit_rollback_membership_and_rejoin() {
    // This test is about ENTRY chunking; a state image is one small unit and
    // completes in a single chunk, which is not what these assertions probe.
    let _entries = super::part4::EnvFlagGuard::off("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard_with_config(
        214,
        [1, 2, 3],
        RaftConfig {
            max_inflights_replicate: 1,
            max_applied_log_bytes: 1,
            send_snapshot_timeout_ms: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("snapshot-lifecycle-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();
    cluster.set_alive(3, true).unwrap();

    let chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
    assert!(chunks.len() > 1);
    cluster.advance_time_ms(2);
    let first = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(first.success);
    assert!(!first.snapshot_complete);
    let duplicate = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(duplicate.success);
    assert!(!duplicate.snapshot_complete);
    for chunk in chunks.iter().skip(1) {
        cluster
            .receive_install_snapshot_chunk(chunk.clone())
            .unwrap();
    }
    assert_eq!(cluster.commit_index(3).unwrap(), 3);

    let rollback_chunks = cluster.build_install_snapshot_chunks(2, 1).unwrap();
    cluster
        .receive_install_snapshot_chunk(rollback_chunks[0].clone())
        .unwrap();
    let mut changed_metadata = rollback_chunks[1].clone();
    changed_metadata.last_included_index = changed_metadata.last_included_index.saturating_add(1);
    assert!(matches!(
        cluster.receive_install_snapshot_chunk(changed_metadata).unwrap_err(),
        RaftError::InvalidSnapshotChunk(message) if message.contains("metadata changed")
    ));

    let admin = cluster.matrixraft_runtime_admin_report();
    assert!(admin.snapshot_retry_backpressure_present);
    assert!(admin.snapshot_chunk_retry_present);
    assert!(admin.snapshot_send_timeout_present);
    assert!(admin.snapshot_rate_limit_present);
    assert!(admin.snapshot_install_progress_present);
    assert!(admin.snapshot_install_rollback_present);
    assert!(admin.snapshot_membership_change_present);
    assert!(admin.snapshot_rejoin_after_compacted_log_present);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "snapshot_sender_downloader_lifecycle" && item.ready));
    let lagging_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .unwrap();
    assert!(lagging_peer.snapshot_send_timeouts > 0);
    assert!(lagging_peer.snapshot_rate_limit_rejections > 0);
    assert!(lagging_peer.snapshot_chunk_retry_count > 0);
    assert_eq!(lagging_peer.snapshot_install_progress_per_mille, 1_000);
    assert!(lagging_peer.snapshot_during_membership_change);
    assert!(lagging_peer.snapshot_rejoin_after_compacted_log);
    let rollback_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 2)
        .unwrap();
    assert!(rollback_peer.snapshot_install_rolled_back > 0);

    let metrics = cluster.prometheus_metrics();
    for metric in [
        "temporalstore_raft_matrixraft_snapshot_chunk_retry_present",
        "temporalstore_raft_matrixraft_snapshot_send_timeout_present",
        "temporalstore_raft_matrixraft_snapshot_rate_limit_present",
        "temporalstore_raft_matrixraft_snapshot_install_progress_present",
        "temporalstore_raft_matrixraft_snapshot_install_rollback_present",
        "temporalstore_raft_matrixraft_snapshot_membership_change_present",
        "temporalstore_raft_matrixraft_snapshot_rejoin_after_compacted_log_present",
        "temporalstore_raft_matrixraft_peer_snapshot_send_timeouts",
        "temporalstore_raft_matrixraft_peer_snapshot_rate_limit_rejections",
        "temporalstore_raft_matrixraft_peer_snapshot_install_progress_per_mille",
        "temporalstore_raft_matrixraft_peer_snapshot_install_rolled_back",
        "temporalstore_raft_matrixraft_peer_snapshot_during_membership_change",
        "temporalstore_raft_matrixraft_peer_snapshot_rejoin_after_compacted_log",
    ] {
        assert!(metrics.contains(metric), "missing snapshot metric {metric}");
    }
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix
#[test]
fn raft_snapshot_transport_rejects_stale_term_before_install() {
    let cluster = RaftCluster::new_single_shard(211, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "snapshot-term".to_string(),
            value: b"leader-value".to_vec(),
        })
        .unwrap();
    cluster.elect_leader(2).unwrap();
    let mut request = cluster.build_install_snapshot_request(3).unwrap();
    request.term = 1;

    let response = cluster.receive_install_snapshot(request).unwrap();
    assert!(!response.success);
    assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
    assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);
}

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix raft_matrixraft_snapshot_lifecycle_depth
#[test]
fn raft_snapshot_chunk_transport_rejects_stale_term_before_buffering() {
    // This test is about ENTRY chunking; a state image is one small unit and
    // completes in a single chunk, which is not what these assertions probe.
    let _entries = super::part4::EnvFlagGuard::off("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard(212, [1, 2, 3]);
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("snapshot-chunk-term-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    cluster.elect_leader(2).unwrap();
    let mut chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
    chunks[0].term = 1;

    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(!response.success);
    assert!(!response.snapshot_complete);
    assert_eq!(response.reject_reason.as_deref(), Some("stale_term"));
    assert_eq!(cluster.hard_state(3).unwrap().current_term, 2);

    chunks[0].term = 2;
    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(response.success);
    assert!(!response.snapshot_complete);
    assert_eq!(response.received_chunks, 1);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix raft_matrixraft_rolling_restart_joint_consensus_fault_harness
#[test]
fn joint_consensus_requires_old_and_new_majorities_before_commit_or_write() {
    let cluster = RaftCluster::new_single_shard(22, [1, 2, 3]);
    let membership = cluster
        .begin_joint_consensus([1, 2, 3, 4, 5, 6, 7])
        .unwrap();
    assert_eq!(membership.old_voters, vec![1, 2, 3]);
    assert_eq!(membership.new_voters, vec![1, 2, 3, 4, 5, 6, 7]);
    for node_id in [4, 5, 6, 7] {
        cluster.set_alive(node_id, false).unwrap();
    }
    assert_eq!(
        cluster
            .propose(Command::StringSet {
                key: "blocked".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 3,
            required: 4,
        }
    );
    assert_eq!(
        cluster.commit_joint_consensus().unwrap_err(),
        RaftError::NoMajority {
            live: 3,
            required: 4,
        }
    );

    cluster.set_alive(4, true).unwrap();
    cluster.commit_joint_consensus().unwrap();
    assert_eq!(cluster.membership().voters, vec![1, 2, 3, 4, 5, 6, 7]);
    cluster
        .propose(Command::StringSet {
            key: "after".to_string(),
            value: b"ok".to_vec(),
        })
        .unwrap();
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix raft_matrixraft_rolling_restart_joint_consensus_fault_harness
#[test]
fn joint_consensus_state_survives_wal_restore_and_still_requires_both_majorities() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(222, [1, 2, 3]);
    cluster.begin_joint_consensus([1, 2, 3, 4, 5]).unwrap();
    cluster.persist_wal(dir.path()).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        222,
        [1, 2, 3, 4, 5],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(
        restored.joint_membership(),
        Some(JointConsensusMembership {
            old_voters: vec![1, 2, 3],
            new_voters: vec![1, 2, 3, 4, 5],
        })
    );

    restored.set_alive(2, false).unwrap();
    restored.set_alive(4, false).unwrap();
    restored.set_alive(5, false).unwrap();
    assert_eq!(
        restored
            .propose(Command::StringSet {
                key: "blocked-after-restore".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 2,
            required: 3,
        }
    );

    restored.set_alive(4, true).unwrap();
    restored.commit_joint_consensus().unwrap();
    assert_eq!(restored.membership().voters, vec![1, 2, 3, 4, 5]);
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn raft_rpc_runtime_retries_transport_errors_and_releases_inflight() {
    let transport = FlakyTransport {
        cluster: RaftCluster::new_single_shard(23, [1, 2, 3]),
        failures_left: Arc::new(Mutex::new(1)),
    };
    let runtime = RaftRpcRuntime::new(
        transport.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 2,
            retry_backoff_ms: 0,
            deadline_ms: 100,
            auth_token_required: false,
        },
    );
    let response = runtime
        .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
        .unwrap();
    assert!(response.success);
    assert_eq!(runtime.inflight(), 0);
    let metrics = runtime.metrics();
    assert_eq!(metrics.attempts, 2);
    assert_eq!(metrics.retries, 1);
    assert_eq!(metrics.successes, 1);
    assert_eq!(metrics.failures, 0);
    assert_eq!(metrics.inflight, 0);

    let _permit = runtime.acquire().unwrap();
    assert!(matches!(
        runtime
            .append_entries(transport.cluster.build_append_entries_request(2).unwrap())
            .unwrap_err(),
        RaftError::Transport(message) if message.contains("backpressure")
    ));
    assert_eq!(runtime.metrics().backpressure_rejections, 1);
}

// shared-corpus: raft_matrixraft_rpc_auth_deadline_transport_matrix
#[test]
fn raft_rpc_runtime_attaches_auth_and_deadline_metadata() {
    let cluster = RaftCluster::new_single_shard(25, [1, 2, 3]);
    let authenticated = AuthenticatedRaftTransport::new(cluster.clone(), "secret");
    let unauthenticated_runtime = RaftRpcRuntime::new(
        authenticated.clone(),
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 0,
            retry_backoff_ms: 0,
            deadline_ms: 250,
            auth_token_required: true,
        },
    );
    assert!(matches!(
        unauthenticated_runtime
            .append_entries(cluster.build_append_entries_request(2).unwrap())
            .unwrap_err(),
        RaftError::Transport(message) if message.contains("auth")
    ));

    let authenticated_runtime = RaftRpcRuntime::with_auth_token(
        authenticated,
        RaftRpcRuntimeOptions {
            max_inflight: 1,
            max_retries: 0,
            retry_backoff_ms: 0,
            deadline_ms: 250,
            auth_token_required: true,
        },
        Some("secret".to_string()),
    );
    let response = authenticated_runtime
        .append_entries(cluster.build_append_entries_request(2).unwrap())
        .unwrap();
    assert!(response.success);
}

#[test]
fn raft_scheduler_randomizes_election_timeout_and_emits_heartbeats() {
    let mut scheduler = RaftScheduler::new(RaftSchedulerOptions {
        heartbeat_interval_tick: 2,
        election_timeout_min_tick: 3,
        election_timeout_max_tick: 5,
        random_seed: 7,
    });
    assert!(!scheduler.tick(true).heartbeat_due);
    assert!(scheduler.tick(true).heartbeat_due);

    let mut election_due_at = None;
    for tick in 1..=8 {
        let event = scheduler.tick(false);
        assert!((3..=5).contains(&event.election_timeout_tick));
        if event.election_due {
            election_due_at = Some(tick);
            break;
        }
    }
    assert!((3..=5).contains(&election_due_at.expect("election should become due")));
}

// shared-corpus: raft_matrixraft_read_lease_fault_matrix raft_matrixraft_packet_loss_fault_harness
#[test]
fn partition_chaos_majority_side_continues_and_healed_replica_catches_up() {
    let cluster = RaftCluster::new_single_shard(24, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("majority-{index}"),
                value: vec![index],
            })
            .unwrap();
    }
    assert_eq!(
        cluster.read_from_replica(
            3,
            Command::StringGet {
                key: "majority-2".to_string(),
            },
        ),
        Err(RaftError::NodeNotFound(3))
    );

    cluster.set_alive(3, true).unwrap();
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster.commit_index(3).unwrap(),
        cluster.status().commit_index
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "majority-2".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(vec![2])
        }
    );
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_packet_loss_fault_harness
#[test]
fn distributed_propose_does_not_wait_for_failed_followers_after_quorum() {
    let cluster =
        RaftCluster::new_single_shard_with_config(7, vec![1, 2, 3], RaftConfig::default()).unwrap();
    let snapshot_attempts = Arc::new(Mutex::new(0usize));
    let transport = OneFailedFollowerTransport {
        cluster: cluster.clone(),
        failed_target: 3,
        snapshot_attempts: Arc::clone(&snapshot_attempts),
    };

    let response = cluster
        .propose_distributed(
            Command::StringSet {
                key: "quorum-fast-path".to_string(),
                value: b"ok".to_vec(),
            },
            &transport,
        )
        .unwrap();
    assert_eq!(response, CommandResponse::Empty);
    assert_eq!(*snapshot_attempts.lock().unwrap(), 0);
    assert_eq!(cluster.commit_index(1).unwrap(), 1);
    assert_eq!(cluster.commit_index(2).unwrap(), 1);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);
}

#[test]
fn distributed_raft_readiness_reports_remaining_production_blockers() {
    let readiness = distributed_raft_readiness();
    assert!(!readiness.complete);
    assert!(!readiness.production_ready);
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.local_model_tested);
    assert!(readiness.transport_contracts_present);
    assert!(readiness.rpc_runtime_observability_present);
    assert!(readiness.external_snapshot_refs_present);
    assert_eq!(
        readiness.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.temporal_raft_data_node_process_startup_present);
    assert!(readiness.temporal_raft_metaserver_process_startup_present);
    assert!(readiness.matrixraft_leader_write_authority_present);
    assert!(readiness.matrixraft_operator_observability_present);
    assert!(readiness.matrixraft_rpc_transport_contract_present);
    assert!(readiness.matrixraft_log_retention_snapshot_trigger_present);
    assert!(readiness.matrixraft_apply_snapshot_fence_present);
    assert!(readiness.raft_storage_apply_fence_present);
    assert!(readiness.matrixraft_snapshot_floor_log_matching_present);
    assert!(readiness.matrixraft_snapshot_tail_catchup_present);
    assert!(readiness.matrixraft_compacted_entry_rejection_present);
    assert!(readiness.matrixraft_metaserver_snapshot_floor_election_present);
    assert!(readiness.learner_catchup_promotion_present);
    assert!(readiness.metaserver_membership_workflow_present);
    assert!(readiness.durable_apply_index_snapshot_integrated);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(readiness.production_mtls_transport_present);
    assert!(readiness.external_chaos_validation_present);
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("applied Raft index")));
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("learner add")));
    assert!(!readiness.missing.iter().any(|item| item.contains("mTLS")));
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("process startup")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("metaserver multi-process rollout evidence")));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "data_node_report_missing" && item.evidence_field == "data_node_report"
    }));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "metaserver_report_missing" && item.evidence_field == "metaserver_report"
    }));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn raft_temporal_raft_rollout_readiness_fails_closed_without_process_rollout_evidence() {
    let readiness = raft_temporal_raft_rollout_readiness();
    assert_eq!(
        readiness.adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.data_node_process_startup_selects_temporal_raft);
    assert!(readiness.metaserver_process_startup_selects_temporal_raft);
    assert!(readiness.durable_log_state_present);
    assert_eq!(
        readiness.local_rollout_ready,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(!readiness.data_node_real_process_rollout_validated);
    assert!(!readiness.metaserver_real_process_rollout_validated);
    assert!(!readiness.multi_process_log_store_validation_present);
    assert!(!readiness.production_ready);
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("metaserver multi-process rollout evidence")));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "data_node_report_missing" && item.evidence_field == "data_node_report"
    }));
    assert!(readiness.missing_evidence_fields.iter().any(|item| {
        item.blocker == "metaserver_report_missing" && item.evidence_field == "metaserver_report"
    }));
    if !cfg!(feature = "temporal-raft-engine") {
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("TemporalRaft production engine adapter")));
    }

    let distributed = distributed_raft_readiness();
    assert_eq!(
        distributed.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(distributed.temporal_raft_data_node_process_startup_present);
    assert!(distributed.temporal_raft_metaserver_process_startup_present);
    assert!(distributed
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));
    assert!(distributed
        .missing_evidence_fields
        .iter()
        .any(|item| item.evidence_field == "data_node_report"));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn raft_temporal_raft_rollout_readiness_accepts_only_multi_process_reports() {
    let data_report = ready_data_node_temporal_raft_rollout_report();
    let meta_report = ready_meta_temporal_raft_rollout_report();
    let readiness =
        raft_temporal_raft_rollout_readiness_from_reports(Some(&data_report), Some(&meta_report));

    assert_eq!(
        readiness.adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(readiness.data_node_real_process_rollout_validated);
    assert!(readiness.metaserver_real_process_rollout_validated);
    assert!(readiness.multi_process_log_store_validation_present);
    assert_eq!(
        readiness.production_ready,
        cfg!(feature = "temporal-raft-engine")
    );
    if cfg!(feature = "temporal-raft-engine") {
        assert!(readiness.missing.is_empty());
    } else {
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("TemporalRaft production engine adapter")));
    }
    let distributed_with_evidence =
        distributed_raft_readiness_from_temporal_raft_reports(&data_report, &meta_report);
    assert_eq!(
        distributed_with_evidence.temporal_raft_engine_adapter_present,
        cfg!(feature = "temporal-raft-engine")
    );
    assert!(distributed_with_evidence.metaserver_driven_membership_present);
    assert_eq!(
        distributed_with_evidence.production_ready,
        cfg!(feature = "temporal-raft-engine")
    );

    let mut local_fixture_like_data = data_report;
    local_fixture_like_data.nodes.truncate(1);
    let rejected = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&local_fixture_like_data),
        Some(&meta_report),
    );
    assert!(!rejected.data_node_real_process_rollout_validated);
    assert!(!rejected.production_ready);
    assert!(rejected
        .missing
        .iter()
        .any(|item| item.contains("data-node multi-process rollout evidence")));

    let mut missing_process_path_proof = ready_data_node_temporal_raft_rollout_report();
    missing_process_path_proof.independent_snapshot_dirs = false;
    missing_process_path_proof.observed_process_requests = 0;
    missing_process_path_proof.read_index_responses_observed = 0;
    missing_process_path_proof.per_node_log_store_inspection_count = 1;
    let rejected_process_path = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_process_path_proof),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_process_path.data_node_real_process_rollout_validated);
    assert!(!rejected_process_path.production_ready);
    assert!(rejected_process_path
        .missing
        .iter()
        .any(|item| item.contains("independent WAL/snapshot dirs")));

    let mut duplicate_process_identity = ready_data_node_temporal_raft_rollout_report();
    duplicate_process_identity.nodes[1].addr = duplicate_process_identity.nodes[0].addr.clone();
    duplicate_process_identity.nodes[1].wal_dir =
        duplicate_process_identity.nodes[0].wal_dir.clone();
    duplicate_process_identity.nodes[1].snapshot_dir =
        duplicate_process_identity.nodes[0].snapshot_dir.clone();
    let rejected_duplicate_identity = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&duplicate_process_identity),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_duplicate_identity.data_node_real_process_rollout_validated);
    assert!(!rejected_duplicate_identity.production_ready);

    let mut missing_api_write = ready_data_node_temporal_raft_rollout_report();
    missing_api_write.write_proposed_through_process_api = false;
    missing_api_write.restart_recovery_validated = false;
    let rejected_missing_api_write = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_api_write),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_missing_api_write.data_node_real_process_rollout_validated);
    assert!(!rejected_missing_api_write.production_ready);

    let mut missing_meta_api_mutation = ready_meta_temporal_raft_rollout_report();
    missing_meta_api_mutation.mutation_proposed_through_process_api = false;
    missing_meta_api_mutation.read_index_validated = false;
    missing_meta_api_mutation.per_node_log_store_inspection_count = 1;
    let rejected_missing_meta_api_mutation = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_api_mutation),
    );
    assert!(!rejected_missing_meta_api_mutation.metaserver_real_process_rollout_validated);
    assert!(!rejected_missing_meta_api_mutation.production_ready);

    let mut missing_behavior_evidence = ready_data_node_temporal_raft_rollout_report();
    missing_behavior_evidence.failover_validated = false;
    missing_behavior_evidence.membership_change_validated = false;
    missing_behavior_evidence.follower_lag_validated = false;
    missing_behavior_evidence.secondary_read_validated = false;
    let rejected_behavior = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_behavior_evidence),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_behavior.data_node_real_process_rollout_validated);
    assert!(!rejected_behavior.production_ready);
    assert!(rejected_behavior
        .missing
        .iter()
        .any(|item| item.contains("failover")));

    let mut missing_meta_behavior = ready_meta_temporal_raft_rollout_report();
    missing_meta_behavior.secondary_read_validated = false;
    let rejected_meta_behavior = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_behavior),
    );
    assert!(!rejected_meta_behavior.metaserver_real_process_rollout_validated);
    assert!(!rejected_meta_behavior.production_ready);
    assert!(rejected_meta_behavior
        .missing
        .iter()
        .any(|item| item.contains("secondary reads")));

    let mut api_only_data = ready_data_node_temporal_raft_rollout_report();
    api_only_data.operational_semantics =
        TemporalRaftProcessOperationalSemanticsEvidence::default();
    let rejected_api_only = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&api_only_data),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_api_only.data_node_real_process_rollout_validated);
    assert!(!rejected_api_only.production_ready);
    assert!(rejected_api_only
        .missing
        .iter()
        .any(|item| item.contains("operational semantics evidence")));
    assert!(rejected_api_only
        .missing
        .iter()
        .any(|item| item.contains("process_path_validated")));

    let mut missing_read_safety = ready_data_node_temporal_raft_rollout_report();
    missing_read_safety
        .operational_semantics
        .read_index_validated = false;
    missing_read_safety
        .operational_semantics
        .leader_lease_validated = false;
    missing_read_safety
        .operational_semantics
        .stale_leader_lease_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .follower_lease_expiration_observed = false;
    missing_read_safety
        .operational_semantics
        .bounded_stale_read_acceptance_observed = false;
    missing_read_safety
        .operational_semantics
        .bounded_stale_read_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .minority_partition_read_rejection_observed = false;
    missing_read_safety
        .operational_semantics
        .healed_follower_catchup_observed = false;
    missing_read_safety
        .operational_semantics
        .stale_follower_write_rejected = false;
    missing_read_safety
        .operational_semantics
        .leader_transfer_exact_once_validated = false;
    missing_read_safety
        .operational_semantics
        .snapshot_bootstrap_validated = false;
    missing_read_safety
        .operational_semantics
        .membership_rescale_validated = false;
    missing_read_safety
        .operational_semantics
        .apply_pipeline_converged = false;
    missing_read_safety
        .operational_semantics
        .wal_persistence_observed = false;
    missing_read_safety
        .operational_semantics
        .fsm_apply_idempotent_replay_observed = false;
    missing_read_safety
        .operational_semantics
        .storage_mutation_wal_fence_atomicity_observed = false;
    missing_read_safety
        .operational_semantics
        .snapshot_install_apply_fence_atomicity_observed = false;
    missing_read_safety
        .operational_semantics
        .process_restart_after_apply_crash_recovered = false;
    let rejected_read_safety = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_read_safety),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_read_safety.data_node_real_process_rollout_validated);
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("read_index_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("leader_lease_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("stale_leader_lease_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("bounded_stale_read_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("minority_partition_read_rejection_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("healed_follower_catchup_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("stale_follower_write_rejected")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("leader_transfer_exact_once_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("snapshot_bootstrap_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("membership_rescale_validated")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("apply_pipeline_converged")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("wal_persistence_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("fsm_apply_idempotent_replay_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("storage_mutation_wal_fence_atomicity_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("snapshot_install_apply_fence_atomicity_observed")));
    assert!(rejected_read_safety
        .missing
        .iter()
        .any(|item| item.contains("process_restart_after_apply_crash_recovered")));

    let mut missing_data_crash_windows = ready_data_node_temporal_raft_rollout_report();
    missing_data_crash_windows.crash_after_storage_mutation_recovered = false;
    missing_data_crash_windows.crash_after_wal_persist_recovered = false;
    missing_data_crash_windows.crash_during_snapshot_install_recovered = false;
    missing_data_crash_windows.apply_fence_recovered_after_restart = false;
    let rejected_data_crash_windows = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&missing_data_crash_windows),
        Some(&ready_meta_temporal_raft_rollout_report()),
    );
    assert!(!rejected_data_crash_windows.data_node_real_process_rollout_validated);
    assert!(
        rejected_data_crash_windows
            .missing
            .iter()
            .any(|item| item
                .contains("storage mutation/WAL persistence/snapshot install/apply fence"))
    );

    let mut missing_meta_crash_windows = ready_meta_temporal_raft_rollout_report();
    missing_meta_crash_windows.crash_after_meta_mutation_recovered = false;
    missing_meta_crash_windows.crash_after_meta_wal_persist_recovered = false;
    missing_meta_crash_windows.crash_during_meta_snapshot_install_recovered = false;
    missing_meta_crash_windows.meta_apply_fence_recovered_after_restart = false;
    let rejected_meta_crash_windows = raft_temporal_raft_rollout_readiness_from_reports(
        Some(&ready_data_node_temporal_raft_rollout_report()),
        Some(&missing_meta_crash_windows),
    );
    assert!(!rejected_meta_crash_windows.metaserver_real_process_rollout_validated);
    assert!(rejected_meta_crash_windows
        .missing
        .iter()
        .any(|item| item.contains("meta mutation/WAL persistence/snapshot install/apply fence")));
}

// shared-corpus: raft_matrixraft_process_rollout_multiplane_report_contract
#[test]
fn raft_process_path_readiness_report_maps_remaining_blockers_to_fields() {
    let data_report = ready_data_node_temporal_raft_rollout_report();
    let meta_report = ready_meta_temporal_raft_rollout_report();
    let ready = raft_process_path_readiness_report_from_reports(&data_report, &meta_report);
    assert!(ready.ready);
    assert!(ready.multi_process_data_node_and_metaserver_raft);
    assert!(ready.failover_on_both_planes);
    assert!(ready.membership_add_remove_under_load);
    assert!(ready.secondary_lag_and_catchup);
    assert!(ready.snapshot_restart_after_compaction);
    assert!(ready.remaining_blockers.is_empty());

    let mut missing_data_failover = ready_data_node_temporal_raft_rollout_report();
    missing_data_failover.failover_validated = false;
    let rejected = raft_process_path_readiness_report_from_reports(
        &missing_data_failover,
        &ready_meta_temporal_raft_rollout_report(),
    );
    assert!(!rejected.ready);
    assert!(!rejected.failover_on_both_planes);
    assert!(rejected.remaining_blockers.iter().any(|item| {
        item.evidence_field == "data_node_report.failover_validated"
            || item.evidence_field == "final_raft_readiness.failover_on_both_planes"
    }));

    let mut missing_membership_load = ready_meta_temporal_raft_rollout_report();
    missing_membership_load
        .operational_semantics
        .leader_transfer_under_load_validated = false;
    let rejected = raft_process_path_readiness_report_from_reports(
        &ready_data_node_temporal_raft_rollout_report(),
        &missing_membership_load,
    );
    assert!(!rejected.membership_add_remove_under_load);
    assert!(rejected.remaining_blockers.iter().any(|item| {
        item.evidence_field
            == "metaserver_report.operational_semantics.leader_transfer_under_load_validated"
            || item.evidence_field == "final_raft_readiness.membership_add_remove_under_load"
    }));

    let mut missing_secondary_catchup = ready_data_node_temporal_raft_rollout_report();
    missing_secondary_catchup
        .operational_semantics
        .healed_follower_catchup_observed = false;
    let rejected = raft_process_path_readiness_report_from_reports(
        &missing_secondary_catchup,
        &ready_meta_temporal_raft_rollout_report(),
    );
    assert!(!rejected.secondary_lag_and_catchup);
    assert!(rejected.remaining_blockers.iter().any(|item| {
        item.evidence_field
            == "data_node_report.operational_semantics.healed_follower_catchup_observed"
            || item.evidence_field == "final_raft_readiness.secondary_lag_and_catchup"
    }));

    let mut missing_snapshot_rejoin = ready_meta_temporal_raft_rollout_report();
    missing_snapshot_rejoin
        .operational_semantics
        .follower_rejoin_after_compaction_validated = false;
    let rejected = raft_process_path_readiness_report_from_reports(
        &ready_data_node_temporal_raft_rollout_report(),
        &missing_snapshot_rejoin,
    );
    assert!(!rejected.snapshot_restart_after_compaction);
    assert!(rejected.remaining_blockers.iter().any(|item| {
        item.evidence_field
            == "metaserver_report.operational_semantics.follower_rejoin_after_compaction_validated"
            || item.evidence_field == "final_raft_readiness.snapshot_restart_after_compaction"
    }));
}

#[test]
fn raft_atomic_apply_readiness_covers_data_node_atomic_durability() {
    let readiness = raft_atomic_apply_readiness();
    assert!(readiness.storage_apply_fence_present);
    assert!(readiness.wal_fence_recovery_validation_present);
    assert!(readiness.snapshot_lifecycle_report_present);
    assert!(readiness.local_contract_ready);
    assert!(readiness.storage_mutation_atomic_commit_present);
    assert!(readiness.snapshot_install_atomic_commit_present);
    assert!(readiness.real_data_node_process_integration_present);
    assert!(readiness.production_ready);
    assert!(readiness.missing.is_empty());

    let distributed = distributed_raft_readiness();
    assert!(distributed.raft_storage_apply_fence_present);
    assert!(distributed.durable_apply_index_snapshot_integrated);
    assert!(!distributed
        .missing
        .iter()
        .any(|item| item.contains("atomic applied-index")));
}

#[test]
fn raft_metaserver_membership_readiness_reports_real_scheduler_execution() {
    let readiness = raft_metaserver_membership_readiness();
    assert!(readiness.topology_membership_plan_present);
    assert!(readiness.data_raft_membership_apply_present);
    assert!(readiness.meta_owned_workflow_report_present);
    assert!(readiness.learner_catchup_promotion_present);
    assert!(readiness.leader_transfer_voter_remove_present);
    assert!(readiness.local_workflow_ready);
    assert!(readiness.networked_scheduler_transport_present);
    assert!(readiness.persisted_scheduler_task_state_present);
    assert!(readiness.real_data_node_group_execution_present);
    assert!(readiness.production_ready);
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("learner add")));
    assert!(!readiness
        .missing
        .iter()
        .any(|item| item.contains("follower lag")));

    let distributed = distributed_raft_readiness();
    assert!(distributed.metaserver_membership_workflow_present);
    assert!(distributed.metaserver_driven_membership_present);
    assert!(!distributed
        .missing
        .iter()
        .any(|item| item.contains("networked Raft groups")));
}

#[test]
fn the_report_says_which_transport_this_node_is_running() {
    use crate::raft::{
        production_raft_security_from_lookup, raft_transport_security_readiness_for,
        ProductionRaftSecurityMode,
    };

    // The `*_present` flags describe the build, so they answer the same on
    // every deployment -- including one running plaintext. Served at an
    // unauthenticated /readiness on both the metaserver and the datanode, that
    // left no way to tell "we can do mTLS" from "this node is doing mTLS".
    let plaintext = raft_transport_security_readiness_for(
        ProductionRaftSecurityMode::PlaintextForLocalChaos,
    );
    assert_eq!(plaintext.configured_transport_mode, "plaintext_for_local_chaos");
    // The build-capability flags are unchanged by this: they still describe the
    // build, and anything reading them keeps the answer it had.
    assert!(plaintext.mtls_cert_key_ca_validation_present);

    let mtls = raft_transport_security_readiness_for(ProductionRaftSecurityMode::Mtls);
    assert_eq!(mtls.configured_transport_mode, "mtls");

    // And the default deployment really is the plaintext one, so the two above
    // are not hypothetical. Read through the lookup rather than the process
    // environment so a test setting these cannot change the answer.
    let configured = production_raft_security_from_lookup("token", true, |_| None);
    assert_eq!(
        configured.security.mode,
        ProductionRaftSecurityMode::PlaintextForLocalChaos,
        "the default transport stopped being plaintext; this test's premise needs revisiting"
    );
}

#[test]
fn raft_transport_security_readiness_covers_service_mtls() {
    let readiness = raft_transport_security_readiness();
    assert!(readiness.auth_token_validation_present);
    assert!(readiness.mtls_cert_key_ca_validation_present);
    assert!(readiness.authenticated_http_transport_present);
    assert!(readiness.plaintext_local_chaos_guard_present);
    assert!(readiness.service_process_mtls_enforcement_present);
    assert!(readiness.production_ready);
    assert!(readiness.missing.is_empty());

    let distributed = distributed_raft_readiness();
    assert!(distributed.production_mtls_transport_present);
    assert!(!distributed
        .missing
        .iter()
        .any(|item| item.contains("service-process mTLS runtime selection")));
}

#[test]
fn raft_external_chaos_readiness_covers_external_faults() {
    let readiness = raft_external_chaos_readiness();
    assert!(readiness.local_os_process_restart_failover_present);
    assert!(readiness.stale_read_partition_heal_present);
    assert!(readiness.lagging_follower_catchup_present);
    assert!(readiness.networked_membership_snapshot_present);
    assert!(readiness.storage_replay_gate_present);
    assert!(readiness.local_chaos_ready);
    assert!(readiness.external_packet_loss_present);
    assert!(readiness.external_disk_pressure_present);
    assert!(readiness.external_process_chaos_present);
    assert!(readiness.production_ready);
    assert!(readiness.missing.is_empty());

    let distributed = distributed_raft_readiness();
    assert!(distributed.external_chaos_validation_present);
    assert!(!distributed
        .missing
        .iter()
        .any(|item| item.contains("process-chaos")));
}

#[test]
fn local_raft_deployment_mode_is_rejected() {
    let err = validate_raft_deployment_mode(RaftDeploymentMode::LocalModel).unwrap_err();
    assert_eq!(err.mode, RaftDeploymentMode::LocalModel);
    assert!(err
        .message
        .contains("local Raft deployment mode is disabled"));
    assert!(err
        .message
        .contains("production distributed Raft is required"));
    assert!(!err.message.contains("LocalModel"));
}

#[test]
fn production_raft_mode_uses_temporal_raft_ready_path_when_adapter_is_enabled() {
    let err = validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).unwrap_err();
    assert_eq!(err.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(!err
        .missing
        .iter()
        .any(|item| item.contains("applied Raft index")));
    assert!(!err.missing.iter().any(|item| item.contains("learner add")));
    assert!(err
        .missing
        .iter()
        .any(|item| item.contains("multi-process rollout evidence")));
    if !cfg!(feature = "temporal-raft-engine") {
        assert!(err
            .missing
            .iter()
            .any(|item| item.contains("TemporalRaft production engine adapter")));
    }
    assert_eq!(require_production_raft_ready().unwrap_err(), err);
}

#[test]
fn production_raft_runtime_validates_security_timer_and_chaos_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let options = ProductionRaftRuntimeOptions {
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        shard_id: 91,
        local_node_id: 1,
        nodes: vec![
            ProductionRaftNode {
                node_id: 1,
                addr: "127.0.0.1:19101".to_string(),
            },
            ProductionRaftNode {
                node_id: 2,
                addr: "127.0.0.1:19102".to_string(),
            },
            ProductionRaftNode {
                node_id: 3,
                addr: "127.0.0.1:19103".to_string(),
            },
        ],
        wal_dir: dir.path().display().to_string(),
        config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions {
            max_retries: 1,
            deadline_ms: 50,
            ..RaftRpcRuntimeOptions::default()
        },
        security: ProductionRaftSecurity::plaintext_for_local_chaos("token"),
        heartbeat_interval_ms: 5,
        election_tick_ms: 1,
        max_catchup_entries_per_heartbeat: 1,
        allow_plaintext_for_local_chaos: true,
    };

    let runtime = ProductionRaftRuntime::start(options.clone()).unwrap();
    runtime.validate_ready().unwrap();
    assert_eq!(runtime.status().leader_id, 1);
    let transport = runtime.transport();
    assert_eq!(transport.metrics().inflight, 0);

    let timer = runtime.start_timer_loop();
    runtime
        .cluster()
        .propose(Command::StringSet {
            key: "production-raft".to_string(),
            value: b"ok".to_vec(),
        })
        .unwrap();
    let durability = runtime.data_node_atomic_durability_report();
    assert!(durability.ready, "{durability:?}");
    assert!(durability.storage_apply_fence_valid);
    assert!(durability.storage_mutation_atomic_commit_present);
    assert!(durability.snapshot_install_atomic_commit_present);
    assert_eq!(durability.commit_index, durability.wal_commit_index);
    assert_eq!(durability.applied_index, durability.fence_applied_index);
    thread::sleep(Duration::from_millis(20));
    timer.stop();
    assert!(runtime
        .cluster()
        .replication_health(0)
        .caught_up_voters
        .contains(&2));

    let chaos = ProductionRaftChaosPlan {
        shard_id: 91,
        nodes: options
            .nodes
            .iter()
            .map(|node| ProductionRaftProcessSpec {
                node_id: node.node_id,
                addr: node.addr.clone(),
                wal_dir: format!("{}/node-{}", options.wal_dir, node.node_id),
                command: "temporalstore-raft-node".to_string(),
                args: vec!["--serve".to_string()],
                env: BTreeMap::new(),
            })
            .collect(),
        partition_pairs: vec![(1, 3)],
        crash_nodes: vec![1],
        restart_nodes: vec![1],
    };
    chaos.validate().unwrap();

    let mut invalid = options.clone();
    invalid.max_catchup_entries_per_heartbeat = 0;
    assert!(invalid.validate().is_err());

    let mut invalid = options.clone();
    invalid.nodes[1].node_id = invalid.nodes[0].node_id;
    assert!(invalid.validate().is_err());

    let mut invalid = options.clone();
    invalid.nodes[1].addr = " ".to_string();
    assert!(invalid.validate().is_err());

    let mut invalid = options.clone();
    invalid.nodes[1].addr = invalid.nodes[0].addr.clone();
    assert!(invalid.validate().is_err());

    let mut invalid = options;
    invalid.allow_plaintext_for_local_chaos = false;
    assert!(invalid.validate().is_err());
}

#[test]
fn production_raft_mtls_requires_readable_nonempty_files() {
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");
    let ca = dir.path().join("ca.crt");
    fs::write(&cert, "cert").unwrap();
    fs::write(&key, "key").unwrap();
    fs::write(&ca, "ca").unwrap();

    let security = ProductionRaftSecurity::mtls(
        "token",
        cert.display().to_string(),
        key.display().to_string(),
        ca.display().to_string(),
    );
    security.validate(false).unwrap();

    let empty_key = dir.path().join("empty.key");
    fs::write(&empty_key, "").unwrap();
    let security = ProductionRaftSecurity::mtls(
        "token",
        cert.display().to_string(),
        empty_key.display().to_string(),
        ca.display().to_string(),
    );
    assert!(security.validate(false).is_err());

    let missing_ca = dir.path().join("missing-ca.crt");
    let security = ProductionRaftSecurity::mtls(
        "token",
        cert.display().to_string(),
        key.display().to_string(),
        missing_ca.display().to_string(),
    );
    assert!(security.validate(false).is_err());
}

#[test]
fn production_raft_security_env_selects_mtls_runtime_mode() {
    let dir = tempfile::tempdir().unwrap();
    let cert = dir.path().join("node.crt");
    let key = dir.path().join("node.key");
    let ca = dir.path().join("ca.crt");
    fs::write(&cert, "cert").unwrap();
    fs::write(&key, "key").unwrap();
    fs::write(&ca, "ca").unwrap();
    let env = BTreeMap::from([
        ("TS_RAFT_SECURITY_MODE".to_string(), "mtls".to_string()),
        ("TS_RAFT_AUTH_TOKEN".to_string(), "secure-token".to_string()),
        ("TS_RAFT_CERT_PATH".to_string(), cert.display().to_string()),
        ("TS_RAFT_KEY_PATH".to_string(), key.display().to_string()),
        ("TS_RAFT_CA_CERT_PATH".to_string(), ca.display().to_string()),
        ("TS_RAFT_ALLOW_PLAINTEXT".to_string(), "false".to_string()),
    ]);
    let selected =
        production_raft_security_from_lookup("fallback", true, |key| env.get(key).cloned());
    assert_eq!(selected.security.mode, ProductionRaftSecurityMode::Mtls);
    assert!(!selected.allow_plaintext_for_local_chaos);
    selected.security.validate(false).unwrap();

    let plaintext = production_raft_security_from_lookup("fallback", false, |_| None);
    assert_eq!(
        plaintext.security.mode,
        ProductionRaftSecurityMode::PlaintextForLocalChaos
    );
    assert!(!plaintext.allow_plaintext_for_local_chaos);
    assert!(plaintext
        .security
        .validate(plaintext.allow_plaintext_for_local_chaos)
        .is_err());

    let invalid_mode = BTreeMap::from([
        ("TS_RAFT_SECURITY_MODE".to_string(), "surprise".to_string()),
        ("TS_RAFT_AUTH_TOKEN".to_string(), "secure-token".to_string()),
    ]);
    let invalid = production_raft_security_from_lookup("fallback", true, |key| {
        invalid_mode.get(key).cloned()
    });
    assert_eq!(invalid.security.mode, ProductionRaftSecurityMode::Mtls);
    assert!(invalid
        .security
        .validate(invalid.allow_plaintext_for_local_chaos)
        .is_err());
}

#[test]
fn raft_security_and_external_chaos_readiness_are_implemented() {
    let security = raft_transport_security_readiness();
    assert!(security.production_ready, "{security:?}");
    assert!(security.service_process_mtls_enforcement_present);
    assert!(security.missing.is_empty());

    let chaos = raft_external_chaos_readiness();
    assert!(chaos.production_ready, "{chaos:?}");
    assert!(chaos.external_packet_loss_present);
    assert!(chaos.external_disk_pressure_present);
    assert!(chaos.external_process_chaos_present);
    assert!(chaos.missing.is_empty());
}

#[test]
fn production_raft_runtime_replicates_over_separate_http_nodes() {
    let dir = tempfile::tempdir().unwrap();
    let addr1 = free_local_addr();
    let addr2 = free_local_addr();
    let addr3 = free_local_addr();
    let nodes = vec![
        ProductionRaftNode {
            node_id: 1,
            addr: addr1,
        },
        ProductionRaftNode {
            node_id: 2,
            addr: addr2,
        },
        ProductionRaftNode {
            node_id: 3,
            addr: addr3,
        },
    ];
    let mut runtimes = Vec::new();
    for node in &nodes {
        let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
            snapshot_check_interval_ms: 0,
            engine: ProductionRaftEngineKind::TemporalRaft,
            shard_id: 193,
            local_node_id: node.node_id,
            nodes: nodes.clone(),
            wal_dir: dir
                .path()
                .join(format!("node-{}", node.node_id))
                .display()
                .to_string(),
            config: RaftConfig::default(),
            rpc: RaftRpcRuntimeOptions {
                max_retries: 2,
                deadline_ms: 200,
                ..RaftRpcRuntimeOptions::default()
            },
            security: ProductionRaftSecurity::plaintext_for_local_chaos("token"),
            heartbeat_interval_ms: 20,
            election_tick_ms: 5,
            max_catchup_entries_per_heartbeat: 32,
            allow_plaintext_for_local_chaos: true,
        })
        .unwrap();
        let addr = node.addr.clone();
        let runtime_for_server = runtime.clone();
        thread::spawn(move || {
            serve(&addr, move |request| {
                match (request.method.as_str(), request.path.as_str()) {
                    ("POST", "/raft/propose") => {
                        match parse_json::<DistributedRaftProposeRequest>(&request.body) {
                            Ok(req) => match runtime_for_server.propose(req.command) {
                                Ok(response) => json_response(
                                    200,
                                    &DistributedRaftCommandResponse {
                                        status: Status::ok(),
                                        response,
                                    },
                                ),
                                Err(err) => json_response(
                                    200,
                                    &DistributedRaftCommandResponse {
                                        status: Status::error("raft_error", err.to_string()),
                                        response: CommandResponse::Empty,
                                    },
                                ),
                            },
                            Err(err) => {
                                json_response(400, &Status::error("bad_request", err.to_string()))
                            }
                        }
                    }
                    _ => handle_raft_http(&runtime_for_server.cluster(), request),
                }
            })
            .unwrap()
        });
        runtimes.push(runtime);
    }
    for node in &nodes {
        wait_for_http(&node.addr);
    }

    let response: DistributedRaftCommandResponse = post_json_with_options(
        &nodes[0].addr,
        "/raft/propose",
        &DistributedRaftProposeRequest {
            command: Command::StringSet {
                key: "separate-node".to_string(),
                value: b"ready".to_vec(),
            },
        },
        HttpRequestOptions {
            connect_timeout_ms: 1_000,
            io_timeout_ms: 5_000,
            max_retries: 3,
        },
    )
    .unwrap();
    assert!(response.status.ok);

    wait_for_replica_value(&runtimes[1], 2, "separate-node", b"ready");
    wait_for_replica_value(&runtimes[2], 3, "separate-node", b"ready");
}

// shared-corpus: raft_matrixraft_read_lease_fault_matrix
#[test]
fn raft_replica_read_rejects_lagging_follower_and_succeeds_after_catchup() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "k".to_string()
                },
            )
            .unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );

    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "k".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );
}

#[test]
fn raft_can_elect_new_leader_and_continue() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(1, false).unwrap();
    cluster.elect_leader(2).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    let response = cluster
        .read_local(
            2,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
    assert_eq!(cluster.commit_index(2).unwrap(), 1);
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
}

// shared-corpus: raft_matrixraft_election_controls
#[test]
fn raft_tick_election_waits_for_timeout_and_prevotes_before_promotion() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            election_cycle_tick: 3,
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::LeaderAlive { leader_id: 1 }
    );
    cluster.set_alive(1, false).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::ElectionPending {
            elapsed_tick: 1,
            timeout_tick: 3,
        }
    );
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::ElectionPending {
            elapsed_tick: 2,
            timeout_tick: 3,
        }
    );
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::LeaderElected {
            leader_id: 2,
            term: 2,
        }
    );
    assert_eq!(cluster.leader_id(), 2);
}

// shared-corpus: raft_matrixraft_election_controls
#[test]
fn raft_prevote_rejects_candidate_without_quorum() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            election_cycle_tick: 1,
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_alive(1, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::PreVoteRejected { candidate_id: 2 }
    );
    assert_eq!(cluster.leader_id(), 1);
    let admin = cluster.matrixraft_runtime_admin_report();
    assert_eq!(admin.pre_vote_requests, 1);
    assert_eq!(admin.pre_vote_rejected, 1);
    assert!(admin.pre_vote_process_evidence_observed);
    assert!(admin
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 2 && peer.pre_vote_rejections == 1));
    assert!(admin
        .blockers
        .contains(&"election_prohibition_evidence_missing".to_string()));
}

// shared-corpus: raft_matrixraft_election_controls
#[test]
fn raft_election_controls_record_prohibition_offline_and_transfer_timeouts() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            election_cycle_tick: 1,
            enable_pre_vote: true,
            prohibits_election: true,
            offline_timeout_tick: 5,
            transfer_timeout_tick: 5,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.add_learner_with_auto_promote(4, true).unwrap();

    cluster.set_alive(1, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::PreVoteRejected { candidate_id: 2 }
    );
    cluster.set_alive(1, true).unwrap();
    assert_eq!(cluster.elect_leader(4), Err(RaftError::ElectionProhibited));
    cluster.advance_time_ms(6);
    cluster.begin_leader_transfer(4).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "transfer-timeout-under-load".to_string(),
            value: b"committed-on-old-leader".to_vec(),
        })
        .unwrap();
    cluster.advance_time_ms(6);

    let admin = cluster.matrixraft_runtime_admin_report();
    assert!(admin.pre_vote_enforced);
    assert!(admin.election_prohibition_observed);
    assert!(admin.offline_timeout_observed);
    assert!(admin.transfer_timeout_observed);
    assert!(admin.election_controls_enforced);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "pre_vote_election_transfer_controls" && item.ready));
    assert!(admin
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 3 && peer.offline_timeout_reached));
    assert!(admin
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 4
            && peer.auto_promoted_from_learner
            && peer.election_rejections > 0
            && peer.transfer_leader_timeouts > 0));
    assert_eq!(
        cluster
            .read_local(
                1,
                Command::StringGet {
                    key: "transfer-timeout-under-load".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"committed-on-old-leader".to_vec())
        }
    );
}

#[test]
fn raft_status_read_index_and_transfer_leader_match_engine_control_shape() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    let status = cluster.status();
    assert_eq!(status.leader_id, 1);
    assert_eq!(status.commit_index, 1);
    assert_eq!(status.majority, 2);
    assert!(status.has_majority);
    assert!(status.leader_lease_valid);
    assert_eq!(status.nodes.len(), 3);
    assert!(status.nodes.iter().all(|node| node.lag == 0));

    let read_index = cluster.read_index(2).unwrap();
    assert_eq!(read_index.leader_id, 1);
    assert_eq!(read_index.node_id, 2);
    assert_eq!(read_index.read_index, 1);

    cluster.transfer_leader(2).unwrap();
    assert_eq!(cluster.leader_id(), 2);
    let local = cluster.local_status(2).unwrap();
    assert_eq!(local.role, RaftRole::Leader);
    assert_eq!(local.commit_index, 1);

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_cluster_commit_index{kind=\"data\"} 1"));
    assert!(metrics.contains("temporalstore_raft_node_lag"));
}

// shared-corpus: raft_matrixraft_read_safety_policy
// shared-corpus: raft_matrixraft_read_lease_fault_matrix
#[test]
fn raft_leader_lease_expiry_blocks_linearizable_reads_and_writes_until_heartbeat() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            lease_duration_ms: 10,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "lease-key".to_string(),
            value: b"before-expiry".to_vec(),
        })
        .unwrap();
    assert!(cluster.status().leader_lease_valid);

    cluster.advance_time_ms(11);
    assert!(!cluster.status().leader_lease_valid);
    assert_eq!(cluster.read_index(1), Err(RaftError::LeaderUnavailable));
    assert_eq!(
        cluster.propose(Command::StringSet {
            key: "lease-key".to_string(),
            value: b"should-not-commit".to_vec(),
        }),
        Err(RaftError::LeaderUnavailable)
    );

    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::LeaderAlive { leader_id: 1 }
    );
    assert!(cluster.status().leader_lease_valid);
    cluster
        .propose(Command::StringSet {
            key: "lease-key".to_string(),
            value: b"after-heartbeat".to_vec(),
        })
        .unwrap();
    assert_eq!(
        cluster
            .read_local(
                1,
                Command::StringGet {
                    key: "lease-key".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"after-heartbeat".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_read_lease_fault_matrix
// shared-corpus: raft_matrixraft_packet_loss_fault_harness
#[test]
fn matrixraft_read_safety_fault_matrix_records_partition_and_catchup_evidence() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            lease_duration_ms: 10,
            election_cycle_tick: 1,
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "read-safety".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "read-safety".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();
    assert_eq!(
        cluster.read_index(3),
        Err(RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 1,
            leader_commit_index: 2,
        })
    );
    assert_eq!(
        cluster.check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 0,
                ..DataRaftReadPolicy::default()
            },
        ),
        Err(RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 1,
            leader_commit_index: 2,
        })
    );
    assert!(cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 1,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());

    assert_eq!(
        cluster.check_write_authority(3),
        Err(RaftError::NotLeader { node_id: 3 })
    );
    cluster.set_alive(2, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    assert_eq!(cluster.read_index(1), Err(RaftError::LeaderUnavailable));
    assert_eq!(
        cluster.check_write_authority(1),
        Err(RaftError::NotLeader { node_id: 1 })
    );
    assert_eq!(
        cluster.propose(Command::StringSet {
            key: "read-safety".to_string(),
            value: b"minority-write".to_vec(),
        }),
        Err(RaftError::NoMajority {
            live: 1,
            required: 2,
        })
    );

    cluster.set_alive(2, true).unwrap();
    cluster.set_alive(3, true).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::LeaderAlive { leader_id: 1 }
    );
    cluster.catch_up(3).unwrap();
    assert!(cluster.read_index(3).is_ok());
    assert!(cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 0,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
    cluster
        .propose(Command::StringSet {
            key: "read-safety".to_string(),
            value: b"v3".to_vec(),
        })
        .unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "read-safety".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v3".to_vec())
        }
    );

    cluster.set_alive(1, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::PreVoteRejected { candidate_id: 2 }
    );
    cluster.set_alive(1, true).unwrap();
    cluster.set_alive(3, true).unwrap();
    assert!(cluster.read_index(1).is_ok());

    let admin = cluster.matrixraft_runtime_admin_report();
    let state = cluster.read_safety_runtime_state();
    assert!(state.stale_leader_lease_rejected > 0);
    assert!(state.lagging_follower_read_rejected > 0);
    assert!(state.bounded_stale_read_accepted > 0);
    assert!(state.bounded_stale_read_rejected > 0);
    assert!(state.minority_partition_read_rejected > 0);
    assert!(state.minority_partition_write_rejected > 0);
    assert!(state.stale_follower_write_rejected > 0);
    assert!(state.healed_follower_catchup_observed > 0);
    assert!(admin.stale_follower_read_rejected);
    assert!(admin.stale_follower_write_rejected);
    assert!(admin.stale_leader_lease_rejected);
    assert!(admin.lagging_follower_read_rejected);
    assert!(admin.bounded_stale_read_accepted);
    assert!(admin.bounded_stale_read_rejected);
    assert!(admin.minority_partition_rejected_reads);
    assert!(admin.minority_partition_rejected_writes);
    assert!(admin.healed_follower_caught_up);
    assert!(admin.pre_vote_enforced);
    assert!(admin.pre_vote_process_evidence_observed);
    assert!(admin.pre_vote_requests > 0);
    assert!(admin.pre_vote_rejected > 0);
    assert_eq!(
        admin.stale_leader_lease_rejection_count,
        state.stale_leader_lease_rejected
    );
    assert_eq!(
        admin.lagging_follower_read_rejection_count,
        state.lagging_follower_read_rejected
    );
    assert_eq!(
        admin.minority_partition_write_rejection_count,
        state.minority_partition_write_rejected
    );
    assert_eq!(
        admin.healed_follower_catchup_count,
        state.healed_follower_catchup_observed
    );
    assert!(admin.read_index_requests >= 3);
    assert!(admin.read_index_rejected >= 2);
    let read_safety_capability = admin
        .capability_matrix
        .iter()
        .find(|item| item.capability == "lease_read_index_pre_vote_semantics")
        .expect("lease/read-index/pre-vote capability should be reported");
    assert!(
        read_safety_capability.ready,
        "{}",
        read_safety_capability.detail
    );
    assert!(read_safety_capability
        .detail
        .contains("stale_write_rejected=true"));
    assert!(read_safety_capability
        .detail
        .contains("minority_read_rejected=true"));
    assert!(read_safety_capability
        .detail
        .contains("minority_write_rejected=true"));
    assert!(read_safety_capability
        .detail
        .contains("healed_catchup=true"));
}

