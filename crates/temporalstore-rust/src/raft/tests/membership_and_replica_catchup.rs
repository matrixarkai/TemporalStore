// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 3, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn raft_config_matches_defaults_and_validates_required_limits() {
    let config = RaftConfig::default();
    assert_eq!(config.election_cycle_tick, 3);
    assert_eq!(config.max_apply_batch_bytes, 64 * 1024);
    assert_eq!(config.max_cache_memory_bytes, 32 * 1024 * 1024);
    assert_eq!(config.raft_transport_timeout_ms, 1_000);
    assert_eq!(config.max_segment_bytes, 64 * 1024 * 1024);
    assert!(!config.wal_sync);
    assert!(config.assume_lease_when_start);
    assert!(config.can_trigger_snapshot);
    assert!(config.validate().is_ok());

    let mut invalid = config;
    invalid.max_memory_replicate_log_bytes = 0;
    assert_eq!(
        invalid.validate(),
        Err(RaftConfigError::InvalidValue(
            "max_memory_replicate_log_bytes"
        ))
    );
}

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_election_controls
#[test]
fn raft_config_rejects_oversized_log_entries_and_prohibited_elections() {
    let mut config = RaftConfig {
        max_memory_replicate_log_bytes: 16,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config.clone()).unwrap();
    let err = cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: vec![b'x'; 128],
        })
        .unwrap_err();
    assert!(matches!(err, RaftError::LogEntryTooLarge { .. }));

    config.max_memory_replicate_log_bytes = 1024;
    config.prohibits_election = true;
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
    assert_eq!(cluster.elect_leader(2), Err(RaftError::ElectionProhibited));
}

#[test]
fn raft_chunks_large_sequence_add_under_default_entry_limit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let rows = long_sequence_rows(5_000);

    cluster
        .propose(Command::SequenceAdd {
            key: "long-sequence".to_string(),
            rows,
        })
        .unwrap();

    assert!(cluster.commit_index(1).unwrap() > 1);
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                2,
                Command::SequenceQuery {
                    key: "long-sequence".to_string(),
                    start_ms: 1_700_000_000_000,
                    end_ms: 1_700_000_999_999,
                    count: 5_000,
                    filters: vec![FeatureFilter {
                        field: "action_type".to_string(),
                        op: FeatureFilterOp::GreaterThan,
                        value: 2,
                    }],
                },
            )
            .unwrap(),
        CommandResponse::SequenceRows {
            rows: long_sequence_rows(5_000)
                .into_iter()
                .filter(|row| row.action_type > 2)
                .collect()
        }
    );
}

#[test]
fn distributed_raft_chunks_large_sequence_add_under_default_entry_limit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let rows = long_sequence_rows(5_000);

    cluster
        .propose_distributed(
            Command::SequenceAdd {
                key: "distributed-long-sequence".to_string(),
                rows,
            },
            &cluster,
        )
        .unwrap();

    assert!(cluster.commit_index(1).unwrap() > 1);
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::SequenceQuery {
                    key: "distributed-long-sequence".to_string(),
                    start_ms: 1_700_000_000_000,
                    end_ms: 1_700_000_999_999,
                    count: 5_000,
                    filters: vec![FeatureFilter {
                        field: "duration".to_string(),
                        op: FeatureFilterOp::LessThan,
                        value: 10,
                    }],
                },
            )
            .unwrap(),
        CommandResponse::SequenceRows {
            rows: long_sequence_rows(5_000)
                .into_iter()
                .filter(|row| row.duration < 10)
                .collect()
        }
    );
}

#[test]
fn raft_read_options_enforce_leader_and_follower_read_paths() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    assert_eq!(
        cluster.check_read(2, RaftReadOptions::default()),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert!(cluster
        .check_read(
            2,
            RaftReadOptions {
                enable_read_from_follower: true,
                strategy: RaftReadStrategy::ReadIndex,
                ..RaftReadOptions::default()
            },
        )
        .is_ok());
    assert!(cluster
        .check_read(
            1,
            RaftReadOptions {
                strategy: RaftReadStrategy::LeaseRead,
                ..RaftReadOptions::default()
            },
        )
        .is_ok());
}

#[test]
fn data_raft_read_policy_matches_partition_manager_modes() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    assert_eq!("leader".parse(), Ok(DataRaftReadMode::Leader));
    assert_eq!("linearizable".parse(), Ok(DataRaftReadMode::Linearizable));
    assert_eq!("bounded_stale".parse(), Ok(DataRaftReadMode::BoundedStale));
    assert_eq!(
        "unsafe_any_replica".parse(),
        Ok(DataRaftReadMode::UnsafeAnyReplica)
    );
    assert!("bad-mode".parse::<DataRaftReadMode>().is_err());

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "policy".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert_eq!(
        cluster.check_data_raft_read_policy(2, DataRaftReadPolicy::default()),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert_eq!(
        cluster.check_data_raft_read_policy(
            2,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::Linearizable,
                ..DataRaftReadPolicy::default()
            },
        ),
        Err(RaftError::NotLeader { node_id: 2 })
    );
    assert!(cluster
        .check_data_raft_read_policy(
            1,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::Linearizable,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
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
            replica_commit_index: 0,
            leader_commit_index: 1,
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
    assert!(cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::UnsafeAnyReplica,
                ..DataRaftReadPolicy::default()
            },
        )
        .is_ok());
}

// shared-corpus: raft_matrixraft_read_safety_policy
// shared-corpus: raft_matrixraft_read_lease_fault_matrix
#[test]
fn raft_read_index_and_transfer_reject_lagging_replica() {
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
        cluster.read_index(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );
    assert_eq!(
        cluster.transfer_leader(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );
    cluster.catch_up(3).unwrap();
    assert!(cluster.read_index(3).is_ok());
    assert!(cluster.transfer_leader(3).is_ok());
}

#[test]
fn raft_wait_for_applied_index_matches_backend_contract() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "wait-applied".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert!(cluster.wait_for_applied_index(1, 1, 0).is_ok());
    assert_eq!(
        cluster.wait_for_applied_index(3, 1, 1),
        Err(RaftError::AppliedIndexTimeout {
            node_id: 3,
            applied_index: 0,
            target_index: 1,
            timeout_ms: 1,
        })
    );

    cluster.catch_up(3).unwrap();
    assert!(cluster.wait_for_applied_index(3, 1, 0).is_ok());
}

#[test]
fn raft_apply_health_reports_commit_to_apply_lag() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "apply-health".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    {
        let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
        let node = inner.nodes.get_mut(&2).unwrap();
        node.applied_index = 0;
        node.applied.clear();
    }

    let health = cluster.apply_health(0);
    assert!(!health.healthy);
    assert_eq!(health.leader_commit_index, 1);
    assert_eq!(health.max_apply_lag, 1);
    assert_eq!(health.fully_applied_nodes, vec![1, 3]);
    assert_eq!(
        health.slow_appliers,
        vec![RaftApplyLag {
            node_id: 2,
            commit_index: 1,
            applied_index: 0,
            apply_lag: 1,
            alive: true,
        }]
    );
    assert!(cluster.prometheus_metrics().contains(
        "temporalstore_raft_node_apply_lag{kind=\"data\",node_id=\"2\",role=\"follower\",replica_role=\"voter\"} 1"
    ));

    cluster.catch_up(2).unwrap();
    let healthy = cluster.apply_health(0);
    assert!(healthy.healthy);
    assert_eq!(healthy.max_apply_lag, 0);
}

#[test]
fn raft_apply_health_excludes_dead_replicas_behind_leader_commit() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "before-dead".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "after-dead".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    let health = cluster.apply_health(0);
    assert!(health.healthy);
    assert_eq!(health.leader_commit_index, 2);
    assert_eq!(health.max_apply_lag, 0);
    assert_eq!(health.fully_applied_nodes, vec![1, 2]);
    assert!(health.slow_appliers.is_empty());
}

#[test]
fn secondary_is_promoted_automatically_when_primary_is_down() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(1, false).unwrap();
    let promoted = cluster.promote_if_leader_down().unwrap();
    assert_eq!(promoted, 2);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"after-promotion".to_vec(),
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
            value: Some(b"after-promotion".to_vec())
        }
    );
}

#[test]
fn scale_up_adds_caught_up_replica() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"before-scale-up".to_vec(),
        })
        .unwrap();
    cluster.add_node(4).unwrap();
    assert_eq!(cluster.commit_index(4).unwrap(), 1);
    let response = cluster
        .read_local(
            4,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"before-scale-up".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn learner_and_witness_roles_match_membership_shape() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();

    let status = cluster.status();
    assert_eq!(status.majority, 3);
    assert_eq!(status.live_voters, 4);
    assert_eq!(
        cluster.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Learner
    );
    assert_eq!(
        cluster.local_status(5).unwrap().replica_role,
        RaftReplicaRole::Witness
    );
    assert_eq!(cluster.membership().voters, vec![1, 2, 3, 5]);

    cluster
        .propose(Command::StringSet {
            key: "role-k".to_string(),
            value: b"role-v".to_vec(),
        })
        .unwrap();
    assert_eq!(
        cluster.read_from_replica(
            4,
            Command::StringGet {
                key: "role-k".to_string()
            }
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"role-v".to_vec())
        })
    );
    assert_eq!(
        cluster.read_from_replica(
            5,
            Command::StringGet {
                key: "role-k".to_string()
            }
        ),
        Err(RaftError::NodeNotFound(5))
    );
    assert!(matches!(
        cluster.elect_leader(4),
        Err(RaftError::NodeNotFound(4))
    ));
    assert!(matches!(
        cluster.elect_leader(5),
        Err(RaftError::NodeNotFound(5))
    ));
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn learner_does_not_count_for_majority_but_witness_does() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2]);
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster.set_alive(2, false).unwrap();
    assert_eq!(
        cluster
            .propose(Command::StringSet {
                key: "k".to_string(),
                value: b"v".to_vec(),
            })
            .unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    );

    cluster.set_alive(2, true).unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();
    cluster.set_alive(2, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.status().live_voters, 2);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn replica_roles_survive_wal_restore() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3, 4, 5],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(
        restored.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Learner
    );
    assert_eq!(
        restored.local_status(5).unwrap().replica_role,
        RaftReplicaRole::Witness
    );
    assert_eq!(restored.membership().voters, vec![1, 2, 3, 5]);
}

// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
#[test]
fn matrixraft_admin_reports_witness_auto_promote_and_pending_joint_consensus() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();
    cluster.add_learner_with_auto_promote(4, true).unwrap();
    cluster.add_node(6).unwrap();
    cluster.remove_node(6).unwrap();
    cluster.begin_leader_transfer(2).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "membership-transfer-exact-once".to_string(),
            value: b"committed-once".to_vec(),
        })
        .unwrap();
    cluster.begin_joint_consensus([1, 2, 3, 4, 5]).unwrap();
    let cluster = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3, 4, 5],
        RaftConfig {
            enable_pre_vote: true,
            ..RaftConfig::default()
        },
    )
    .unwrap();

    let admin = cluster.matrixraft_runtime_admin_report();
    assert!(admin.witness_membership_present);
    assert!(admin.witness_role_behavior_present);
    assert!(admin.learner_add_present);
    assert!(admin.learner_catchup_present);
    assert!(admin.learner_promote_present);
    assert!(admin.voter_remove_present);
    assert!(admin.learner_auto_promote_present);
    assert!(admin.leader_transfer_exact_once_present);
    assert!(admin.pending_joint_consensus_present);
    assert!(admin.pending_joint_consensus_restart_present);
    assert_eq!(admin.membership_evidence.learner_add_count, 1);
    assert_eq!(admin.membership_evidence.learner_promote_count, 1);
    assert_eq!(admin.membership_evidence.voter_remove_count, 1);
    assert_eq!(admin.membership_evidence.leader_transfer_write_count, 1);
    assert_eq!(
        admin
            .membership_evidence
            .leader_transfer_exact_once_commit_count,
        1
    );
    assert_eq!(
        admin
            .membership_evidence
            .leader_transfer_exact_once_commit_ids,
        vec![1]
    );
    assert_eq!(
        cluster.read_local(
            1,
            Command::StringGet {
                key: "membership-transfer-exact-once".to_string(),
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"committed-once".to_vec()),
        })
    );
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "membership_role_semantics" && item.ready));
    let local = cluster.matrixraft_local_status_report();
    assert_eq!(local.wal_first_log_index, admin.wal_first_log_index);
    assert_eq!(local.wal_last_log_index, admin.wal_last_log_index);
    assert!(local.witness_membership_present);
    assert!(local.learner_auto_promote_present);
    assert!(local.pending_joint_consensus.is_some());
    assert!(local.peers.iter().any(|peer| peer.status.node_id == 5
        && peer.status.replica_role == RaftReplicaRole::Witness
        && peer.participates_in_quorum
        && !peer.can_serve_data
        && !peer.can_be_leader));
    let metrics = cluster.prometheus_metrics();
    for metric in [
        "temporalstore_raft_matrixraft_local_wal_first_log_index",
        "temporalstore_raft_matrixraft_local_wal_last_log_index",
        "temporalstore_raft_matrixraft_local_peer_match_index",
        "temporalstore_raft_matrixraft_local_peer_next_index",
        "temporalstore_raft_matrixraft_local_peer_snapshot_sending",
        "temporalstore_raft_matrixraft_local_peer_snapshot_installing",
        "temporalstore_raft_matrixraft_local_peer_snapshot_installed_index",
        "temporalstore_raft_matrixraft_local_peer_transfer_leader_target",
        "temporalstore_raft_matrixraft_local_peer_pre_vote_rejections",
        "temporalstore_raft_matrixraft_local_peer_election_rejections",
        "temporalstore_raft_matrixraft_learner_add_present",
        "temporalstore_raft_matrixraft_learner_catchup_present",
        "temporalstore_raft_matrixraft_learner_promote_present",
        "temporalstore_raft_matrixraft_voter_remove_present",
        "temporalstore_raft_matrixraft_witness_role_behavior_present",
        "temporalstore_raft_matrixraft_leader_transfer_exact_once_present",
        "temporalstore_raft_matrixraft_pending_joint_consensus_restart_present",
        "temporalstore_raft_matrixraft_membership_learner_add_count",
        "temporalstore_raft_matrixraft_membership_voter_remove_count",
        "temporalstore_raft_matrixraft_membership_leader_transfer_exact_once_commit_count",
        "temporalstore_raft_matrixraft_membership_leader_transfer_exact_once_commit_id_count",
    ] {
        assert!(
            metrics.contains(metric),
            "missing local-status metric {metric}"
        );
    }
}

// shared-corpus: raft_matrixraft_leader_election_learner_promotion_parity
#[test]
fn matrixraft_leader_election_and_learner_promotion_parity_report_is_ready() {
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
    cluster.set_alive(3, true).unwrap();
    assert_eq!(
        cluster.tick_election().unwrap(),
        RaftTickOutcome::LeaderElected {
            leader_id: 2,
            term: 2,
        }
    );

    cluster.add_learner_with_auto_promote(4, true).unwrap();
    cluster.begin_leader_transfer(4).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "leader-election-learner-promotion".to_string(),
            value: b"committed-once".to_vec(),
        })
        .unwrap();

    let report = cluster.matrixraft_leader_election_parity_report();
    assert!(report.ready, "report blockers: {:?}", report.blockers);
    assert!(report.leader_election_ready);
    assert!(report.pre_vote_ready);
    assert!(report.leader_failover_observed);
    assert!(report.learner_add_ready);
    assert!(report.learner_catchup_ready);
    assert!(report.learner_promote_ready);
    assert!(report.learner_auto_promote_ready);
    assert!(report.membership_ready);
    assert!(report.leader_transfer_exact_once_ready);
    assert_eq!(report.leader_id, 2);
    assert_eq!(report.current_term, 2);
    assert_eq!(report.pre_vote_requests, 2);
    assert_eq!(report.pre_vote_accepted, 1);
    assert_eq!(report.pre_vote_rejected, 1);
    assert_eq!(report.learner_add_count, 1);
    assert_eq!(report.learner_catchup_count, 1);
    assert_eq!(report.learner_promote_count, 1);
    assert_eq!(report.auto_promote_count, 1);
    assert_eq!(report.leader_transfer_exact_once_commit_count, 1);
    assert_eq!(
        cluster.read_local(
            4,
            Command::StringGet {
                key: "leader-election-learner-promotion".to_string(),
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"committed-once".to_vec()),
        })
    );
}

#[test]
fn replication_health_reports_lag_and_heartbeat_catches_up_secondary() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "lag-key".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let health = cluster.replication_health(0);
    assert!(!health.healthy);
    assert_eq!(health.max_lag, 1);
    assert_eq!(
        health.lagging_voters,
        vec![RaftReplicaLag {
            node_id: 3,
            lag: 1,
            alive: true,
        }]
    );

    let caught_up = cluster.catch_up_live_followers().unwrap();
    assert_eq!(caught_up, vec![2, 3]);
    let health = cluster.replication_health(0);
    assert!(health.healthy);
    assert_eq!(health.caught_up_voters, vec![1, 2, 3]);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "lag-key".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
}

#[test]
fn bounded_replica_catchup_replays_limited_log_entries_per_loop() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: "bounded-catchup".to_string(),
                value: format!("v{index}").into_bytes(),
            })
            .unwrap();
    }
    cluster.set_alive(3, true).unwrap();

    let first = cluster.catch_up_live_followers_bounded(1).unwrap();
    assert_eq!(first.replayed_log_entries, 1);
    assert_eq!(first.leader_commit_index, 3);
    assert_eq!(
        first.lagging_voters,
        vec![RaftReplicaLag {
            node_id: 3,
            lag: 2,
            alive: true,
        }]
    );
    assert_eq!(cluster.local_status(3).unwrap().commit_index, 1);

    let second = cluster.catch_up_live_followers_bounded(1).unwrap();
    assert_eq!(second.replayed_log_entries, 1);
    assert_eq!(cluster.local_status(3).unwrap().commit_index, 2);

    let third = cluster.catch_up_live_followers_bounded(1).unwrap();
    assert_eq!(third.replayed_log_entries, 1);
    assert_eq!(third.lagging_voters, Vec::<RaftReplicaLag>::new());
    assert_eq!(third.caught_up_voters, vec![1, 2, 3]);
    assert_eq!(
        cluster.read_from_replica(
            3,
            Command::StringGet {
                key: "bounded-catchup".to_string()
            }
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        })
    );

    assert!(matches!(
        cluster.catch_up_live_followers_bounded(0).unwrap_err(),
        RaftError::InvalidConfig(_)
    ));
}

#[test]
fn safe_scale_up_adds_replica_only_after_catchup() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "scale-up-safe".to_string(),
            value: b"ready".to_vec(),
        })
        .unwrap();

    let report = cluster.add_node_safely(4).unwrap();
    assert_eq!(report.voters, vec![1, 2, 3, 4]);
    assert_eq!(report.majority, 3);
    assert_eq!(report.caught_up_voters, vec![1, 2, 3, 4]);
    assert_eq!(
        cluster.commit_index(4).unwrap(),
        cluster.status().commit_index
    );
    assert!(cluster.read_index(4).is_ok());
}

#[test]
fn safe_membership_change_adds_voter_through_joint_consensus() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "membership-add".to_string(),
            value: b"before".to_vec(),
        })
        .unwrap();

    let plan = cluster.plan_membership_change([1, 2, 3, 4]).unwrap();
    assert_eq!(plan.kind, RaftMembershipChangeKind::AddVoter);
    assert_eq!(plan.old_voters, vec![1, 2, 3]);
    assert_eq!(plan.new_voters, vec![1, 2, 3, 4]);
    assert_eq!(plan.add_voters, vec![4]);
    assert!(plan.remove_voters.is_empty());

    let report = cluster
        .apply_membership_change_safely([1, 2, 3, 4])
        .unwrap();
    assert_eq!(report.plan, plan);
    assert_eq!(report.joint_membership.old_voters, vec![1, 2, 3]);
    assert_eq!(report.joint_membership.new_voters, vec![1, 2, 3, 4]);
    assert_eq!(report.committed_membership.voters, vec![1, 2, 3, 4]);
    assert_eq!(report.caught_up_voters, vec![2, 3, 4]);
    assert_eq!(
        cluster.read_from_replica(
            4,
            Command::StringGet {
                key: "membership-add".to_string()
            }
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"before".to_vec())
        })
    );
}

#[test]
fn safe_membership_change_removes_leader_after_caught_up_successor_exists() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "membership-remove-leader".to_string(),
            value: b"before".to_vec(),
        })
        .unwrap();

    let report = cluster.apply_membership_change_safely([2, 3]).unwrap();
    assert_eq!(report.plan.kind, RaftMembershipChangeKind::RemoveVoter);
    assert_eq!(report.plan.remove_voters, vec![1]);
    assert_eq!(report.committed_membership.voters, vec![2, 3]);
    assert_ne!(report.leader_id, 1);
    assert_eq!(cluster.commit_index(1), Err(RaftError::NodeNotFound(1)));
    cluster
        .propose(Command::StringSet {
            key: "membership-after-leader-remove".to_string(),
            value: b"after".to_vec(),
        })
        .unwrap();
}

#[test]
fn safe_membership_change_replaces_voter_and_rejects_invalid_targets() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);

    assert!(matches!(
        cluster.plan_membership_change([1, 2, 3]).unwrap_err(),
        RaftError::InvalidConfig(_)
    ));
    assert_eq!(
        cluster.plan_membership_change([]).unwrap_err(),
        RaftError::CannotRemoveLastNode
    );

    cluster.set_alive(2, false).unwrap();
    assert_eq!(
        cluster.apply_membership_change_safely([2, 3]).unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    );
    cluster.set_alive(2, true).unwrap();

    let report = cluster.apply_membership_change_safely([1, 2, 4]).unwrap();
    assert_eq!(report.plan.kind, RaftMembershipChangeKind::ReplaceVoter);
    assert_eq!(report.plan.add_voters, vec![4]);
    assert_eq!(report.plan.remove_voters, vec![3]);
    assert_eq!(report.committed_membership.voters, vec![1, 2, 4]);
    assert_eq!(cluster.commit_index(3), Err(RaftError::NodeNotFound(3)));
    assert!(cluster.read_index(4).is_ok());
}

#[test]
fn topology_membership_planner_maps_metaserver_replicas_to_raft_voters() {
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let topology = topology_for_shard(7, "s1", ["s1", "s2", "s4"]);
    let servers = vec![
        server_meta("s1", 1, MetaEntityState::Normal),
        server_meta("s2", 2, MetaEntityState::Normal),
        server_meta("s3", 3, MetaEntityState::Normal),
        server_meta("s4", 4, MetaEntityState::Normal),
    ];

    let plan = plan_data_raft_membership_from_topology(&cluster, &topology, &servers, 7).unwrap();

    assert!(!plan.no_change);
    assert_eq!(plan.target_servers, vec!["s1", "s2", "s4"]);
    assert_eq!(plan.target_voters, vec![1, 2, 4]);
    let membership = plan.membership_change.unwrap();
    assert_eq!(membership.kind, RaftMembershipChangeKind::ReplaceVoter);
    assert_eq!(membership.add_voters, vec![4]);
    assert_eq!(membership.remove_voters, vec![3]);
}

#[test]
fn topology_membership_planner_reports_noop_and_rejects_bad_servers() {
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let topology = topology_for_shard(7, "s1", ["s1", "s2", "s3"]);
    let servers = vec![
        server_meta("s1", 1, MetaEntityState::Normal),
        server_meta("s2", 2, MetaEntityState::Normal),
        server_meta("s3", 3, MetaEntityState::Normal),
    ];

    let plan = plan_data_raft_membership_from_topology(&cluster, &topology, &servers, 7).unwrap();

    assert!(plan.no_change);
    assert_eq!(plan.target_voters, vec![1, 2, 3]);
    assert!(plan.membership_change.is_none());

    let frozen_servers = vec![
        server_meta("s1", 1, MetaEntityState::Normal),
        server_meta("s2", 2, MetaEntityState::Frozen),
        server_meta("s3", 3, MetaEntityState::Normal),
    ];
    assert!(matches!(
        plan_data_raft_membership_from_topology(&cluster, &topology, &frozen_servers, 7),
        Err(RaftError::InvalidConfig(message)) if message.contains("non-normal server s2")
    ));
}

#[test]
fn topology_membership_apply_updates_data_raft_voters() {
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let topology = topology_for_shard(7, "s1", ["s1", "s2", "s4"]);
    let servers = vec![
        server_meta("s1", 1, MetaEntityState::Normal),
        server_meta("s2", 2, MetaEntityState::Normal),
        server_meta("s3", 3, MetaEntityState::Normal),
        server_meta("s4", 4, MetaEntityState::Normal),
    ];

    let report =
        apply_data_raft_membership_from_topology(&cluster, &topology, &servers, 7).unwrap();

    assert!(report.applied);
    assert_eq!(report.plan.target_servers, vec!["s1", "s2", "s4"]);
    assert_eq!(report.plan.target_voters, vec![1, 2, 4]);
    let membership = report.membership_report.unwrap();
    assert_eq!(membership.plan.kind, RaftMembershipChangeKind::ReplaceVoter);
    assert_eq!(membership.committed_membership.voters, vec![1, 2, 4]);
    assert_eq!(
        cluster
            .status()
            .nodes
            .into_iter()
            .filter(|node| node.replica_role.participates_in_quorum())
            .map(|node| node.node_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 4]
    );
}

#[test]
fn topology_membership_apply_is_noop_when_voters_match() {
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let topology = topology_for_shard(7, "s1", ["s1", "s2", "s3"]);
    let servers = vec![
        server_meta("s1", 1, MetaEntityState::Normal),
        server_meta("s2", 2, MetaEntityState::Normal),
        server_meta("s3", 3, MetaEntityState::Normal),
    ];

    let report =
        apply_data_raft_membership_from_topology(&cluster, &topology, &servers, 7).unwrap();

    assert!(!report.applied);
    assert!(report.membership_report.is_none());
    assert!(report.plan.no_change);
    assert_eq!(cluster.status().commit_index, 0);
}

#[test]
fn scale_down_removes_replica_and_continues_with_majority() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.remove_node(3).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"after-scale-down".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.commit_index(1).unwrap(), 1);
    assert_eq!(cluster.commit_index(2).unwrap(), 1);
    assert_eq!(cluster.commit_index(3), Err(RaftError::NodeNotFound(3)));
}

#[test]
fn safe_scale_down_rejects_quorum_loss_and_promotes_caught_up_leader_successor() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(2, false).unwrap();
    assert_eq!(
        cluster.remove_node_safely(3).unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2,
        }
    );

    cluster.set_alive(2, true).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "before-leader-remove".to_string(),
            value: b"ok".to_vec(),
        })
        .unwrap();
    let report = cluster.remove_node_safely(1).unwrap();
    assert_ne!(report.leader_id, 1);
    assert_eq!(report.voters, vec![2, 3]);
    assert_eq!(report.majority, 2);
    assert!(report.caught_up_voters.contains(&report.leader_id));
    cluster
        .propose(Command::StringSet {
            key: "after-leader-remove".to_string(),
            value: b"still-ok".to_vec(),
        })
        .unwrap();
}

#[test]
fn primary_crash_promotes_caught_up_secondary_and_old_primary_recovers() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "before-crash".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.set_alive(1, false).unwrap();

    let report = cluster.failover_primary().unwrap();
    assert_eq!(report.old_leader_id, 1);
    assert_eq!(report.new_leader_id, 2);
    assert_eq!(report.commit_index, 1);
    assert_eq!(cluster.leader_id(), 2);
    cluster
        .propose(Command::StringSet {
            key: "after-crash".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    cluster.set_alive(1, true).unwrap();
    assert_eq!(cluster.local_status(1).unwrap().lag, 1);
    cluster.catch_up_live_followers().unwrap();
    assert_eq!(cluster.local_status(1).unwrap().lag, 0);
    assert_eq!(
        cluster
            .read_from_replica(
                1,
                Command::StringGet {
                    key: "after-crash".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v2".to_vec())
        }
    );
}

#[test]
fn raft_snapshot_bootstraps_lagging_data_replica_then_catches_up_logs() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k1".to_string(),
            value: b"snapshot-value".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();

    cluster.set_alive(3, true).unwrap();
    cluster.install_snapshot(3, snapshot).unwrap();
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
    assert_eq!(
        cluster
            .read_local(
                3,
                Command::StringGet {
                    key: "k1".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"snapshot-value".to_vec())
        }
    );

    cluster
        .propose(Command::StringSet {
            key: "k2".to_string(),
            value: b"post-snapshot-log".to_vec(),
        })
        .unwrap();
    cluster.catch_up(3).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "k2".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"post-snapshot-log".to_vec())
        }
    );
}

#[test]
fn raft_snapshot_lifecycle_report_installs_and_replays_tail() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-lifecycle-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-lifecycle-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();

    cluster.set_alive(3, true).unwrap();
    let report = cluster.install_snapshot_with_lifecycle_report(3, snapshot);
    assert_eq!(report.node_id, 3);
    assert_eq!(report.snapshot_index, 1);
    assert_eq!(report.before_commit_index, 0);
    assert_eq!(report.after_commit_index, 2);
    assert!(report.freeze_started);
    assert!(report.flush_completed);
    assert!(report.manifest_verified);
    assert!(report.checksum_verified);
    assert!(report.install_completed);
    assert!(report.tail_replay_completed);
    assert!(!report.rollback_performed);
    assert!(report.error.is_none());
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "snapshot-lifecycle-b".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"b".to_vec())
        }
    );
}

#[test]
fn raft_snapshot_lifecycle_report_rolls_back_stale_snapshot() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "snapshot-lifecycle-stale-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-lifecycle-stale-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();
    cluster.catch_up(3).unwrap();
    let before = cluster.commit_index(3).unwrap();

    let report = cluster.install_snapshot_with_lifecycle_report(3, snapshot);
    assert_eq!(report.before_commit_index, before);
    assert_eq!(report.after_commit_index, before);
    assert!(report.freeze_started);
    assert!(!report.flush_completed);
    assert!(!report.manifest_verified);
    assert!(!report.checksum_verified);
    assert!(!report.install_completed);
    assert!(!report.tail_replay_completed);
    assert!(report.rollback_performed);
    assert!(report
        .error
        .as_deref()
        .unwrap_or_default()
        .contains("stale snapshot"));
    assert_eq!(cluster.commit_index(3).unwrap(), before);
}

#[test]
fn raft_snapshot_only_replica_keeps_commit_index_after_empty_heartbeat() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-only".to_string(),
            value: b"snapshot-value".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();

    cluster.set_alive(3, true).unwrap();
    cluster.install_snapshot(3, snapshot).unwrap();
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
    assert_eq!(cluster.local_status(3).unwrap().last_log_index, 1);

    let heartbeat = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 1,
        prev_log_term: 1,
        entries: Vec::new(),
        leader_commit: 1,
    };
    let response = cluster.receive_append_entries(heartbeat).unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 1);
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "snapshot-only".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"snapshot-value".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_follower_rejoin_compacted_logs_fault_harness
#[test]
fn append_entries_matches_snapshot_floor_after_leader_compaction() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-floor".to_string(),
            value: b"before-a".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-floor".to_string(),
            value: b"before-b".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 2);

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-floor".to_string(),
            value: b"after".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let request = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(request.prev_log_index, 2);
    assert_eq!(request.prev_log_term, 1);
    assert_eq!(
        request
            .entries
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![3]
    );
    let response = cluster.receive_append_entries(request).unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 3);
    assert_eq!(cluster.commit_index(3).unwrap(), 3);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "snapshot-floor".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"after".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_follower_rejoin_compacted_logs_fault_harness
#[test]
fn matrixraft_follower_rejoin_after_compaction_installs_snapshot_and_replays_tail() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted-rejoin-base".to_string(),
            value: b"base".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted-rejoin-base".to_string(),
            value: b"snapshotted".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 2);
    let compacted_snapshot = cluster.create_snapshot().unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted-rejoin-tail".to_string(),
            value: b"tail".to_vec(),
        })
        .unwrap();

    cluster.set_alive(3, true).unwrap();
    assert_eq!(
        cluster.read_index(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 3,
        }
    );

    cluster.install_snapshot(3, compacted_snapshot).unwrap();
    assert_eq!(cluster.commit_index(3).unwrap(), 2);
    assert_eq!(
        cluster.read_index(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 2,
            leader_commit_index: 3,
        }
    );
    cluster.catch_up(3).unwrap();
    assert!(cluster.read_index(3).is_ok());
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "compacted-rejoin-base".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"snapshotted".to_vec())
        }
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "compacted-rejoin-tail".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"tail".to_vec())
        }
    );
}

// The metaserver-driven raft auto-failover feature (TS_RAFT_AUTO_FAILOVER) works
// by POSTing the dead leader's liveness and a native failover request to a
// surviving replica. These tests exercise exactly the two raft primitives that
// path invokes — `set_alive(dead, false)` (what `/raft/admin/liveness` calls) and
// `failover_primary()` (what `/raft/admin/failover` calls) — proving the group
// re-elects and writes resume, and that the election refuses without a majority.

#[test]
fn metaserver_driven_failover_reelects_and_resumes_writes() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    // A committed write exists before the leader dies; the promoted follower must
    // retain it (leader-completeness).
    cluster
        .propose(Command::StringSet {
            key: "before-failover".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.leader_id(), 1);

    // Metaserver detects node 1 stale -> POST /raft/admin/liveness {1, false}.
    cluster.set_alive(1, false).unwrap();
    // Metaserver -> POST /raft/admin/failover -> native promotion of the best
    // caught-up live follower (guarded by elect_leader's majority + log checks).
    let report = cluster.failover_primary().unwrap();
    assert_eq!(report.old_leader_id, 1);
    assert_ne!(report.new_leader_id, 1);
    assert_ne!(report.new_leader_id, 0);
    assert_eq!(cluster.leader_id(), report.new_leader_id);

    // Writes resume on the freshly elected leader...
    cluster
        .propose(Command::StringSet {
            key: "after-failover".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();
    // ...and the pre-failover committed write survived the leadership change.
    assert_eq!(
        cluster
            .read_from_replica(
                report.new_leader_id,
                Command::StringGet {
                    key: "before-failover".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
}

#[test]
fn metaserver_driven_failover_refuses_without_a_live_majority() {
    // Split-brain guard: if the metaserver only reaches a minority of the group,
    // the native failover must refuse to elect rather than fabricate a leader.
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(1, false).unwrap();
    cluster.set_alive(2, false).unwrap();
    // Only node 3 is alive: 1 live < 2 required, so no election is possible.
    assert_eq!(
        cluster.failover_primary().unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    );
    // Leadership is unchanged; no split-brain second leader was created.
    assert_eq!(cluster.leader_id(), 1);
}

