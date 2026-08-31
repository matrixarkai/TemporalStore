// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 4, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

#[test]
fn a_proxy_group_needs_a_name_on_the_raft_path_too() {
    use crate::meta::{PutProxyGroupRequest, SingleNodeMeta};

    // put_proxy_group validates in the public method, and the propose path
    // dispatches straight to apply_put_proxy_group, which does not. So a raft
    // metaserver committed a group with no name and no namespace into
    // replicated metadata, where the single-node one answered bad_request.
    //
    // Judged before proposing, never while applying: replay has to reapply what
    // was already accepted.
    let empty = || PutProxyGroupRequest {
        drop_percent: 0,
        group: String::new(),
        namespace: String::new(),
        location: "rack-1".to_string(),
        instance_num: 1,
    };

    let single = SingleNodeMeta::default();
    let single_ack = single.put_proxy_group(empty());
    assert_eq!(single_ack.status.code, "bad_request");
    assert!(single.list_proxy_groups().groups.is_empty());

    let meta = MetaRaftCluster::new([10, 11, 12]);
    let raft_ack = meta.put_proxy_group(empty());
    assert_eq!(
        raft_ack.status.code, "bad_request",
        "the raft path accepted a proxy group with no name"
    );
    assert!(
        meta.list_proxy_groups().groups.is_empty(),
        "a nameless proxy group reached replicated metadata"
    );

    // A named group still goes through, so the guard is not simply refusing.
    let good = meta.put_proxy_group(PutProxyGroupRequest {
        drop_percent: 0,
        group: "orders".to_string(),
        namespace: "ns".to_string(),
        location: "rack-1".to_string(),
        instance_num: 1,
    });
    assert!(good.status.ok, "a valid group was refused: {good:?}");
    assert_eq!(meta.list_proxy_groups().groups.len(), 1);
}

#[test]
fn add_node_after_leader_snapshot_installs_snapshot_and_tail() {
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
            key: "snapshotted-key".to_string(),
            value: b"base".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 1);
    cluster
        .propose(Command::StringSet {
            key: "tail-key".to_string(),
            value: b"tail".to_vec(),
        })
        .unwrap();

    cluster.add_node(4).unwrap();
    assert_eq!(cluster.commit_index(4).unwrap(), 2);
    assert_eq!(cluster.local_status(4).unwrap().last_log_index, 2);
    assert_eq!(
        cluster
            .read_from_replica(
                4,
                Command::StringGet {
                    key: "snapshotted-key".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"base".to_vec())
        }
    );
    assert_eq!(
        cluster
            .read_from_replica(
                4,
                Command::StringGet {
                    key: "tail-key".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"tail".to_vec())
        }
    );
}

// shared-corpus: raft_matrixraft_leader_transfer_high_write_fault_harness
#[test]
fn matrixraft_leader_transfer_under_high_write_load_commits_once() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard_with_wal(
        dir.path(),
        1,
        [1, 2, 3],
        RaftConfig {
            transfer_timeout_tick: 50,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    let mut accepted = Vec::new();

    for index in 0..24 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("before-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    cluster.begin_leader_transfer(2).unwrap();
    for index in 24..48 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("during-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    cluster.transfer_leader(2).unwrap();
    assert_eq!(cluster.leader_id(), 2);

    for index in 48..72 {
        let key = format!("transfer-load-{index:02}");
        let value = format!("after-{index:02}").into_bytes();
        cluster
            .propose(Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            })
            .unwrap();
        accepted.push((key, value, cluster.status().commit_index));
    }

    let mut commit_indexes = accepted
        .iter()
        .map(|(_, _, index)| *index)
        .collect::<Vec<_>>();
    commit_indexes.sort_unstable();
    assert_eq!(
        commit_indexes,
        (1..=accepted.len() as u64).collect::<Vec<_>>()
    );
    assert_eq!(cluster.status().commit_index, accepted.len() as u64);
    for replica_id in [1, 2, 3] {
        assert_eq!(
            cluster.commit_index(replica_id).unwrap(),
            accepted.len() as u64
        );
    }

    for replica_id in [1, 2, 3] {
        for (key, value, _) in &accepted {
            assert_eq!(
                cluster
                    .read_from_replica(replica_id, Command::StringGet { key: key.clone() },)
                    .unwrap(),
                CommandResponse::Bytes {
                    value: Some(value.clone())
                }
            );
        }
    }

    let admin = cluster.matrixraft_runtime_admin_report();
    let transferred_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 2)
        .expect("new leader peer status");
    assert_eq!(transferred_peer.transfer_leader_requests, 2);
    assert_eq!(transferred_peer.transfer_leader_accepted, 2);
    assert_eq!(transferred_peer.transfer_leader_completed, 1);
    assert_eq!(transferred_peer.transfer_leader_rejected, 0);
    assert_eq!(transferred_peer.transfer_leader_timeouts, 0);
    assert!(admin.admin_status_surface_complete);
    assert!(admin.wal_segment_lifecycle_present);
    assert!(admin.wal_last_log_index >= admin.commit_index);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "admin_status_surface" && item.ready));
}

// shared-corpus: raft_matrixraft_admin_status_surface
#[test]
fn matrixraft_admin_status_surface_requires_wal_and_peer_pipeline_fields() {
    let local_fixture = RaftCluster::new_single_shard(1, [1, 2, 3]);
    local_fixture
        .propose(Command::StringSet {
            key: "admin-without-wal".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let local_admin = local_fixture.matrixraft_runtime_admin_report();
    assert!(!local_admin.admin_status_surface_complete);
    assert!(!local_admin.wal_segment_lifecycle_present);
    assert!(local_admin.quorum_peer_progress_observed);
    assert!(local_admin.peer_pipeline_runtime_activity_observed);
    assert!(local_admin.peer_pipeline_limits_observed);
    assert!(local_admin
        .blockers
        .contains(&"admin_status_surface_incomplete".to_string()));

    let dir = tempfile::tempdir().unwrap();
    let durable =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    durable
        .propose(Command::StringSet {
            key: "admin-with-wal".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let durable_admin = durable.matrixraft_runtime_admin_report();
    assert!(durable_admin.wal_segment_lifecycle_present);
    assert!(durable_admin.quorum_peer_progress_observed);
    assert!(durable_admin.peer_pipeline_runtime_activity_observed);
    assert!(durable_admin.peer_pipeline_limits_observed);
    assert!(durable_admin.admin_status_surface_complete);
    assert!(durable_admin.wal_first_log_index > 0);
    assert!(durable_admin.wal_last_log_index >= durable_admin.commit_index);
    assert!(durable_admin.peer_pipeline_states.iter().all(|peer| {
        peer.next_index > 0
            && peer.append_queue_limit > 0
            && peer.inflight_bytes_limit > 0
            && peer.apply_inflight_limit > 0
            && peer.apply_batch_bytes_limit > 0
    }));
    assert!(durable_admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "admin_status_surface" && item.ready));
}

#[test]
fn append_entries_ignores_entries_at_or_below_snapshot_floor() {
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
            key: "compacted".to_string(),
            value: b"snapshot".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "compacted-tail".to_string(),
            value: b"before".to_vec(),
        })
        .unwrap();
    let snapshot_report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 2);

    let request = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 2,
        prev_log_term: 1,
        entries: vec![
            RaftLogEntry {
                term: 1,
                index: 2,
                shard_id: 1,
                command: Command::StringSet {
                    key: "compacted-tail".to_string(),
                    value: b"stale-should-not-replay".to_vec(),
                },
            },
            RaftLogEntry {
                term: 1,
                index: 3,
                shard_id: 1,
                command: Command::StringSet {
                    key: "compacted-tail".to_string(),
                    value: b"after".to_vec(),
                },
            },
        ],
        leader_commit: 3,
    };
    let response = cluster.receive_append_entries(request).unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 3);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "compacted-tail".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"after".to_vec())
        }
    );
}

#[test]
fn data_raft_snapshot_trigger_compacts_applied_log_bytes() {
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
            key: "trigger-data-snapshot".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let report = cluster.maybe_trigger_snapshot().unwrap();
    assert!(report.triggered);
    assert_eq!(report.reason, "applied_log_bytes_threshold");
    assert_eq!(report.applied_index, 1);
    assert!(report.applied_log_bytes >= 1);
    assert_eq!(cluster.local_status(1).unwrap().last_log_index, 1);

    let second = cluster.maybe_trigger_snapshot().unwrap();
    assert!(!second.triggered);
    assert_eq!(second.reason, "no_new_applied_logs");
    assert_eq!(second.last_snapshot_index, 1);
}

#[test]
fn raft_snapshot_cannot_overwrite_newer_data_state() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k1".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    let snapshot = cluster.create_snapshot().unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k2".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    assert_eq!(
        cluster.install_snapshot(2, snapshot).unwrap_err(),
        RaftError::StaleSnapshot {
            snapshot_index: 1,
            local_commit_index: 2,
        }
    );
}

#[test]
fn raft_election_does_not_depend_on_snapshot_availability() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "before".to_string(),
            value: b"leader-local".to_vec(),
        })
        .unwrap();
    cluster.set_alive(1, false).unwrap();
    assert_eq!(cluster.promote_if_leader_down().unwrap(), 2);
    cluster
        .propose(Command::StringSet {
            key: "after".to_string(),
            value: b"new-leader".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.commit_index(2).unwrap(), 2);
}

#[test]
fn metaserver_raft_replicates_shard_location_metadata() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let location = ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 1,
        server_addr: "127.0.0.1:17002".to_string(),
        latest_snapshot: None,
    };
    meta.propose(MetaCommand::PutShardLocation(location.clone()))
        .unwrap();
    for node_id in [10, 11, 12] {
        assert_eq!(
            meta.get_shard_location(node_id, 1).unwrap(),
            Some(location.clone())
        );
    }
}

#[test]
fn metaserver_raft_replays_scheduler_state_and_partition_set_topology() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let mut scheduler = DeterministicTaskScheduler::default();
    let task = scheduler.submit(
        0,
        100,
        SchedulerTaskKind::RebalanceStep(RebalanceStep::LoadTarget {
            shard_id: 42,
            replica_id: 2,
            node_id: 11,
            load_version: 9,
        }),
    );
    scheduler
        .run_next(
            200,
            SchedulerTaskResult::RetryLater,
            TaskSchedulerOptions::default(),
        )
        .unwrap();
    let topology = PartitionSetTopology::from_replicas(
        "ns",
        "tbl",
        7,
        700,
        42,
        12,
        5,
        3,
        &BTreeMap::from([
            (10, "127.0.0.1:18010".to_string()),
            (11, "127.0.0.1:18011".to_string()),
        ]),
        [
            ShardReplica {
                shard_id: 42,
                replica_id: 1,
                node_id: 10,
                role: ShardRole::Primary,
                state: ShardReplicaState::Normal,
                load_version: 8,
            },
            ShardReplica {
                shard_id: 42,
                replica_id: 2,
                node_id: 11,
                role: ShardRole::Secondary,
                state: ShardReplicaState::Normal,
                load_version: 9,
            },
        ],
    )
    .unwrap();
    let persisted = RaftPersistedSchedulerState {
        scheduler: scheduler.export_snapshot(),
        topology,
        executions: vec![NetworkSchedulerTaskExecution {
            task_id: task.id,
            target_node_id: 11,
            target_addr: "127.0.0.1:18011".to_string(),
            result: SchedulerTaskResult::RetryLater,
            retry_times: 1,
            next_run_time_ms: Some(1200),
            status: Status::error("node_request_failed", "timeout"),
            lifecycle_token: task.lifecycle_token(),
        }],
        raft_generation: 44,
    };

    meta.propose(MetaCommand::PersistSchedulerState(persisted.clone()))
        .unwrap();
    let snapshot = meta.create_snapshot().unwrap();
    assert_eq!(snapshot.state.scheduler_state.as_ref().unwrap(), &persisted);
    for node_id in [10, 11, 12] {
        assert_eq!(
            meta.status()
                .nodes
                .iter()
                .find(|node| node.node_id == node_id)
                .unwrap()
                .commit_index,
            snapshot.last_included_index
        );
    }
}

#[test]
fn metaserver_raft_replicates_full_metadata_mutation_api() {
    let meta = MetaRaftCluster::new([10, 11, 12]);

    assert!(
        meta.register_server(RegisterServerRequest {
            registered_at_ms: 0,
            numa_nodes: Vec::new(),
            server_addr: "server-a".to_string(),
            node_id: 1,
            location: "az-a".to_string(),
            binary_version: "test".to_string(),
        })
        .status
        .ok
    );
    assert!(
        meta.add_namespace(AddNamespaceRequest {
            namespace: "feature".to_string(),
        })
        .status
        .ok
    );
    assert!(
        meta.add_table(AddTableRequest {
            namespace: "feature".to_string(),
            table_name: "user_seq".to_string(),
            first_shard_id: 100,
            shard_count: 2,
            replica_count: 1,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        })
        .status
        .ok
    );
    assert!(
        meta.register(RegisterShardRequest {
            registered_at_ms: 0,
            shard_id: 100,
            server_addr: "server-a".to_string(),
        })
        .status
        .ok
    );

    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 4);
    }

    let topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(topology.status.ok);
    assert_eq!(topology.table.unwrap().shard_count, 2);
    assert_eq!(topology.shards.len(), 2);
    assert_eq!(topology.shards[0].primary.as_deref(), Some("server-a"));

    // A replica-count change, because shard_count is pinned once a shard is
    // registered against the table -- and this test is about UpdateTable
    // reaching every peer, which either change demonstrates.
    let updated = meta.update_table(UpdateTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        shard_count: None,
        replica_count: Some(2),
        first_shard_id: None,
        partition_version: None,
        serving_options: None,
    });
    assert!(updated.status.ok, "{updated:?}");
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 5);
    }
    let updated_topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(updated_topology.status.ok, "{updated_topology:?}");
    let updated_table = updated_topology.table.unwrap();
    assert_eq!(updated_table.shard_count, 2);
    assert_eq!(updated_table.replica_count, 2);
    assert_eq!(updated_topology.shards.len(), 2);

    let duplicate = meta.add_table(AddTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        first_shard_id: 100,
        shard_count: 2,
        replica_count: 1,
        partition_version: 0,
        serving_options: crate::meta::TableServingOptions::default(),
    });
    assert!(!duplicate.status.ok);
    assert_eq!(duplicate.status.code, "already_exists");

    let deleted = meta.delete_table(DeleteTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
    });
    assert!(deleted.status.ok, "{deleted:?}");
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 7);
    }
    let dropped_topology = meta.get_table_topology(GetTableTopologyRequest {
        client_location: String::new(),
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert_eq!(dropped_topology.status.code, "table_not_found");
    assert_eq!(
        dropped_topology.table.unwrap().state,
        MetaEntityState::Dropped
    );
}

#[test]
fn metaserver_raft_freeze_stale_server_is_replicated_mutation() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.register_server(RegisterServerRequest {
        registered_at_ms: 0,
        numa_nodes: Vec::new(),
        server_addr: "server-stale".to_string(),
        node_id: 1,
        location: "az-a".to_string(),
        binary_version: "test".to_string(),
    });
    thread::sleep(Duration::from_millis(2));

    let report = meta.freeze_stale_servers(0);
    assert!(report.status.ok);
    assert_eq!(report.frozen_servers, vec!["server-stale".to_string()]);

    let servers = meta.list_servers();
    assert!(servers.status.ok);
    assert_eq!(servers.servers[0].state, MetaEntityState::Frozen);
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 2);
    }
}

#[test]
fn production_meta_raft_runtime_ticks_failover_and_failure_detection() {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:17101".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:17102".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:17103".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 5,
        election_tick_ms: 2,
        failure_detector_interval_ms: 5,
        stale_server_after_ms: 1,
    })
    .unwrap();
    assert!(runtime.validate_ready().is_ok());
    assert!(
        runtime
            .cluster()
            .register_server(RegisterServerRequest {
                registered_at_ms: 0,
                numa_nodes: Vec::new(),
                server_addr: "server-stale".to_string(),
                node_id: 1,
                location: "az-a".to_string(),
                binary_version: "test".to_string(),
            })
            .status
            .ok
    );
    runtime.cluster().set_alive(10, false).unwrap();
    let timer = runtime.start_timer_loop();
    let deadline = std::time::Instant::now() + Duration::from_millis(200);
    while std::time::Instant::now() < deadline {
        let status = runtime.status();
        let servers = runtime.cluster().list_servers();
        let server_frozen = servers
            .servers
            .first()
            .map(|server| server.state == MetaEntityState::Frozen)
            .unwrap_or(false);
        if status.leader_id != 10 && server_frozen {
            timer.stop();
            assert!(status.has_majority);
            return;
        }
        thread::sleep(Duration::from_millis(5));
    }
    timer.stop();
    panic!("production metaserver raft runtime did not fail over and freeze stale server");
}

#[test]
fn production_meta_raft_runtime_matches_multinode_control_and_fault_contract() {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:17101".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:17102".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:17103".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 5,
        election_tick_ms: 2,
        failure_detector_interval_ms: 5,
        stale_server_after_ms: 1_000,
    })
    .unwrap();

    assert_eq!(runtime.list_membership(), vec![10, 11, 12]);
    assert!(runtime.validate_ready().is_ok());
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-control".to_string(),
            },
        )))
        .unwrap();
    assert_eq!(runtime.wait_for_log_applied().unwrap().read_index, 1);

    let add_report = runtime.add_node(13, RaftReplicaRole::Voter).unwrap();
    assert_eq!(add_report.live_voters, 4);
    assert_eq!(runtime.list_membership(), vec![10, 11, 12, 13]);
    assert!(matches!(
        runtime.add_node(14, RaftReplicaRole::Learner),
        Err(RaftError::InvalidConfig(message)) if message.contains("voter membership only")
    ));

    let membership = runtime.apply_membership([10, 11, 13]).unwrap();
    assert_eq!(membership.committed_membership.voters, vec![10, 11, 13]);
    assert_eq!(runtime.list_membership(), vec![10, 11, 13]);

    let snapshot = runtime.trigger_snapshot().unwrap();
    assert!(snapshot.last_included_index >= 1);
    runtime.transfer_leader(11).unwrap();
    assert_eq!(runtime.status().leader_id, 11);
    assert_eq!(runtime.read_index(10).unwrap().leader_id, 11);

    runtime.cluster().set_alive(11, false).unwrap();
    runtime
        .propose(MetaCommand::ApplyMutation(MetaMutation::AddNamespace(
            AddNamespaceRequest {
                namespace: "meta-raft-after-failover".to_string(),
            },
        )))
        .unwrap();
    let status = runtime.status();
    assert_ne!(status.leader_id, 11);
    assert!(status.has_majority);
    assert!(runtime
        .cluster()
        .list_namespaces()
        .namespaces
        .iter()
        .any(|namespace| namespace.namespace == "meta-raft-after-failover"));
}

#[test]
fn metaserver_owns_data_raft_membership_workflow() {
    let meta = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:19110".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:19111".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:19112".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    let data = RaftCluster::new_single_shard(7, [1, 2, 3]);
    data.propose(Command::StringSet {
        key: "membership-workflow".to_string(),
        value: b"before".to_vec(),
    })
    .unwrap();

    let report = meta
        .drive_data_raft_membership_workflow(&data, 4, Some(4), Some(1))
        .unwrap();
    assert_eq!(report.shard_id, 7);
    assert_eq!(report.learner_id, 4);
    assert_eq!(report.removed_voter_id, Some(1));
    assert_eq!(report.requested_leader_id, Some(4));
    assert_eq!(report.initial_voters, vec![1, 2, 3]);
    assert!(report.learner_added);
    assert!(report.catch_up_verified);
    assert_eq!(
        report.learner_catch_up_index,
        report.required_catch_up_index
    );
    assert!(report.promoted_to_voter);
    assert!(report.membership_committed);
    assert_eq!(report.voters_after_promote, vec![1, 2, 3, 4]);
    assert!(report.leader_transferred);
    assert!(report.voter_removed);
    assert_eq!(report.final_leader_id, 4);
    assert_eq!(report.final_voters, vec![2, 3, 4]);
    assert_eq!(data.membership().voters, vec![2, 3, 4]);
    assert_eq!(
        data.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Voter
    );
    assert_eq!(
        data.read_from_replica(
            4,
            Command::StringGet {
                key: "membership-workflow".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"before".to_vec())
        })
    );
}

#[cfg(feature = "temporal-raft-engine")]
#[test]
fn production_raft_readiness_requires_temporal_raft_process_and_meta_owned_membership_evidence() {
    let rollout = raft_temporal_raft_rollout_readiness();
    assert!(rollout.adapter_present);
    assert!(!rollout.data_node_real_process_rollout_validated);
    assert!(!rollout.metaserver_real_process_rollout_validated);
    assert!(!rollout.multi_process_log_store_validation_present);
    assert!(!rollout.production_ready);

    let data_report = ready_data_node_temporal_raft_rollout_report();
    let meta_report = ready_meta_temporal_raft_rollout_report();
    let rollout_with_evidence =
        raft_temporal_raft_rollout_readiness_from_reports(Some(&data_report), Some(&meta_report));
    assert!(rollout_with_evidence.data_node_real_process_rollout_validated);
    assert!(rollout_with_evidence.metaserver_real_process_rollout_validated);
    assert!(rollout_with_evidence.multi_process_log_store_validation_present);
    assert!(rollout_with_evidence.production_ready);

    let membership = raft_metaserver_membership_readiness();
    assert!(membership.networked_scheduler_transport_present);
    assert!(membership.persisted_scheduler_task_state_present);
    assert!(membership.real_data_node_group_execution_present);
    assert!(membership.production_ready);

    let readiness = distributed_raft_readiness();
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(!readiness.production_ready);
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("multi-process rollout evidence")));
    assert!(matches!(
        validate_raft_deployment_mode(RaftDeploymentMode::LocalModel),
        Err(RaftProductionReadinessError { message, .. })
            if message.contains("local Raft deployment mode is disabled")
    ));
}

#[test]
fn meta_owned_membership_report_covers_networked_scheduler_contract() {
    let workflow = MetaDataRaftMembershipWorkflowReport {
        shard_id: 7,
        learner_id: 4,
        removed_voter_id: Some(1),
        requested_leader_id: Some(4),
        initial_voters: vec![1, 2, 3],
        learner_added: true,
        catch_up_verified: true,
        learner_catch_up_index: 9,
        required_catch_up_index: 9,
        promoted_to_voter: true,
        membership_committed: true,
        voters_after_promote: vec![1, 2, 3, 4],
        leader_transferred: true,
        voter_removed: true,
        final_leader_id: 4,
        final_voters: vec![2, 3, 4],
        commit_index: 9,
    };
    let report = MetaOwnedDataRaftMembershipReport {
        scheduler_task_id: 99,
        scheduler_generation: 9,
        stale_scheduler_token_rejected: true,
        workflow,
        executed_steps: vec![
            "learner_add".to_string(),
            "catch_up_verify".to_string(),
            "promote_to_voter".to_string(),
            "leader_transfer".to_string(),
            "voter_remove".to_string(),
        ],
        final_node_evidence: vec![
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
            ready_temporal_raft_process_node(4),
        ],
        final_secondary_replica_lag: 0,
        follower_lag_validated: true,
        failover_validated: true,
        scale_up_validated: true,
        scale_down_validated: true,
        secondary_replication_validated: true,
        networked_process_api_used: true,
        scheduler_process_api_calls_observed: 5,
        data_node_membership_apply_process_api_calls_observed: 5,
        data_node_raft_group_process_nodes_observed: 3,
        data_node_raft_group_commit_indexes_observed: vec![9, 9, 9],
        learner_add_process_api_observed: true,
        catchup_verification_process_api_observed: true,
        promote_process_api_observed: true,
        leader_transfer_process_api_observed: true,
        voter_remove_process_api_observed: true,
        scheduler_generation_token_coupling_observed: true,
        stale_generation_rejection_observed: true,
        membership_generation_replayed_from_meta_raft: true,
        persisted_through_meta_raft_replay: true,
        ready: true,
        blockers: Vec::new(),
    };
    assert!(report.ready);
    assert!(report.networked_process_api_used);
    assert!(report.scheduler_generation_token_coupling_observed);
    assert!(report.stale_generation_rejection_observed);
    assert!(report.membership_generation_replayed_from_meta_raft);
    assert!(report.persisted_through_meta_raft_replay);
    assert!(report.stale_scheduler_token_rejected);
    assert_eq!(report.workflow.final_voters, vec![2, 3, 4]);
    assert_eq!(report.final_secondary_replica_lag, 0);
    assert!(report
        .executed_steps
        .iter()
        .any(|step| step == "leader_transfer"));
    assert!(report
        .final_node_evidence
        .iter()
        .all(|node| node.log_store_validated));
}

#[test]
fn metaserver_membership_workflow_requires_meta_majority() {
    let meta = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: "127.0.0.1:19210".to_string(),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: "127.0.0.1:19211".to_string(),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: "127.0.0.1:19212".to_string(),
            },
        ],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    meta.cluster().set_alive(11, false).unwrap();
    meta.cluster().set_alive(12, false).unwrap();
    let data = RaftCluster::new_single_shard(8, [1, 2, 3]);

    let error = meta
        .drive_data_raft_membership_workflow(&data, 4, Some(4), Some(1))
        .unwrap_err();
    assert!(matches!(
        error,
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    ));
    assert!(data.local_status(4).is_err());
}

#[test]
fn metaserver_raft_mutation_api_rejects_without_majority() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_alive(11, false).unwrap();
    meta.set_alive(12, false).unwrap();

    let response = meta.add_namespace(AddNamespaceRequest {
        namespace: "feature".to_string(),
    });
    assert!(!response.status.ok);
    assert_eq!(response.status.code, "raft_error");
    assert!(response.status.message.contains("majority"));
}

fn cluster_with_an_installed_snapshot() -> MetaRaftCluster {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "before".to_string()
        })
        .status
        .ok);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    meta
}

#[test]
fn a_change_after_a_snapshot_install_actually_takes_effect() {
    // Installing a meta snapshot truncates the log and marks everything up to
    // the snapshot applied. The next index came from the log alone, so
    // numbering restarted at 1 -- indices the node had already applied. Every
    // proposal after an install was skipped as a duplicate and reported ok, so
    // the cluster accepted metadata changes and silently discarded them.
    let meta = cluster_with_an_installed_snapshot();

    let added = meta.add_namespace(AddNamespaceRequest {
        namespace: "after".to_string(),
    });
    assert!(added.status.ok, "{:?}", added.status);

    let namespaces = meta
        .read_meta()
        .expect("a readable replica")
        .list_namespaces()
        .namespaces;
    assert!(
        namespaces.iter().any(|namespace| namespace.namespace == "after"),
        "a change reported as accepted never took effect: {namespaces:?}"
    );
    // What the snapshot carried is still there too.
    assert!(namespaces.iter().any(|namespace| namespace.namespace == "before"));
}

#[test]
fn changes_keep_taking_effect_after_a_snapshot_install() {
    // One change could succeed by luck if the numbering only collided once.
    let meta = cluster_with_an_installed_snapshot();
    for round in 0..5u64 {
        let name = format!("ns-{round}");
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: name.clone()
            })
            .status
            .ok);
        let namespaces = meta
            .read_meta()
            .expect("a readable replica")
            .list_namespaces()
            .namespaces;
        assert!(
            namespaces.iter().any(|namespace| namespace.namespace == name),
            "change {round} was accepted and discarded: {namespaces:?}"
        );
    }
}

#[test]
fn a_snapshot_install_does_not_rewind_the_commit_index() {
    // The reused index was also written straight into commit_index, walking it
    // backwards past entries the cluster had already committed.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    for round in 0..3u64 {
        assert!(meta
            .add_namespace(AddNamespaceRequest {
                namespace: format!("ns-{round}")
            })
            .status
            .ok);
    }
    let before = meta.status().commit_index;
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "after".to_string()
        })
        .status
        .ok);
    assert!(
        meta.status().commit_index >= before,
        "commit index went backwards: {} then {}",
        before,
        meta.status().commit_index
    );
}

fn convicted_proxy_cluster(forbid: bool) -> MetaRaftCluster {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_conviction_lock(forbid);
    assert!(meta
        .register_proxy(RegisterProxyRequest {
            registered_at_ms: 0,
            proxy_addr: "proxy-a".to_string(),
            namespace: "ns".to_string(),
            location: "rack-1".to_string(),
            config_version: 1,
            binary_version: "v1".to_string(),
        })
        .status
        .ok);
    // Frozen for a reason that counts as conviction, which is what the lock is
    // about: a resource the metaserver took out, not one an operator did.
    assert!(meta
        .freeze_proxy(StateChangeRequest {
            endpoint: "proxy-a".to_string(),
            reason: crate::meta::FreezeReason::Unresponsive,
            freeze_cooldown_ms: 0,
        })
        .status
        .ok);
    meta
}

fn rejoin(meta: &MetaRaftCluster) -> Status {
    meta.register_proxy(RegisterProxyRequest {
        registered_at_ms: 0,
        proxy_addr: "proxy-a".to_string(),
        namespace: "ns".to_string(),
        location: "rack-1".to_string(),
        config_version: 1,
        binary_version: "v1".to_string(),
    })
    .status
}

#[test]
fn the_conviction_lock_holds_on_a_raft_backed_metaserver() {
    // The setting is read after `from_env` has already returned the raft
    // backend, so it reached the single-node metaserver and nothing else. The
    // check that consults it runs on these nodes -- against a flag that was
    // always false, which let a convicted resource register its way back in.
    let meta = convicted_proxy_cluster(true);
    let refused = rejoin(&meta);
    assert!(!refused.ok, "a convicted proxy registered its way back in");
    assert_eq!(refused.code, "conviction_requires_unfreeze");

    // An explicit unfreeze is the way back, or the lock would be a dead end.
    assert!(meta
        .unfreeze_proxy(StateChangeRequest {
            endpoint: "proxy-a".to_string(),
            reason: crate::meta::FreezeReason::Unspecified,
            freeze_cooldown_ms: 0,
        })
        .status
        .ok);
    assert!(rejoin(&meta).ok, "an unfrozen proxy could not rejoin");
}

#[test]
fn the_runtime_carries_the_conviction_lock_to_its_nodes() {
    // The setter alone proves nothing about production: what was broken is that
    // the option never reached the nodes, because the flag is read on a path the
    // raft backend returns before.
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: true,
        snapshot_check_interval_ms: 0,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 1,
        nodes: vec![ProductionRaftNode {
            node_id: 1,
            addr: "127.0.0.1:18147".to_string(),
        }],
        config: RaftConfig::default(),
        heartbeat_interval_ms: 100,
        election_tick_ms: 50,
        failure_detector_interval_ms: 1_000,
        stale_server_after_ms: 30_000,
    })
    .unwrap();
    assert_eq!(
        runtime
            .cluster()
            .read_meta()
            .expect("a readable replica")
            .conviction_lock_enabled(),
        true,
        "the runtime did not carry the setting to its nodes"
    );
}

#[test]
fn the_conviction_lock_stays_off_when_it_is_not_asked_for() {
    // Off by default on purpose: the automatic recovery it removes is
    // load-bearing wherever the freeze cooldown is left at zero.
    let meta = convicted_proxy_cluster(false);
    assert!(
        rejoin(&meta).ok,
        "a setting nobody asked for locked a proxy out"
    );
}

#[test]
fn installing_a_snapshot_does_not_clear_the_conviction_lock() {
    // Each node's metadata used to be rebuilt from `Default` on install, which
    // discarded everything configured on it -- the lock along with the event
    // bus, the metrics recorder and the counters.
    let meta = convicted_proxy_cluster(true);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");

    let refused = rejoin(&meta);
    assert!(
        !refused.ok,
        "a snapshot install turned the conviction lock off"
    );
    assert_eq!(refused.code, "conviction_requires_unfreeze");
}

#[test]
fn an_ordinary_change_still_reports_success() {
    // The guard on "committed but applied nothing" must not catch the normal
    // path: every real change still has to come back ok, before and after a
    // snapshot install.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "before".to_string()
        })
        .status
        .ok);
    let snapshot = meta.export_meta_snapshot().expect("a snapshot");
    meta.install_meta_snapshot_on_live_nodes(snapshot)
        .expect("the snapshot installs");
    let after = meta.add_namespace(AddNamespaceRequest {
        namespace: "after".to_string(),
    });
    assert!(after.status.ok, "{:?}", after.status);
    assert_ne!(after.status.code, "mutation_not_applied");
}

#[test]
fn muting_metadata_change_refuses_changes_on_the_raft_path_too() {
    // The mute is the incident lever: while it is set the metaserver is meant
    // to refuse every recorded metadata mutation. That check lived only in
    // SingleNodeMeta's public methods, and the raft backend proposes straight
    // past them -- so on a raft-backed metaserver, which is what a real
    // deployment runs, setting the mute changed nothing.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta.set_meta_change_muted(true).status.ok);

    let muted = meta.add_namespace(AddNamespaceRequest {
        namespace: "during-an-incident".to_string(),
    });
    assert!(
        !muted.status.ok,
        "the cluster was muted and the change went through anyway"
    );
    assert_eq!(muted.status.code, "meta_change_muted");

    // And the lever has to be releasable, or muting would be a one-way door.
    assert!(meta.set_meta_change_muted(false).status.ok);
    assert!(
        meta.add_namespace(AddNamespaceRequest {
            namespace: "after-the-incident".to_string(),
        })
        .status
        .ok,
        "unmuting did not restore metadata change"
    );
}

fn table_in(meta: &MetaRaftCluster, namespace: &str, table_name: &str) -> Status {
    meta.add_table(AddTableRequest {
        namespace: namespace.to_string(),
        table_name: table_name.to_string(),
        first_shard_id: 500,
        shard_count: 1,
        replica_count: 1,
        partition_version: 0,
        serving_options: crate::meta::TableServingOptions::default(),
    })
    .status
}

#[test]
fn a_reserved_name_cannot_be_taken_on_the_raft_path() {
    // Reserved names exist to hold a name back from creation. The check lived
    // in the public method, and the raft path proposes past it, so on a
    // raft-backed metaserver the reservation held nothing back at all.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    let mut reserved = crate::meta::ReservedNames::default();
    reserved.namespaces.insert("system".to_string());
    reserved.tables.insert("internal".to_string());
    assert!(meta.set_reserved_names(reserved).status.ok);

    let taken = meta.add_namespace(AddNamespaceRequest {
        namespace: "system".to_string(),
    });
    assert!(!taken.status.ok, "a reserved namespace was created anyway");
    assert_eq!(taken.status.code, "name_reserved");

    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string()
        })
        .status
        .ok);
    let table = table_in(&meta, "tenant", "internal");
    assert!(!table.ok, "a reserved table name was created anyway");
    assert_eq!(table.code, "name_reserved");

    // And an unreserved name is still allowed, so the guard is not a blanket no.
    assert!(table_in(&meta, "tenant", "orders").ok);
}

#[test]
fn dropping_a_namespace_cannot_strand_a_live_table_on_the_raft_path() {
    // The single-node path refuses this precisely so a drop cannot leave tables
    // behind in a namespace that no longer exists. The raft path did not.
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(meta
        .add_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string()
        })
        .status
        .ok);
    assert!(table_in(&meta, "tenant", "orders").ok);

    let stranding = meta.drop_namespace(AddNamespaceRequest {
        namespace: "tenant".to_string(),
    });
    assert!(
        !stranding.status.ok,
        "a namespace was dropped out from under a live table"
    );
    assert_eq!(stranding.status.code, "namespace_not_empty");

    // Once the table is gone the namespace can be dropped, or the guard would
    // make a namespace undroppable rather than merely safe to drop.
    assert!(meta
        .delete_table(DeleteTableRequest {
            namespace: "tenant".to_string(),
            table_name: "orders".to_string(),
        })
        .status
        .ok);
    assert!(
        meta.drop_namespace(AddNamespaceRequest {
            namespace: "tenant".to_string()
        })
        .status
        .ok,
        "an empty namespace could not be dropped"
    );
}

#[test]
fn metaserver_raft_can_read_from_any_live_committed_replica() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 7,
        server_addr: "server-a".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    assert_eq!(meta.commit_index(11).unwrap(), 1);
    meta.set_alive(10, false).unwrap();

    assert_eq!(
        meta.get_shard_location_from_any_live(7).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 7,
            server_addr: "server-a".to_string(),
            latest_snapshot: None,
        })
    );
}

#[test]
fn metaserver_raft_supports_promotion_and_membership_changes() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_alive(10, false).unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 2,
        server_addr: "server-b".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    meta.add_node(13).unwrap();
    assert_eq!(
        meta.get_shard_location(13, 2).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 2,
            server_addr: "server-b".to_string(),
            latest_snapshot: None,
        })
    );
    meta.remove_node(12).unwrap();
    meta.propose(MetaCommand::RemoveShard(2)).unwrap();
    assert_eq!(meta.get_shard_location(11, 2).unwrap(), None);
    assert_eq!(meta.get_shard_location(13, 2).unwrap(), None);
}

#[test]
fn metaserver_raft_health_catchup_safe_scale_and_failover_work() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_alive(12, false).unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 42,
        server_addr: "server-before-meta-lag".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    meta.set_alive(12, true).unwrap();

    let health = meta.replication_health(0);
    assert!(!health.healthy);
    assert_eq!(
        health.lagging_voters,
        vec![RaftReplicaLag {
            node_id: 12,
            lag: 1,
            alive: true,
        }]
    );
    assert_eq!(meta.catch_up_live_followers().unwrap(), vec![11, 12]);
    assert!(meta.replication_health(0).healthy);

    let report = meta.add_node_safely(13).unwrap();
    assert_eq!(report.voters, vec![10, 11, 12, 13]);
    assert_eq!(report.caught_up_voters, vec![10, 11, 12, 13]);

    meta.set_alive(11, false).unwrap();
    meta.set_alive(12, false).unwrap();
    assert_eq!(
        meta.remove_node_safely(13).unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2,
        }
    );
    meta.set_alive(11, true).unwrap();
    meta.set_alive(12, true).unwrap();
    meta.catch_up_live_followers().unwrap();

    meta.set_alive(10, false).unwrap();
    let failover = meta.failover_primary().unwrap();
    assert_eq!(failover.old_leader_id, 10);
    assert_ne!(failover.new_leader_id, 10);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 43,
        server_addr: "server-after-meta-failover".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    assert_eq!(
        meta.get_shard_location(failover.new_leader_id, 43).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 43,
            server_addr: "server-after-meta-failover".to_string(),
            latest_snapshot: None,
        })
    );
}

#[test]
fn metaserver_raft_apply_health_reports_commit_to_apply_lag() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 144,
        server_addr: "server-meta-apply-health".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    {
        let mut inner = meta.inner.write().expect("meta raft lock poisoned");
        let node = inner.nodes.get_mut(&11).unwrap();
        node.applied.clear();
    }

    let health = meta.apply_health(0);
    assert!(!health.healthy);
    assert_eq!(health.leader_commit_index, 1);
    assert_eq!(health.max_apply_lag, 1);
    assert_eq!(
        health.slow_appliers,
        vec![RaftApplyLag {
            node_id: 11,
            commit_index: 1,
            applied_index: 0,
            apply_lag: 1,
            alive: true,
        }]
    );
    assert!(meta.prometheus_metrics().contains(
        "temporalstore_raft_node_apply_lag{kind=\"meta\",node_id=\"11\",role=\"follower\",replica_role=\"voter\"} 1"
    ));

    meta.catch_up(11).unwrap();
    assert!(meta.apply_health(0).healthy);
}

#[test]
fn metaserver_raft_membership_plan_and_apply_match_data_raft_shape() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 44,
        server_addr: "server-before-meta-membership".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    let plan = meta.plan_membership_change([10, 11, 13]).unwrap();
    assert_eq!(plan.shard_id, 0);
    assert_eq!(plan.kind, RaftMembershipChangeKind::ReplaceVoter);
    assert_eq!(plan.old_voters, vec![10, 11, 12]);
    assert_eq!(plan.new_voters, vec![10, 11, 13]);
    assert_eq!(plan.add_voters, vec![13]);
    assert_eq!(plan.remove_voters, vec![12]);

    let report = meta.apply_membership_change_safely([10, 11, 13]).unwrap();
    assert_eq!(report.plan, plan);
    assert_eq!(report.joint_membership.old_voters, vec![10, 11, 12]);
    assert_eq!(report.joint_membership.new_voters, vec![10, 11, 13]);
    assert_eq!(report.committed_membership.voters, vec![10, 11, 13]);
    assert_eq!(
        meta.get_shard_location(13, 44).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 44,
            server_addr: "server-before-meta-membership".to_string(),
            latest_snapshot: None,
        })
    );
    assert_eq!(meta.commit_index(12), Err(RaftError::NodeNotFound(12)));
}

#[test]
fn metaserver_raft_membership_apply_rejects_noop_and_quorum_loss() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    assert!(matches!(
        meta.plan_membership_change([10, 11, 12]),
        Err(RaftError::InvalidConfig(message))
            if message.contains("membership change must add or remove")
    ));

    meta.set_alive(11, false).unwrap();
    meta.set_alive(12, false).unwrap();
    assert_eq!(
        meta.apply_membership_change_safely([10, 11]).unwrap_err(),
        RaftError::NoMajority {
            live: 1,
            required: 2,
        }
    );
}

#[test]
fn metaserver_raft_status_read_index_and_transfer_leader_work() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 7,
        server_addr: "127.0.0.1:17002".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    let status = meta.status();
    assert_eq!(status.leader_id, 10);
    assert_eq!(status.commit_index, 1);
    assert!(status.leader_lease_valid);
    assert_eq!(status.nodes.len(), 3);

    let read_index = meta.read_index(11).unwrap();
    assert_eq!(read_index.leader_id, 10);
    assert_eq!(read_index.node_id, 11);
    assert_eq!(read_index.read_index, 1);

    meta.transfer_leader(11).unwrap();
    assert_eq!(meta.leader_id(), 11);
    assert_eq!(meta.local_status(11).unwrap().role, RaftRole::Leader);
    assert!(meta
        .prometheus_metrics()
        .contains("temporalstore_raft_cluster_commit_index{kind=\"meta\"} 1"));
}

#[test]
fn metaserver_raft_promotes_follower_after_leader_failure_and_keeps_metadata_available() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 7,
        server_addr: "server-before-failover".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    meta.set_alive(10, false).unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 8,
        server_addr: "server-after-failover".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    let status = meta.status();
    assert_ne!(status.leader_id, 10);
    assert!(status.has_majority);
    assert_eq!(
        meta.get_shard_location(status.leader_id, 7).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 7,
            server_addr: "server-before-failover".to_string(),
            latest_snapshot: None,
        })
    );
    assert_eq!(
        meta.get_shard_location(status.leader_id, 8).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 8,
            server_addr: "server-after-failover".to_string(),
            latest_snapshot: None,
        })
    );
}

#[test]
fn metaserver_raft_rejects_reads_and_writes_without_majority() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 7,
        server_addr: "server-before-quorum-loss".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    meta.set_alive(11, false).unwrap();
    meta.set_alive(12, false).unwrap();

    let status = meta.status();
    assert!(!status.has_majority);
    assert!(!status.leader_lease_valid);
    assert_eq!(meta.read_index(10), Err(RaftError::LeaderUnavailable));
    assert_eq!(
        meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 8,
            server_addr: "server-without-quorum".to_string(),
            latest_snapshot: None,
        })),
        Err(RaftError::NoMajority {
            live: 1,
            required: 2
        })
    );
}

#[test]
fn metaserver_snapshot_bootstraps_lagging_meta_replica() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.set_alive(12, false).unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 9,
        server_addr: "server-snapshot".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    let snapshot = meta.create_snapshot().unwrap();

    meta.set_alive(12, true).unwrap();
    meta.install_snapshot(12, snapshot).unwrap();
    assert_eq!(
        meta.get_shard_location(12, 9).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 9,
            server_addr: "server-snapshot".to_string(),
            latest_snapshot: None,
        })
    );
    assert_eq!(meta.commit_index(12).unwrap(), 1);
}

/// Start a meta raft runtime whose applied log is over the compaction threshold
/// the moment anything is written, with `snapshot_check_interval_ms` as given.
fn meta_runtime_for_snapshot_wiring(
    snapshot_check_interval_ms: u64,
    base_port: u16,
) -> ProductionMetaRaftRuntime {
    ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        forbid_self_clearing_conviction: false,
        snapshot_check_interval_ms,
        engine: ProductionRaftEngineKind::TemporalRaft,
        local_node_id: 10,
        nodes: vec![
            ProductionRaftNode {
                node_id: 10,
                addr: format!("127.0.0.1:{base_port}"),
            },
            ProductionRaftNode {
                node_id: 11,
                addr: format!("127.0.0.1:{}", base_port + 1),
            },
            ProductionRaftNode {
                node_id: 12,
                addr: format!("127.0.0.1:{}", base_port + 2),
            },
        ],
        config: RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
        heartbeat_interval_ms: 5,
        election_tick_ms: 2,
        failure_detector_interval_ms: 10_000,
        stale_server_after_ms: 0,
    })
    .unwrap()
}

#[test]
fn meta_raft_timer_loop_compacts_the_applied_log() {
    // The wiring this exercises did not exist: `maybe_trigger_snapshot` was
    // implemented and the config asked for compaction at a byte threshold, but
    // no loop ever called it, so the log grew for the life of the process.
    let runtime = meta_runtime_for_snapshot_wiring(2, 17111);
    runtime.cluster().register(RegisterShardRequest {
        registered_at_ms: 0,
        shard_id: 41,
        server_addr: "server-timer-compaction".to_string(),
    });
    let timer = runtime.start_timer_loop();
    std::thread::sleep(std::time::Duration::from_millis(400));

    // One observation, and it discriminates: the loop having already compacted
    // leaves nothing new to apply. Had nothing called it, this same first call
    // would report `triggered` against the threshold instead.
    let report = runtime.cluster().maybe_trigger_snapshot().unwrap();
    timer.stop();
    assert!(
        !report.triggered,
        "the timer loop should have compacted already, got {report:?}"
    );
    assert_eq!(report.reason, "no_new_applied_logs");
    assert!(report.last_snapshot_index >= 1);
}

#[test]
fn a_zero_snapshot_check_interval_leaves_the_log_alone() {
    // The negative control. Without it the test above could pass for reasons
    // that have nothing to do with the timer loop.
    let runtime = meta_runtime_for_snapshot_wiring(0, 17121);
    runtime.cluster().register(RegisterShardRequest {
        registered_at_ms: 0,
        shard_id: 42,
        server_addr: "server-timer-disabled".to_string(),
    });
    let timer = runtime.start_timer_loop();
    std::thread::sleep(std::time::Duration::from_millis(400));

    let report = runtime.cluster().maybe_trigger_snapshot().unwrap();
    timer.stop();
    assert!(
        report.triggered,
        "nothing should have compacted with checking disabled, got {report:?}"
    );
    assert_eq!(report.reason, "applied_log_bytes_threshold");
    assert_eq!(report.last_snapshot_index, 0);
}

#[test]
fn metaserver_raft_snapshot_trigger_compacts_applied_log_bytes() {
    let meta = MetaRaftCluster::new_with_config(
        [10, 11, 12],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    meta.register(RegisterShardRequest {
        registered_at_ms: 0,
        shard_id: 33,
        server_addr: "server-meta-trigger".to_string(),
    });

    let report = meta.maybe_trigger_snapshot().unwrap();
    assert!(report.triggered);
    assert_eq!(report.reason, "applied_log_bytes_threshold");
    assert_eq!(report.applied_index, 1);
    assert!(report.applied_log_bytes >= 1);
    assert_eq!(meta.local_status(10).unwrap().last_log_index, 1);

    let second = meta.maybe_trigger_snapshot().unwrap();
    assert!(!second.triggered);
    assert_eq!(second.reason, "no_new_applied_logs");
    assert_eq!(second.last_snapshot_index, 1);
}

#[test]
fn metaserver_snapshot_floor_survives_failover_and_add_node() {
    let meta = MetaRaftCluster::new_with_config(
        [10, 11, 12],
        RaftConfig {
            max_applied_log_bytes: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 88,
        server_addr: "meta-snapshot-floor".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    let snapshot_report = meta.maybe_trigger_snapshot().unwrap();
    assert!(snapshot_report.triggered);
    assert_eq!(snapshot_report.applied_index, 1);
    assert_eq!(meta.local_status(10).unwrap().last_log_index, 1);
    assert_eq!(meta.local_status(11).unwrap().last_log_index, 1);

    meta.set_alive(10, false).unwrap();
    let failover = meta.failover_primary().unwrap();
    assert_ne!(failover.new_leader_id, 10);
    assert_eq!(failover.commit_index, 1);
    assert_eq!(
        meta.get_shard_location(failover.new_leader_id, 88).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 88,
            server_addr: "meta-snapshot-floor".to_string(),
            latest_snapshot: None,
        })
    );

    meta.add_node(13).unwrap();
    assert_eq!(meta.local_status(13).unwrap().last_log_index, 1);
    assert_eq!(
        meta.get_shard_location(13, 88).unwrap(),
        Some(ShardLocation {
<<<<<<< HEAD
            registered_at_ms: 0,
||||||| a7277311
=======
            preferred_location: String::new(),
>>>>>>> matrixark/main
            state: crate::meta::MetaEntityState::Normal,
            shard_id: 88,
            server_addr: "meta-snapshot-floor".to_string(),
            latest_snapshot: None,
        })
    );
}

#[test]
fn metaserver_snapshot_cannot_overwrite_newer_meta_state() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 1,
        server_addr: "server-a".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    let snapshot = meta.create_snapshot().unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
<<<<<<< HEAD
        registered_at_ms: 0,
||||||| a7277311
=======
        preferred_location: String::new(),
>>>>>>> matrixark/main
        state: crate::meta::MetaEntityState::Normal,
        shard_id: 2,
        server_addr: "server-b".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    assert_eq!(
        meta.install_snapshot(11, snapshot).unwrap_err(),
        RaftError::StaleSnapshot {
            snapshot_index: 1,
            local_commit_index: 2,
        }
    );
}

#[test]
fn local_raft_wal_persists_hard_state_membership_and_entries() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "wal-key".to_string(),
            value: b"wal-value".to_vec(),
        })
        .unwrap();
    cluster.persist_wal(dir.path()).unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(7, 1).unwrap().unwrap();
    assert_eq!(record.hard_state.commit_index, 1);
    assert_eq!(record.membership.shard_id, 7);
    assert_eq!(record.membership.voters, vec![1, 2, 3]);
    assert_eq!(record.entries.len(), 1);
    assert_eq!(record.entries[0].index, 1);
}

#[test]
fn local_raft_wal_recovers_latest_valid_record_and_truncates_corrupt_tail() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "wal-crash".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster.persist_wal(dir.path()).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "wal-crash".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();
    cluster.persist_wal(dir.path()).unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let path = wal.node_path(7, 1);
    let before_corruption = fs::metadata(&path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&path).unwrap();
    file.write_all(b"{\"sequence\":3,\"checksum\":\"bad\"")
        .unwrap();
    file.sync_data().unwrap();

    let recovery = wal.recover_node(7, 1).unwrap();
    assert!(recovery.corrupt_tail);
    assert!(recovery.truncated_bytes > 0);
    assert_eq!(recovery.valid_records, 2);
    assert_eq!(fs::metadata(&path).unwrap().len(), before_corruption);
    let record = recovery.record.unwrap();
    assert_eq!(record.hard_state.commit_index, 2);
    assert_eq!(record.entries.len(), 2);
}

#[test]
fn local_raft_wal_segments_roll_retain_and_recover_latest_state() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let wal = LocalRaftWal::new(dir.path());
    let mut released_segments = 0u64;
    let mut slow_fsync_seen = false;
    for index in 0..8 {
        cluster
            .propose(Command::StringSet {
                key: "segmented-wal".to_string(),
                value: format!("v{index}").into_bytes(),
            })
            .unwrap();
        for (node_id, record) in cluster.wal_records() {
            let report = wal
                .persist_node_segmented_with_fsync_threshold(7, node_id, &record, 256, 2, 0)
                .unwrap();
            if node_id == 1 {
                released_segments = released_segments.saturating_add(report.released_segment_count);
                slow_fsync_seen |= report.slow_fsync_backpressure_observed;
            }
        }
    }

    let report = wal.segment_report(7, 1).unwrap();
    assert_eq!(report.segments.len(), 2);
    assert!(report.active_segment_id >= 2);
    assert!(report.segments.iter().all(|segment| segment.bytes > 0));
    assert!(report
        .segments
        .iter()
        .all(|segment| segment.first_log_index > 0
            && segment.last_log_index >= segment.first_log_index));
    assert!(released_segments > 0);
    assert!(slow_fsync_seen);
    assert!(report.slow_fsync_backpressure_observed);
    assert!(report.first_retained_log_index > 0);
    assert_eq!(report.last_retained_log_index, 8);

    let recovery = wal.recover_node(7, 1).unwrap();
    let record = recovery.record.unwrap();
    assert_eq!(record.hard_state.commit_index, 8);
    assert_eq!(record.entries.len(), 8);
}

#[test]
fn local_raft_wal_segment_recovery_truncates_corrupt_tail_only() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    let wal = LocalRaftWal::new(dir.path());
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: "segmented-crash".to_string(),
                value: format!("v{index}").into_bytes(),
            })
            .unwrap();
        let record = cluster
            .wal_records()
            .into_iter()
            .find(|(node_id, _)| *node_id == 1)
            .unwrap()
            .1;
        wal.persist_node_segmented(7, 1, &record, 1024, 2).unwrap();
    }
    let report = wal.segment_report(7, 1).unwrap();
    let active = report.segments.last().unwrap();
    let before_corruption = fs::metadata(&active.path).unwrap().len();
    let mut file = OpenOptions::new().append(true).open(&active.path).unwrap();
    file.write_all(b"{\"sequence\":99,\"checksum\":\"bad\"")
        .unwrap();
    file.sync_data().unwrap();

    let recovery = wal.recover_node(7, 1).unwrap();
    assert!(recovery.corrupt_tail);
    assert!(recovery.truncated_bytes > 0);
    assert_eq!(fs::metadata(&active.path).unwrap().len(), before_corruption);
    assert_eq!(recovery.record.unwrap().hard_state.commit_index, 3);
}

#[test]
fn raft_cluster_recovers_committed_state_from_local_wal() {
    let dir = tempfile::tempdir().unwrap();
    let cluster = RaftCluster::new_single_shard(7, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "recovered".to_string(),
            value: b"from-wal".to_vec(),
        })
        .unwrap();
    cluster.transfer_leader(2).unwrap();
    cluster.persist_wal(dir.path()).unwrap();

    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 7, [1, 2, 3], RaftConfig::default())
            .unwrap();
    assert_eq!(restored.leader_id(), 2);
    assert_eq!(restored.commit_index(1).unwrap(), 1);
    assert_eq!(
        restored.read_local(
            3,
            Command::StringGet {
                key: "recovered".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"from-wal".to_vec())
        })
    );
}

#[test]
fn local_recovery_proof_covers_raft_wal_write_ahead_log_indexlog_and_pages() {
    let storage_dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        256,
        storage_dir.path().join("cache"),
        storage_dir.path().join("pages"),
        storage_dir.path().join("indexes"),
    );
    engine.load_shard(1);
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringSet {
                    key: "storage-recovered".to_string(),
                    value: b"page-value".to_vec(),
                },
            })
            .status
            .ok
    );
    engine.block_store().roll_slab().unwrap();
    assert!(
        engine
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: "storage-feature".to_string(),
                    points: large_feature_points(),
                },
            })
            .status
            .ok
    );

    let recovered = TemporalEngine::with_local_dirs(
        256,
        storage_dir.path().join("cache"),
        storage_dir.path().join("pages"),
        storage_dir.path().join("indexes"),
    );
    recovered.load_shard(1);
    let recovery = recovered.storage_recovery_report(1);
    assert_eq!(recovery.wal_records, 2);
    assert_eq!(recovery.index_log_records, 2);
    assert!(recovery.index_bytes > 0);
    assert!(recovery.index_write_atomic);
    assert!(recovery.active_page_slab_ids.len() >= 2);
    assert!(recovery.total_page_refs >= 2);
    assert_eq!(recovery.readable_page_refs, recovery.total_page_refs);
    assert!(recovery.all_live_pages_readable);
    assert!(recovery.slab_integrity.integrity_ok);
    assert!(recovery.feature_page_layout.packed_feature_pages > 1);
    assert_eq!(
        recovered
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::StringGet {
                    key: "storage-recovered".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"page-value".to_vec())
        }
    );

    let wal_dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(wal_dir.path(), 7, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "wal-recovered".to_string(),
            value: b"raft-value".to_vec(),
        })
        .unwrap();
    cluster.transfer_leader(2).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        wal_dir.path(),
        7,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.leader_id(), 2);
    assert_eq!(restored.commit_index(1).unwrap(), 1);
    assert_eq!(
        restored
            .read_local(
                3,
                Command::StringGet {
                    key: "wal-recovered".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"raft-value".to_vec())
        }
    );
}

#[test]
fn wal_backed_raft_cluster_auto_persists_commits_leadership_and_membership() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 77, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "auto-wal".to_string(),
            value: b"committed".to_vec(),
        })
        .unwrap();
    cluster.transfer_leader(2).unwrap();
    cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        77,
        [1, 2, 3, 4],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.leader_id(), 2);
    assert_eq!(restored.commit_index(1).unwrap(), 1);
    assert_eq!(
        restored.joint_membership(),
        Some(JointConsensusMembership {
            old_voters: vec![1, 2, 3],
            new_voters: vec![1, 2, 3, 4],
        })
    );
    assert_eq!(
        restored.read_local(
            3,
            Command::StringGet {
                key: "auto-wal".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"committed".to_vec())
        })
    );
    restored.commit_joint_consensus().unwrap();

    let rerestored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        77,
        [1, 2, 3, 4],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(rerestored.joint_membership(), None);
    assert_eq!(rerestored.membership().voters, vec![1, 2, 3, 4]);
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn wal_backed_raft_cluster_compacts_wal_tail_but_recovers_latest_state() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        max_segment_bytes: 512,
        min_keep_segment_num: 2,
        ..RaftConfig::default()
    };
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 78, [1, 2, 3], config).unwrap();
    for index in 0..8 {
        cluster
            .propose(Command::StringSet {
                key: "compact-wal".to_string(),
                value: format!("v{index}").into_bytes(),
            })
            .unwrap();
    }

    let wal = LocalRaftWal::new(dir.path());
    let recovery = wal.recover_node(78, 1).unwrap();
    let report = wal.segment_report(78, 1).unwrap();
    assert_eq!(report.segments.len(), 2);
    assert!(report.active_segment_id >= 2);
    assert!(report
        .segments
        .windows(2)
        .all(|pair| pair[0].segment_id < pair[1].segment_id));
    assert!(report.segments.iter().all(|segment| segment.bytes > 0));
    assert!(report
        .segments
        .iter()
        .all(|segment| segment.first_sequence <= segment.last_sequence));
    let record = recovery.record.unwrap();
    assert_eq!(record.hard_state.commit_index, 8);
    assert_eq!(record.entries.len(), 8);

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        78,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.commit_index(1).unwrap(), 8);
    assert_eq!(
        restored.read_local(
            2,
            Command::StringGet {
                key: "compact-wal".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"v7".to_vec())
        })
    );
    let admin = restored.matrixraft_runtime_admin_report();
    assert!(admin.wal_segment_lifecycle_present);
    assert_eq!(admin.wal_segment_count, 2);
    assert!(admin.wal_active_segment_id >= admin.wal_first_retained_segment_id);
    assert!(admin.wal_last_retained_segment_id >= admin.wal_first_retained_segment_id);
    assert!(admin.wal_total_bytes > 0);
    assert!(admin.wal_active_segment_bytes > 0);
    assert!(admin.wal_total_records > 0);
    assert!(admin.wal_last_sequence >= admin.wal_first_sequence);
    assert!(admin.wal_first_log_index > 0);
    assert!(admin.wal_last_log_index >= admin.wal_first_log_index);
    assert_eq!(admin.wal_last_log_index, 8);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "wal_segment_lifecycle" && item.ready));
}

#[test]
fn wal_backed_installed_snapshot_survives_restart_without_old_log_entries() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 79, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-wal-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-wal-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();

    let snapshot = cluster.create_snapshot().unwrap();
    assert_eq!(snapshot.last_included_index, 2);
    cluster.install_snapshot(3, snapshot).unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(79, 3).unwrap().unwrap();
    assert_eq!(record.hard_state.commit_index, 2);
    assert_eq!(
        record
            .installed_snapshot
            .as_ref()
            .map(|snapshot| snapshot.last_included_index),
        Some(2)
    );
    assert!(record.entries.is_empty());

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        79,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.commit_index(3).unwrap(), 2);
    assert_eq!(
        restored.read_local(
            3,
            Command::StringGet {
                key: "snapshot-wal-b".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"b".to_vec())
        })
    );
}

#[test]
fn wal_backed_apply_snapshot_fence_survives_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 80, [1, 2, 3], RaftConfig::default())
            .unwrap();
    for value in ["a", "b", "c"] {
        cluster
            .propose(Command::StringSet {
                key: "fenced-snapshot".to_string(),
                value: value.as_bytes().to_vec(),
            })
            .unwrap();
    }

    let snapshot = cluster.create_snapshot().unwrap();
    assert_eq!(snapshot.last_included_index, 3);
    cluster.install_snapshot(2, snapshot).unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(80, 2).unwrap().unwrap();
    assert_eq!(
        record.apply_snapshot_fence,
        RaftApplySnapshotFence {
            applied_index: 3,
            commit_index: 3,
            installed_snapshot_index: 3,
            first_retained_log_index: 0,
        }
    );
    validate_raft_apply_snapshot_fence(&record).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        80,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.commit_index(2).unwrap(), 3);
    assert_eq!(
        restored.read_local(
            2,
            Command::StringGet {
                key: "fenced-snapshot".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"c".to_vec())
        })
    );
}

#[test]
fn wal_recovery_rejects_inconsistent_apply_snapshot_fence() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 81, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "bad-fence".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(81, 1).unwrap().unwrap();
    record.apply_snapshot_fence.applied_index = record.hard_state.commit_index + 1;
    wal.persist_node_segmented(81, 1, &record, 1024, 1).unwrap();

    let error = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        81,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap_err();
    assert!(matches!(error, RaftError::ApplySnapshotFence(_)));
}

#[test]
fn wal_backed_storage_apply_fence_survives_snapshot_restart() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 82, [1, 2, 3], RaftConfig::default())
            .unwrap();
    for value in ["a", "b", "c"] {
        cluster
            .propose(Command::StringSet {
                key: "storage-fenced-snapshot".to_string(),
                value: value.as_bytes().to_vec(),
            })
            .unwrap();
    }
    let snapshot = cluster.create_snapshot().unwrap();
    cluster.install_snapshot(2, snapshot).unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let record = wal.load_node(82, 2).unwrap().unwrap();
    assert_eq!(record.storage_apply_fence.shard_id, 82);
    assert_eq!(record.storage_apply_fence.committed_index, 3);
    assert_eq!(record.storage_apply_fence.applied_index, 3);
    assert_eq!(record.storage_apply_fence.storage_epoch, 3);
    assert_eq!(
        record.storage_apply_fence.snapshot_id.as_deref(),
        Some("local-snapshot-82-1-3")
    );
    validate_raft_storage_apply_fence(&record).unwrap();

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        82,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(restored.commit_index(2).unwrap(), 3);
}

#[test]
fn wal_recovery_rejects_missing_storage_apply_fence() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 83, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "missing-storage-fence".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(83, 1).unwrap().unwrap();
    record.storage_apply_fence = RaftStorageApplyFence::default();
    wal.persist_node_segmented(83, 1, &record, 1024, 1).unwrap();

    let error = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        83,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RaftError::ApplySnapshotFence(message) if message.contains("missing raft storage apply fence"))
    );
}

#[test]
fn wal_recovery_rejects_corrupt_storage_apply_fence() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 84, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "corrupt-storage-fence".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(84, 1).unwrap().unwrap();
    record.storage_apply_fence.checksum = "bad-checksum".to_string();
    wal.persist_node_segmented(84, 1, &record, 1024, 1).unwrap();

    let error = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        84,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RaftError::ApplySnapshotFence(message) if message.contains("checksum mismatch"))
    );
}

#[test]
fn wal_recovery_rejects_ahead_of_storage_apply_fence() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 85, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "ahead-storage-fence".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();

    let wal = LocalRaftWal::new(dir.path());
    let mut record = wal.load_node(85, 1).unwrap().unwrap();
    record.storage_apply_fence.applied_index = record.storage_apply_fence.committed_index + 1;
    record.storage_apply_fence.checksum = raft_storage_apply_fence_checksum(
        record.storage_apply_fence.shard_id,
        record.storage_apply_fence.raft_term,
        record.storage_apply_fence.committed_index,
        record.storage_apply_fence.applied_index,
        record.storage_apply_fence.snapshot_id.as_deref(),
        record.storage_apply_fence.storage_epoch,
    );
    wal.persist_node_segmented(85, 1, &record, 1024, 1).unwrap();

    let error = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        85,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap_err();
    assert!(
        matches!(error, RaftError::ApplySnapshotFence(message) if message.contains("ahead of committed index"))
    );
}

/// Sets an env gate for the duration of a test and removes it on drop (even on panic), so a
/// gated-behavior test never leaks its flag into the rest of the single-threaded suite.
pub(super) struct EnvFlagGuard {
    name: &'static str,
}

/// Environment variables are process-global, and the tests that pin one run in parallel. A bare
/// set-on-create / remove-on-drop guard races destructively: two tests pin the same variable, the
/// first to finish removes it, and the second silently runs the rest of its body on the DEFAULT
/// path -- which is how the pipeline invariant test ended up watching fan-out appends. Pins are
/// therefore refcounted per variable: agreeing pins share the variable, a conflicting pin WAITS
/// until the holders drop, and only the last holder clears it. Tests that pin several variables
/// must acquire them in one fixed (alphabetical) order so two waiters cannot deadlock.
type EnvPinTable = std::sync::Mutex<std::collections::HashMap<&'static str, (usize, &'static str)>>;

fn env_pins() -> &'static (EnvPinTable, std::sync::Condvar) {
    static PINS: std::sync::OnceLock<(EnvPinTable, std::sync::Condvar)> = std::sync::OnceLock::new();
    PINS.get_or_init(|| (std::sync::Mutex::new(std::collections::HashMap::new()), std::sync::Condvar::new()))
}

impl EnvFlagGuard {
    fn pin(name: &'static str, value: &'static str) -> Self {
        let (table, released) = env_pins();
        let mut pins = table.lock().unwrap();
        loop {
            match pins.get_mut(name) {
                None => {
                    pins.insert(name, (1, value));
                    break;
                }
                Some((holders, held)) if *held == value => {
                    *holders += 1;
                    break;
                }
                // Someone holds the opposite value; wait for every holder to drop.
                Some(_) => pins = released.wait(pins).unwrap(),
            }
        }
        // Set while still holding the table lock, so a var never disagrees with its pin entry.
        std::env::set_var(name, value);
        Self { name }
    }

    pub(super) fn set(name: &'static str) -> Self {
        Self::pin(name, "1")
    }

    /// Explicitly DISABLES a gate for the test's lifetime. Needed because the shipped fixes are
    /// default-ON: leaving the variable unset now selects the fixed path, not the legacy one.
    pub(super) fn off(name: &'static str) -> Self {
        Self::pin(name, "0")
    }
}

impl Drop for EnvFlagGuard {
    fn drop(&mut self) {
        let (table, released) = env_pins();
        let mut pins = table.lock().unwrap();
        if let Some((holders, _)) = pins.get_mut(self.name) {
            *holders -= 1;
            if *holders == 0 {
                pins.remove(self.name);
                std::env::remove_var(self.name);
                released.notify_all();
            }
        }
    }
}

/// R3: a replica whose state machine has only APPLIED a prefix of what it has COMMITTED must
/// not answer a follower read with that half-applied state as if it were fresh.
#[test]
fn r3_replica_with_applied_below_commit_does_not_serve_unapplied_as_fresh() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    // All voters have committed AND applied index 1 at this point.
    assert_eq!(cluster.commit_index(3).unwrap(), 1);

    // Simulate replica 3 having committed index 1 but only applied through 0 (apply lag).
    cluster.set_applied_index_for_test(3, 0).unwrap();

    // The read is rejected because applied_index (0) is below the leader's committed frontier.
    let err = cluster
        .read_from_replica(
            3,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap_err();
    assert!(
        matches!(
            err,
            RaftError::ReplicaApplyLagging {
                replica_id: 3,
                replica_applied_index: 0,
                required_index: 1,
            }
        ),
        "expected ReplicaApplyLagging, got {err:?}"
    );

    // Once apply catches up to the committed frontier, the read is served normally.
    cluster.set_applied_index_for_test(3, 1).unwrap();
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
}

/// R5: promotion goes through a durable per-term quorum vote round. A candidate that can only
/// collect a minority of grants (its peers are partitioned away) cannot elect itself, so two
/// disjoint partitions can never both promote a leader.
#[test]
fn r5_quorum_election_requires_majority_and_minority_cannot_elect() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);

    // Healthy quorum: node 2 collects a majority of grants and is promoted.
    cluster.elect_leader(2).unwrap();
    assert_eq!(cluster.leader_id(), 2);
    let status = cluster.hard_state(2).unwrap();
    assert_eq!(status.voted_for, Some(2));

    // Partition away a majority: only node 1 remains reachable.
    cluster.set_alive(2, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    let err = cluster.elect_leader(1).unwrap_err();
    assert!(
        matches!(err, RaftError::NoMajority { live: 1, required: 2 }),
        "a minority partition must not elect, got {err:?}"
    );
    // The lone reachable node did NOT install itself as leader.
    assert_ne!(cluster.leader_id(), 1);
}

/// R4: a freshly elected leader must not serve a linearizable read-index until it has committed
/// an entry in its own term — otherwise it could answer with stale state predating its
/// leadership (the case a partitioned/just-superseded leader falls into).
#[test]
fn r4_new_leader_withholds_linearizable_read_until_committed_in_term() {
    let _guard = EnvFlagGuard::set("TS_RAFT_LEADER_READY_BARRIER");
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);

    // Promote node 2 to a new term. It has not yet committed anything in that term.
    cluster.elect_leader(2).unwrap();
    assert_eq!(cluster.leader_id(), 2);

    // The linearizable read-index is withheld: the leader is not yet ready.
    let err = cluster.read_index(2).unwrap_err();
    assert!(
        matches!(err, RaftError::LeaderUnavailable),
        "a not-yet-ready leader must withhold read-index, got {err:?}"
    );

    // Committing a write in the new term discharges the barrier; reads are then served.
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let response = cluster.read_index(2).unwrap();
    assert_eq!(response.leader_id, 2);
}

/// S2: with the state-image gate on, a snapshot carries an opaque engine STATE IMAGE instead of
/// the full committed entry log, and a far-behind follower reconstructs complete state from that
/// image alone (O(state)) — no per-entry replay of the whole history.
#[test]
fn s2_state_image_snapshot_installs_without_replaying_full_entry_log() {
    let _guard = EnvFlagGuard::set("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    for i in 0..16 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            })
            .unwrap();
    }

    let snapshot = cluster.create_snapshot().unwrap();
    assert!(
        snapshot.entries.is_empty(),
        "state-image snapshot must NOT carry the full committed entry log"
    );
    let image = snapshot
        .state_image
        .as_ref()
        .expect("state-image snapshot must carry an engine image");
    assert!(!image.index_bytes.is_empty(), "image must include the served index");

    // Far-behind follower installs from the image and serves every key correctly.
    cluster.install_snapshot(3, snapshot).unwrap();
    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let shard_id = inner.shard_id;
    let node3 = inner.nodes.get(&3).expect("follower 3 exists");
    for i in 0..16 {
        assert_eq!(
            node3
                .engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringGet {
                        key: format!("k{i}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("v{i}").into_bytes())
            },
            "follower must reconstruct key k{i} from the state image"
        );
    }
}

/// S2: the state image travels through the chunked install path (it rides chunk 0 of a
/// single-chunk, empty-entries stream) and the follower installs from it end-to-end.
#[test]
fn s2_state_image_snapshot_travels_through_chunk_stream() {
    let _guard = EnvFlagGuard::set("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    for i in 0..8 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            })
            .unwrap();
    }

    let chunks = cluster.build_install_snapshot_chunks(3, 4).unwrap();
    assert_eq!(
        chunks.len(),
        1,
        "an image snapshot has empty entries, so it is a single chunk"
    );
    assert!(
        chunks[0].state_image.is_some(),
        "the state image must ride the chunk stream"
    );
    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(response.snapshot_complete, "single-chunk install must complete");

    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let shard_id = inner.shard_id;
    let node3 = inner.nodes.get(&3).expect("follower 3 exists");
    for i in 0..8 {
        assert_eq!(
            node3
                .engine
                .execute(ExecuteRequest {
                    shard_id,
                    command: Command::StringGet {
                        key: format!("k{i}"),
                    },
                })
                .response,
            CommandResponse::Bytes {
                value: Some(format!("v{i}").into_bytes())
            },
        );
    }
}

/// S2 gate OFF (default): the snapshot still carries the committed entry log and no state image,
/// so behavior is byte-identical to before.
#[test]
fn s2_snapshot_gate_off_still_carries_entries_and_no_image() {
    let _gate = EnvFlagGuard::off("TS_RAFT_SNAPSHOT_STATE_IMAGE");
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    for i in 0..4 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{i}"),
                value: format!("v{i}").into_bytes(),
            })
            .unwrap();
    }
    let snapshot = cluster.create_snapshot().unwrap();
    assert!(
        snapshot.state_image.is_none(),
        "gate off must not attach a state image"
    );
    assert!(
        !snapshot.entries.is_empty(),
        "gate off must still carry the committed entry log"
    );
}

/// Sum the durable WAL records on disk for one node (each record == one real fdatasync in the
/// segmented append path), read via a fresh WAL handle so it reflects the on-disk truth.
/// Serialises the tests in this file that mutate process-global env gates.
static PART4_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn node_wal_record_count(root: &std::path::Path, shard: ShardId, node: RaftNodeId) -> u64 {
    LocalRaftWal::new(root)
        .segment_report(shard, node)
        .map(|report| report.segments.iter().map(|s| s.record_count).sum())
        .unwrap_or(0)
}

/// The counterpart to the local-node persist test: with NO local node declared -- the in-process
/// form, which genuinely hosts every node -- all three must still be persisted. This is what
/// makes the narrowing safe to apply unconditionally: it is reachable only once a runtime says
/// one node per process.
#[test]
fn persist_covers_every_node_when_no_local_node_is_declared() {
    let _serial = PART4_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    let base: Vec<u64> = (1..=3)
        .map(|n| node_wal_record_count(dir.path(), 1, n))
        .collect();
    for i in 0..10 {
        cluster
            .propose(Command::StringSet {
                key: format!("all-nodes-{i}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
    let after: Vec<u64> = (1..=3)
        .map(|n| node_wal_record_count(dir.path(), 1, n))
        .collect();

    for node in 0..3 {
        assert!(
            after[node] > base[node],
            "node {} must still be persisted when no local node is declared: {} -> {}",
            node + 1,
            base[node],
            after[node]
        );
    }
}

/// P6 core: a committed entry was applied into EVERY node held by this process, and each node
/// owns its own `TemporalEngine` -- so a deployed leader drove three engine WALs, and took three
/// barriers, per write. Once the local node is declared, only its engine is applied into. This
/// also pins the consequence, which is a refusal rather than a wrong answer: a read aimed at a
/// peer's in-process shadow is rejected as apply-lagging, never served from un-applied state.
/// No deployed read path aims there -- the server routes reads at the leader (which is the local
/// node on the leader's own process) or explicitly at the local node.
#[test]
fn apply_scoped_to_local_node_leaves_peer_engines_unapplied() {
    let _serial = PART4_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);

    cluster
        .propose(Command::StringSet {
            key: "apply-local".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    let read = |node| {
        cluster.read_from_replica(
            node,
            Command::StringGet {
                key: "apply-local".to_string(),
            },
        )
    };
    assert_eq!(
        read(1).unwrap(),
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        },
        "the local node must still apply committed entries"
    );
    for peer in [2, 3] {
        let err = read(peer).expect_err("a peer's shadow engine must not answer this read");
        assert!(
            matches!(
                err,
                RaftError::ReplicaApplyLagging { replica_id, .. } if replica_id == peer
            ),
            "peer {peer} must refuse the read as apply-lagging rather than answer from
             un-applied state, got {err:?}"
        );
    }
}

/// Counterpart to the above: with no local node declared, every node still applies, so the
/// in-process cluster that existing tests use is unchanged.
#[test]
fn apply_covers_every_node_when_no_local_node_is_declared() {
    let _serial = PART4_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    cluster
        .propose(Command::StringSet {
            key: "apply-all".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    for node in [1, 2, 3] {
        assert_eq!(
            cluster
                .read_from_replica(
                    node,
                    Command::StringGet {
                        key: "apply-all".to_string(),
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            },
            "node {node} must apply when no local node is declared"
        );
    }
}

/// P3 core: a DEPLOYED process owns one node but keeps a full cluster view, so the persist loop
/// fsyncs a record for every peer. Once the runtime has declared which node we are, only OUR
/// record may grow -- peers persist their own hard state before answering an RPC, so the leader
/// writing it buys no Raft safety. Durability of the local node is unchanged, which is what the
/// first assertion pins.
#[test]
fn persist_local_only_writes_just_this_nodes_record() {
    let _serial = PART4_ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_local_node_id(1);

    let base: Vec<u64> = (1..=3).map(|n| node_wal_record_count(dir.path(), 1, n)).collect();
    for i in 0..10 {
        cluster
            .propose(Command::StringSet {
                key: format!("p3-{i}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
    let after: Vec<u64> = (1..=3).map(|n| node_wal_record_count(dir.path(), 1, n)).collect();

    assert!(
        after[0] > base[0],
        "local node 1 must still take its durability barrier: {} -> {}",
        base[0],
        after[0]
    );
    for peer in 1..3 {
        assert_eq!(
            after[peer], base[peer],
            "peer node {} must not be persisted by us under local-only: {} -> {}",
            peer + 1,
            base[peer],
            after[peer]
        );
    }
}

/// P1 core: with `TS_RAFT_WAL_COALESCE` on, a burst of read-index calls (which only touch the
/// volatile `read_safety_state` accounting counters) must NOT append/fsync a single new WAL
/// record, while a real committed write still does. This is the idle read-index/tick fsync-storm
/// fix, and the durability barrier of a write is preserved.
#[test]
fn raft_wal_coalesce_skips_volatile_only_read_index_persists() {
    let _guard = EnvFlagGuard::set("TS_RAFT_WAL_COALESCE");
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    let baseline = node_wal_record_count(dir.path(), 1, 1);
    for _ in 0..25 {
        cluster.read_index(1).unwrap();
    }
    let after_reads = node_wal_record_count(dir.path(), 1, 1);
    assert_eq!(
        after_reads, baseline,
        "read-index (volatile-only) must not fsync a new WAL record when coalescing is on"
    );

    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let after_write = node_wal_record_count(dir.path(), 1, 1);
    assert!(
        after_write > after_reads,
        "a committed write must still persist a durable WAL record (coalescing never drops it)"
    );
}

/// P1 contrast: with the gate OFF the shipped behavior is byte-identical -- every read-index
/// persists, so the same burst grows the WAL by one record per call. This pins the win as real.
#[test]
fn raft_wal_coalesce_off_persists_every_read_index() {
    let _gate = EnvFlagGuard::off("TS_RAFT_WAL_COALESCE");
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    let baseline = node_wal_record_count(dir.path(), 1, 1);
    for _ in 0..25 {
        cluster.read_index(1).unwrap();
    }
    let after_reads = node_wal_record_count(dir.path(), 1, 1);
    assert!(
        after_reads >= baseline + 25,
        "gate-off must persist every read-index (byte-identical legacy behavior): {baseline} -> {after_reads}"
    );
}

/// P1 safety: coalescing must never lose a committed entry. Commit three writes with the gate on,
/// drop the cluster, and recover from the WAL: commit_index and the full log must survive.
#[test]
fn raft_wal_coalesce_preserves_committed_entries_across_restart() {
    let _guard = EnvFlagGuard::set("TS_RAFT_WAL_COALESCE");
    let dir = tempfile::tempdir().unwrap();
    {
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 9, [1, 2, 3], RaftConfig::default())
                .unwrap();
        for index in 0..3 {
            cluster
                .propose(Command::StringSet {
                    key: format!("k{index}"),
                    value: format!("v{index}").into_bytes(),
                })
                .unwrap();
        }
        assert_eq!(cluster.commit_index(1).unwrap(), 3);
    }

    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 9, [1, 2, 3], RaftConfig::default())
            .unwrap();
    assert_eq!(
        restored.commit_index(1).unwrap(),
        3,
        "committed index must survive restart under coalescing"
    );
    for index in 0..3 {
        assert_eq!(
            restored
                .read_from_replica(
                    1,
                    Command::StringGet {
                        key: format!("k{index}"),
                    },
                )
                .unwrap(),
            CommandResponse::Bytes {
                value: Some(format!("v{index}").into_bytes())
            },
            "committed write k{index} must be durable across restart"
        );
    }
}

/// P1 safety: hard-state (term + self-vote) must be fsynced before a RequestVote is advertised,
/// even with coalescing on -- the fingerprint includes hard_state, so the vote persist is never
/// coalesced away. Recover from the WAL and confirm the bumped term + vote survived.
#[test]
fn raft_wal_coalesce_keeps_hard_state_vote_durable_before_request() {
    let _coalesce = EnvFlagGuard::set("TS_RAFT_WAL_COALESCE");
    let dir = tempfile::tempdir().unwrap();
    let term_before;
    {
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 11, [1, 2, 3], RaftConfig::default())
                .unwrap();
        term_before = cluster.hard_state(2).unwrap().current_term;
        // Advertising a vote bumps node 2's term + records a self-vote and must fsync BEFORE
        // returning the request.
        cluster.build_vote_request(2, 3).unwrap();
    }

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        11,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    let hs = restored.hard_state(2).unwrap();
    assert_eq!(
        hs.current_term,
        term_before + 1,
        "the incremented vote term must be durable before the request was advertised"
    );
    assert_eq!(
        hs.voted_for,
        Some(2),
        "the self-vote must be durable before the request was advertised"
    );
}

/// P2: the replication deadline is configurable (was a hardcoded 5 s). With a low deadline and a
/// transport where no follower can ack, a propose returns `NoMajority` in ~deadline, NOT 5 s -- so
/// a lagging/rejecting follower no longer freezes the proposer.
#[test]
fn raft_replication_deadline_is_configurable_and_bounds_propose() {
    let config = RaftConfig {
        replication_deadline_ms: 300,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
    let transport = FlakyTransport {
        cluster: cluster.clone(),
        failures_left: Arc::new(Mutex::new(usize::MAX)),
    };

    let started = Instant::now();
    let result = cluster.propose_distributed(
        Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
        &transport,
    );
    let elapsed = started.elapsed();

    assert!(
        matches!(result, Err(RaftError::NoMajority { .. })),
        "an unreachable follower quorum must fail as NoMajority, got {result:?}"
    );
    assert!(
        elapsed < Duration::from_secs(2),
        "propose must return in ~deadline (300ms), not the legacy 5s: {elapsed:?}"
    );
    assert!(
        elapsed >= Duration::from_millis(150),
        "propose must actually honor the configured deadline before giving up: {elapsed:?}"
    );
}

/// P2: the in-order propose serialize gate must not deadlock and must preserve correctness --
/// concurrent proposers all commit, in sequential log order, and the run completes promptly (not a
/// pile of full-deadline stalls).
#[test]
fn raft_propose_serialize_commits_concurrent_proposals_in_order() {
    let _guard = EnvFlagGuard::set("TS_RAFT_PROPOSE_SERIALIZE");
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    let started = Instant::now();
    let mut handles = Vec::new();
    for index in 0..6 {
        let c = cluster.clone();
        handles.push(std::thread::spawn(move || {
            c.propose_distributed(
                Command::StringSet {
                    key: format!("k{index}"),
                    value: format!("v{index}").into_bytes(),
                },
                &c,
            )
        }));
    }
    for handle in handles {
        handle.join().unwrap().unwrap();
    }
    let elapsed = started.elapsed();
    assert_eq!(
        cluster.commit_index(1).unwrap(),
        6,
        "all six serialized proposals must commit with sequential indices"
    );
    assert!(
        elapsed < Duration::from_secs(5),
        "serialized concurrent proposals must not pile up into deadline stalls: {elapsed:?}"
    );
}



fn r8_branch_entry(index: u64, term: u64, value: &str) -> RaftLogEntry {
    RaftLogEntry {
        term,
        index,
        shard_id: 1,
        command: Command::StringSet {
            key: "k".to_string(),
            value: value.as_bytes().to_vec(),
        },
    }
}

/// R8 (Raft §7): a snapshot install whose boundary entry does NOT term-match the local log must
/// discard the entire log. The entries following a divergent boundary belong to an uncommitted,
/// superseded branch; retaining them folds that dead branch onto the snapshot's state.
#[test]
fn r8_snapshot_install_with_divergent_boundary_discards_whole_log() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();
    assert_eq!(cluster.commit_index(3).unwrap(), 2);

    // Rewrite node 3's log as a SUPERSEDED branch: the boundary index (2) carries term 1, and
    // indexes 3-4 sit past it on that same dead branch.
    cluster
        .set_node_log_for_test(
            3,
            vec![
                r8_branch_entry(1, 1, "v1"),
                r8_branch_entry(2, 1, "v2"),
                r8_branch_entry(3, 1, "dead-branch-3"),
                r8_branch_entry(4, 1, "dead-branch-4"),
            ],
        )
        .unwrap();

    // A snapshot from the WINNING branch: same boundary index (2), different term (5).
    cluster
        .install_snapshot(
            3,
            RaftSnapshot {
                shard_id: 1,
                last_included_term: 5,
                last_included_index: 2,
                external_snapshot_ref: None,
                entries: vec![r8_branch_entry(2, 5, "winning")],
                state_image: None,
                state_image_externalized: false,
            },
        )
        .unwrap();

    assert!(
        cluster.node_log_index_terms_for_test(3).unwrap().is_empty(),
        "divergent boundary term must discard the whole log, got {:?}",
        cluster.node_log_index_terms_for_test(3).unwrap()
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"winning".to_vec())
        },
        "state must come from the snapshot, not the superseded branch"
    );
}

/// R8 is CONDITIONAL, not a blanket truncation: when the boundary entry term-matches the
/// snapshot, the log tail past the boundary is a legitimate continuation and must be retained.
#[test]
fn r8_snapshot_install_with_matching_boundary_retains_tail() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v2".to_vec(),
        })
        .unwrap();

    // Boundary (index 2) is at term 1, and so is the snapshot -- the tail is a real continuation.
    cluster
        .set_node_log_for_test(
            3,
            vec![
                r8_branch_entry(1, 1, "v1"),
                r8_branch_entry(2, 1, "v2"),
                r8_branch_entry(3, 1, "tail-3"),
                r8_branch_entry(4, 1, "tail-4"),
            ],
        )
        .unwrap();

    cluster
        .install_snapshot(
            3,
            RaftSnapshot {
                shard_id: 1,
                last_included_term: 1,
                last_included_index: 2,
                external_snapshot_ref: None,
                entries: vec![r8_branch_entry(2, 1, "v2")],
                state_image: None,
                state_image_externalized: false,
            },
        )
        .unwrap();

    assert_eq!(
        cluster.node_log_index_terms_for_test(3).unwrap(),
        vec![(3, 1), (4, 1)],
        "a term-matching boundary must retain the tail past the snapshot"
    );
}

/// A peer whose sends keep failing must not be locked out of replication forever.
///
/// Building an AppendEntries request reserves inflight capacity against the target, and a response
/// -- success or rejection -- releases it. A send that never reaches the peer produces no response.
/// Without releasing that reservation the leader leaks one per attempt, and once the peer is over
/// its inflight limit every future request for it is refused: it cannot catch up, because nothing
/// is sent to it, and so it cannot drain the reservation, because that only drains on catching up.
///
/// That is a follower whose process dies while the leader keeps heartbeating at it: when it comes
/// back the leader never speaks to it again. It then serves reads from a stale log while reporting
/// no lag, because the only leader it can compare itself against is its own stale shadow.
#[test]
fn a_peer_whose_sends_fail_is_not_locked_out_of_replication() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    // Node 3's process is down, so it falls behind what the majority commits.
    cluster.set_alive(3, false).unwrap();
    for index in 0..4 {
        cluster
            .propose(Command::StringSet {
                key: format!("committed-{index}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    // The leader heartbeats at the dead process. Every send fails, exactly as the timer loop sees
    // it: a request is built, the send returns an error, and no response is ever recorded.
    let limit = RaftConfig::default().max_inflights_replicate.max(1);
    for _ in 0..(limit * 4) {
        if cluster.build_append_entries_request(3).is_ok() {
            cluster.record_append_entries_send_failure(3).unwrap();
        }
    }

    // Node 3 is back. The leader must still be willing to send to it.
    let outcome = cluster.build_append_entries_request(3);
    assert!(
        outcome.is_ok(),
        "a peer must not be permanently excluded from replication by failed sends; got {outcome:?}"
    );
    let request = outcome.unwrap();
    assert!(
        !request.entries.is_empty(),
        "the request should carry the entries the peer is missing, got {request:?}"
    );
}

/// A failed send leaves the reservation exactly as a response would have: released.
///
/// The counterpart is the accumulation the backpressure limit depends on, which is unchanged --
/// consecutive builds with no response and no failure still reserve, and still eventually refuse.
#[test]
fn a_failed_send_releases_its_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.set_alive(3, false).unwrap();
    for index in 0..4 {
        cluster
            .propose(Command::StringSet {
                key: format!("committed-{index}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    // Reserve right up to the limit, the way repeated un-answered sends do. A charged window
    // no longer refuses -- it degrades to single-entry probes -- but the reservations still
    // accumulate and still bind the batch size.
    let limit = RaftConfig::default().max_inflights_replicate.max(1);
    let mut probed = false;
    for _ in 0..(limit * 4) {
        let request = cluster
            .build_append_entries_request(3)
            .expect("a charged window degrades to a probe, never a refusal");
        if request.entries.len() == 1 {
            probed = true;
            break;
        }
    }
    assert!(
        probed,
        "un-answered builds should accumulate until the window degrades to probes -- the bound is deliberate"
    );

    // Reporting the send failure releases the reservations, and the next build is a full
    // batch again -- bigger than any probe.
    cluster.record_append_entries_send_failure(3).unwrap();
    let released = cluster
        .build_append_entries_request(3)
        .expect("a released reservation should let the next request through");
    assert!(
        released.entries.len() > 1,
        "the released window must reopen past probe size (got {})",
        released.entries.len()
    );
}

/// A rejected AppendEntries must make the next attempt ask about an EARLIER entry.
///
/// That retreat is the only way two diverged logs ever agree again. The leader lowers `next_index`
/// on a rejection, but the request builder used to derive `prev_log_index` from this process's
/// shadow of the peer, which a rejection does not move -- so every retry asked about the same
/// index and was rejected again. A follower whose log disagreed with the leader could never be
/// repaired: it rejected every heartbeat forever while the leader reported it as alive and merely
/// lagging, and it went on serving reads from its stale log.
#[test]
fn a_rejected_append_retreats_to_an_earlier_entry() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();

    // The peer keeps up for a while, so the leader has a real position for it...
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("early-{index}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
    // ...and then falls behind.
    cluster.set_alive(3, false).unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("later-{index}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }

    let first = cluster.build_append_entries_request(3).unwrap();
    assert!(
        first.prev_log_index > 1,
        "the peer needs somewhere to retreat to for this to mean anything, got {}",
        first.prev_log_index
    );

    // The peer says its log does not match at that point.
    cluster
        .record_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: first.term,
                success: false,
                match_index: first.prev_log_index,
                reject_reason: Some("log_mismatch".to_string()),
            },
        )
        .unwrap();

    let second = cluster.build_append_entries_request(3).unwrap();
    assert!(
        second.prev_log_index < first.prev_log_index,
        "a rejection must lower the point the next request asks about, but it stayed at {} --          the retry is identical, so the peer can never converge",
        second.prev_log_index
    );
}

/// A reply carrying a newer term makes the leader step down.
///
/// A node that was isolated keeps calling elections, so it rejoins with a term far ahead of the
/// leader's and refuses every append as a stale term -- correctly. If the leader ignores the term
/// in that reply it stays leader at the old term and keeps sending the same doomed request, so the
/// rejoining node can never be reintegrated and goes on serving reads missing committed data.
#[test]
fn a_reply_from_a_newer_term_makes_the_leader_step_down() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "committed".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let leader_term = cluster.status().current_term;

    // The peer was isolated and called elections of its own while away.
    cluster
        .record_append_entries_response(
            3,
            &AppendEntriesResponse {
                term: leader_term + 5,
                success: false,
                match_index: 0,
                reject_reason: Some("stale_term".to_string()),
            },
        )
        .unwrap();

    let status = cluster.status();
    assert!(
        status.current_term >= leader_term + 5,
        "the leader must adopt the newer term, stayed at {}",
        status.current_term
    );
    let still_leader = status
        .nodes
        .iter()
        .any(|node| node.node_id == 1 && node.role == RaftRole::Leader);
    assert!(
        !still_leader,
        "the leader must step down rather than keep sending requests that can only be refused"
    );
}

/// Answering a pre-vote changes nothing about the node that answers.
///
/// That is the whole property. A node that cannot reach the cluster campaigns on a timer, and if
/// asking raised terms or recorded votes, one unreachable node would drag a healthy cluster
/// through an election every time it tried.
#[test]
fn a_pre_vote_does_not_move_the_term_or_spend_the_vote() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    let term_before = cluster.status().current_term;

    let answer = cluster
        .receive_vote_request(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: term_before + 40,
            candidate_id: 3,
            target_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: true,
        })
        .unwrap();
    assert!(
        answer.vote_granted,
        "an up-to-date candidate at a newer term should be told yes: {answer:?}"
    );

    let term_after = cluster
        .status()
        .nodes
        .iter()
        .find(|node| node.node_id == 2)
        .map(|node| node.current_term)
        .unwrap();
    assert_eq!(
        term_after, term_before,
        "answering a pre-vote must not adopt the term it asks about"
    );

    // And no vote was spent: a different candidate can still win that term for real.
    let real = cluster
        .receive_vote_request(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: term_before + 40,
            candidate_id: 1,
            target_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: false,
        })
        .unwrap();
    assert!(
        real.vote_granted,
        "the pre-vote must not have recorded a vote, so this real one should be granted: {real:?}"
    );
}

/// A candidate whose log is behind is told no, and still nothing moves.
#[test]
fn a_pre_vote_from_a_behind_candidate_is_declined() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    for index in 0..3 {
        cluster
            .propose(Command::StringSet {
                key: format!("committed-{index}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
    let term_before = cluster
        .status()
        .nodes
        .iter()
        .find(|node| node.node_id == 2)
        .map(|node| node.current_term)
        .unwrap();

    let answer = cluster
        .receive_vote_request(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: term_before + 40,
            candidate_id: 3,
            target_id: 2,
            last_log_index: 0,
            last_log_term: 0,
            pre_vote: true,
        })
        .unwrap();
    assert!(
        !answer.vote_granted,
        "a candidate missing committed entries must be told no: {answer:?}"
    );
    let term_after = cluster
        .status()
        .nodes
        .iter()
        .find(|node| node.node_id == 2)
        .map(|node| node.current_term)
        .unwrap();
    assert_eq!(
        term_after, term_before,
        "a declined pre-vote must not move the term either"
    );
}

/// Preparing the question does not raise the asker's own term.
///
/// A node that never gets a majority of yes answers repeats this forever, so if preparing it cost
/// a term, an isolated node would still walk its term upward exactly as before.
#[test]
fn preparing_a_pre_vote_does_not_raise_the_term() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    let before = cluster.status().current_term;
    for _ in 0..25 {
        let probe = cluster.prepare_pre_vote(2).expect("a voter can ask");
        assert!(probe.pre_vote, "the request must be marked as a question");
        assert!(
            probe.term > before,
            "it should ask about the term it would use"
        );
    }
    let after = cluster
        .status()
        .nodes
        .iter()
        .map(|node| node.current_term)
        .max()
        .unwrap();
    assert_eq!(
        after, before,
        "asking twenty-five times must leave every term where it was"
    );
}

/// A follower that REFUSES an append still learns its leader is alive.
///
/// A follower marks its leader down when its election timer expires, and only an accepted append
/// used to mark it back up. A follower that is merely behind rejects appends while it catches up,
/// so it held a healthy leader as down indefinitely -- and every operation needing a leader, a
/// membership change among them, was refused with "leader is not available" while the leader was
/// leading a healthy majority.
#[test]
fn a_rejected_append_still_proves_the_leader_is_alive() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "committed".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    // Node 3 timed out waiting and wrote its leader off, the way the election timer does.
    cluster.set_alive(1, false).unwrap();

    // The leader speaks to it, but from a point their logs do not agree on, so it is refused.
    let response = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: cluster.status().current_term,
            leader_id: 1,
            target_id: 3,
            prev_log_index: 9,
            prev_log_term: 9,
            entries: Vec::new(),
            leader_commit: 1,
        })
        .unwrap();
    assert!(
        !response.success,
        "this append should be refused, or the test proves nothing: {response:?}"
    );

    let leader_alive = cluster
        .status()
        .nodes
        .iter()
        .any(|node| node.node_id == 1 && node.alive);
    assert!(
        leader_alive,
        "being spoken to by the leader is proof it is alive, even when we refuse what it sent"
    );
}

/// RAFT, on the format that now ships: every node durable, restored, and serving.
///
/// Consensus replicates operations -- that is what agreement means, and it does not change. What
/// changed is what each node's own log records once it has applied one, and the question a
/// distributed test has to answer is whether a node that RESTARTS from that log comes back with
/// the same shard the cluster agreed on.
///
/// Three nodes, a spread of kinds, one node taken out and caught up, then the whole cluster
/// restored from its logs and every node read back. A single node passing proves the codec; every
/// node passing after a restore proves the cluster.
#[test]
fn a_raft_cluster_restores_every_node_from_its_own_log_on_the_live_format() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig::default();
    let writes: Vec<(String, Vec<u8>)> = (0..12)
        .map(|index| {
            (
                format!("dr-{index:02}"),
                format!("value-{index:02}").into_bytes(),
            )
        })
        .collect();

    {
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], config.clone())
                .unwrap();

        // A node misses the middle of the run and has to catch up -- the case where a follower's
        // log and the leader's diverge in length before they converge in content.
        for (key, value) in writes.iter().take(4) {
            cluster
                .propose(Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap();
        }
        cluster.set_alive(3, false).unwrap();
        for (key, value) in writes.iter().skip(4).take(4) {
            cluster
                .propose(Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap();
        }
        cluster.set_alive(3, true).unwrap();
        cluster.catch_up(3).unwrap();
        for (key, value) in writes.iter().skip(8) {
            cluster
                .propose(Command::StringSet {
                    key: key.clone(),
                    value: value.clone(),
                })
                .unwrap();
        }

        // Every node agrees BEFORE anything restarts, so a later failure is about recovery rather
        // than about replication.
        for node_id in [1, 2, 3] {
            for (key, value) in &writes {
                assert_eq!(
                    cluster
                        .read_local(
                            node_id,
                            Command::StringGet {
                                key: key.to_string()
                            }
                        )
                        .unwrap(),
                    CommandResponse::Bytes {
                        value: Some(value.clone())
                    },
                    "node {node_id} did not have {key} before the restore"
                );
            }
        }
    }

    // Every node comes back from what its own log recorded.
    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 1, [1, 2, 3], config).unwrap();
    for node_id in [1, 2, 3] {
        for (key, value) in &writes {
            assert_eq!(
                restored
                    .read_local(
                        node_id,
                        Command::StringGet {
                            key: key.to_string()
                        }
                    )
                    .unwrap(),
                CommandResponse::Bytes {
                    value: Some(value.clone())
                },
                "node {node_id} lost {key} across the restore"
            );
        }
    }
    println!(
        "[raft] 3 nodes, {} writes, one outage and catch-up, all restored and serving",
        writes.len()
    );
}

/// The same cluster, with the log written in the LEGACY encoding.
///
/// The defaults moved; the old path has to keep working, and it is now the one that can rot
/// unnoticed. This is the same scenario with all three flags off, so a difference between them is
/// a difference in the encoding rather than in the test.
#[test]
fn a_raft_cluster_restores_on_the_legacy_encoding_too() {
    std::env::set_var("TS_WAL_BINARY_RECORDS", "0");
    std::env::set_var("TS_WAL_OUTCOME_ITEMS", "0");
    std::env::set_var("TS_WAL_DATA_ONLY", "0");
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig::default();
    {
        let cluster =
            RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], config.clone())
                .unwrap();
        for index in 0..8 {
            cluster
                .propose(Command::StringSet {
                    key: format!("lg-{index:02}"),
                    value: format!("legacy-{index:02}").into_bytes(),
                })
                .unwrap();
        }
    }
    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 1, [1, 2, 3], config).unwrap();
    for node_id in [1, 2, 3] {
        for index in 0..8 {
            assert_eq!(
                restored
                    .read_local(
                        node_id,
                        Command::StringGet {
                            key: format!("lg-{index:02}")
                        }
                    )
                    .unwrap(),
                CommandResponse::Bytes {
                    value: Some(format!("legacy-{index:02}").into_bytes())
                },
                "node {node_id} lost lg-{index:02} on the legacy encoding"
            );
        }
    }
    std::env::remove_var("TS_WAL_BINARY_RECORDS");
    std::env::remove_var("TS_WAL_OUTCOME_ITEMS");
    std::env::remove_var("TS_WAL_DATA_ONLY");
    println!("[raft] legacy encoding: 3 nodes restored and serving");
}
