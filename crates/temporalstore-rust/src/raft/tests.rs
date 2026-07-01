use super::*;
use crate::control::{Config, SetConfigRequest};
use crate::http::{json_response, parse_json, post_json_with_options, serve, HttpRequestOptions};
use crate::meta::{ServerMetaInfo, TableMetaInfo, TablePartition};
use crate::rebalance::{
    CppPartitionSetTopology, DeterministicTaskScheduler, NetworkSchedulerTaskExecution,
    RebalanceStep, SchedulerTaskKind, SchedulerTaskResult, ShardReplica, ShardReplicaState,
    ShardRole, TaskSchedulerOptions,
};
use crate::types::{Command, FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow};
use std::time::{Duration, Instant};

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn rustraft_parity_contract_is_library_consumable_and_openraft_free() {
    let contract = rustraft_parity_contract();
    assert!(contract.openraft_dependency_removed);
    assert_eq!(
        contract.consensus_backend_boundary,
        "temporalstore_rust::raft::DataRaftConsensusBackend"
    );
    assert!(contract
        .requirements
        .iter()
        .any(|requirement| requirement.id == "leader_write_authority"));
    assert!(contract
        .requirements
        .iter()
        .all(|requirement| requirement.required_for_production));

    let cargo_toml = include_str!("../../Cargo.toml").to_ascii_lowercase();
    assert!(!cargo_toml.contains("openraft"));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence raft_temporal_raft_process_read_safety_and_membership_matrix
#[test]
fn rustraft_parity_report_tracks_distributed_readiness_fields() {
    let readiness = distributed_raft_readiness();
    let report = rustraft_parity_report(&readiness);
    assert!(report.contract.openraft_dependency_removed);
    assert!(report
        .satisfied
        .contains(&"leader_write_authority".to_string()));
    assert!(report
        .satisfied
        .contains(&"snapshot_tail_catchup".to_string()));
    assert!(report
        .satisfied
        .contains(&"metaserver_membership_workflow".to_string()));
    assert!(report.missing.is_empty(), "missing: {:?}", report.missing);
    assert!(report.ready);
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_round_trips_cxx_style_header() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        oplog_sequence: 17,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    };

    let bytes = serialize_data_raft_log(&entry).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        DATA_RAFT_LOG_MAGIC
    );
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        DATA_RAFT_CODEC_VERSION
    );
    let decoded = parse_data_raft_log(&bytes).unwrap();
    assert_eq!(decoded.shard_id, entry.shard_id);
    assert_eq!(decoded.raft_index, entry.raft_index);
    assert_eq!(decoded.log_id, entry.log_id);
    assert_eq!(decoded.oplog_sequence, entry.oplog_sequence);
    assert_eq!(decoded.command, entry.command);
    assert!(decoded.log_size > 0);
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_rejects_bad_header_and_sequence() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        oplog_sequence: 17,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    };
    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes[0] = 0;
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes[4..8].copy_from_slice(&2_u32.to_le_bytes());
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let mut bytes = serialize_data_raft_log(&entry).unwrap();
    bytes.truncate(DATA_RAFT_LOG_HEADER_LEN + 1);
    assert!(matches!(
        parse_data_raft_log(&bytes),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let zero_sequence = DataRaftLogCodecEntry {
        oplog_sequence: 0,
        ..entry
    };
    assert!(matches!(
        serialize_data_raft_log(&zero_sequence),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn cpp_data_raft_replication_rejects_corrupt_log_payload() {
    assert!(matches!(
        parse_data_raft_log(b"bad"),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let entry = DataRaftLogCodecEntry {
        shard_id: 1,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        oplog_sequence: 1,
        command: Command::StringSet {
            key: "clicks".to_string(),
            value: b"1".to_vec(),
        },
    };
    let mut encoded = serialize_data_raft_log(&entry).unwrap();
    encoded[0] = 0;
    assert!(matches!(
        parse_data_raft_log(&encoded),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_command_codec_round_trips_batch_request() {
    let entry = DataRaftCommandCodecEntry {
        shard_id: 7,
        raft_index: 19,
        request_id: 23,
        commands: vec![
            Command::StringSet {
                key: "k1".to_string(),
                value: b"v1".to_vec(),
            },
            Command::StringSet {
                key: "k2".to_string(),
                value: b"v2".to_vec(),
            },
        ],
    };

    let bytes = serialize_data_raft_command(&entry).unwrap();
    assert_eq!(
        u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        DATA_RAFT_COMMAND_MAGIC
    );
    let decoded = parse_data_raft_command(&bytes).unwrap();
    assert_eq!(decoded, entry);
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_command_codec_round_trips_chunked_timestamped_kv_payload() {
    let points = large_feature_points();
    let entry = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 11,
        request_id: 42,
        commands: vec![Command::FeatureAppend {
            key: "codec-chunked-feature".to_string(),
            points: points.clone(),
        }],
    };

    let bytes = serialize_data_raft_command(&entry).unwrap();
    let decoded = parse_data_raft_command(&bytes).unwrap();
    assert_eq!(decoded, entry);
    assert!(matches!(
        decoded.commands.first(),
        Some(Command::FeatureAppend { key, points: decoded_points })
            if key == "codec-chunked-feature" && decoded_points == &points
    ));
}

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
#[test]
fn cpp_data_raft_replication_rejects_invalid_command_payload() {
    assert!(matches!(
        parse_data_raft_command(b"bad"),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let empty_batch = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 1,
        request_id: 1,
        commands: Vec::new(),
    };
    assert!(matches!(
        serialize_data_raft_command(&empty_batch),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let missing_partition = DataRaftCommandCodecEntry {
        shard_id: 0,
        raft_index: 1,
        request_id: 1,
        commands: vec![Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        }],
    };
    assert!(matches!(
        serialize_data_raft_command(&missing_partition),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));

    let valid = DataRaftCommandCodecEntry {
        shard_id: 1,
        raft_index: 1,
        request_id: 1,
        commands: vec![Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        }],
    };
    let mut encoded = serialize_data_raft_command(&valid).unwrap();
    encoded[0] = 0;
    assert!(matches!(
        parse_data_raft_command(&encoded),
        Err(RaftError::InvalidDataRaftCommand(_))
    ));
}

#[test]
fn cpp_data_raft_unavailable_consensus_fails_closed_for_safety_operations() {
    let options = DataRaftConsensusOptions {
        shard_id: 11,
        replica_id: 11,
        group_id: 11,
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = UnavailableDataRaftConsensusBackend::new(options);
    let peer = DataRaftPeer {
        replica_id: 12,
        raft_addr: "127.0.0.1:17012".to_string(),
        snapshot_addr: "127.0.0.1:18012".to_string(),
        auto_promote: false,
    };

    assert!(matches!(backend.start(), Err(RaftError::Transport(_))));
    assert!(!backend.is_leader());
    assert!(matches!(
        backend.propose(b"x".to_vec()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.wait_for_applied_index(1, 1),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.trigger_snapshot(),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.read_index(1),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.add_peer(peer.clone()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.add_learner(peer.clone()),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.promote_peer(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.remove_peer(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.transfer_leader(peer.replica_id),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.campaign(1, false),
        Err(RaftError::Transport(_))
    ));
    assert!(matches!(
        backend.can_serve_bounded_stale_read(0),
        Err(RaftError::Transport(_))
    ));
}

#[cfg(feature = "temporal-raft-engine")]
// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle raft_temporal_raft_process_rollout_evidence
#[test]
fn temporal_raft_data_node_backend_persists_log_snapshot_read_index_and_leader_transfer() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let options = DataRaftConsensusOptions {
        shard_id: 7,
        replica_id: 1,
        group_id: 77,
        wal_dir: Some(dir.path().to_path_buf()),
        peers: vec![
            DataRaftPeer {
                replica_id: 2,
                raft_addr: "127.0.0.1:17002".to_string(),
                snapshot_addr: "127.0.0.1:18002".to_string(),
                auto_promote: false,
            },
            DataRaftPeer {
                replica_id: 3,
                raft_addr: "127.0.0.1:17003".to_string(),
                snapshot_addr: "127.0.0.1:18003".to_string(),
                auto_promote: false,
            },
        ],
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    assert!(backend.is_leader());

    let command = Command::StringSet {
        key: "TemporalRaft-k".to_string(),
        value: b"TemporalRaft-v".to_vec(),
    };
    let encoded = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        oplog_sequence: 1,
        command,
    })
    .unwrap();
    let index = backend.propose(encoded).unwrap();
    assert_eq!(index, 1);
    backend.wait_for_applied_index(index, 10).unwrap();
    backend.read_index(10).unwrap();
    let report = backend.report();
    assert!(report.storage_apply_fence_valid);
    assert_eq!(report.storage_apply_fence.shard_id, 7);
    assert_eq!(report.storage_apply_fence.committed_index, index);
    assert_eq!(report.storage_apply_fence.applied_index, index);
    assert!(report.storage_apply_fence.snapshot_id.is_none());

    let read = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringGet {
            key: "TemporalRaft-k".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"TemporalRaft-v".to_vec())
        }
    );

    let snapshot_index = backend.trigger_snapshot().unwrap();
    assert_eq!(snapshot_index, index);
    let meta = backend.build_temporal_raft_snapshot_meta_compat();
    assert_eq!(meta.last_log_id.unwrap().index, index);
    let report = backend.report();
    assert!(report.snapshot_installed);
    assert!(report.storage_apply_fence_valid);
    assert_eq!(
        report.storage_apply_fence.snapshot_id.as_deref(),
        Some(meta.snapshot_id.as_str())
    );
    assert_eq!(report.storage_apply_fence.storage_epoch, index);

    backend.transfer_leader(2).unwrap();
    assert!(!backend.is_leader());
    backend.campaign(10, false).unwrap();
    assert!(backend.is_leader());
    let restored = TemporalRaftConsensusBackend::new_data_node(options, engine);
    let status = restored.status().unwrap();
    assert_eq!(status.leader_replica_id, 1);
    assert_eq!(status.applied_index, 1);
    assert_eq!(status.first_index, 2);
    assert_eq!(status.fatal_event_count, 0);
    assert!(!status.snapshot_creating);
    assert!(!status.snapshot_loading);
    assert_eq!(restored.report().durable_log_records, 0);
    assert!(restored.report().storage_apply_fence_valid);
    assert_eq!(restored.report().storage_apply_fence.applied_index, 1);
    assert!(restored.report().storage_apply_fence.snapshot_id.is_some());
    assert!(restored.report().campaign_supported);
    assert!(restored.report().learner_bootstrap_supported);
}

#[cfg(feature = "temporal-raft-engine")]
#[test]
#[should_panic(expected = "temporal raft storage fence checksum mismatch")]
fn temporal_raft_data_node_backend_rejects_corrupt_storage_apply_fence_on_restart() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let options = DataRaftConsensusOptions {
        shard_id: 7,
        replica_id: 1,
        group_id: 77,
        wal_dir: Some(dir.path().to_path_buf()),
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    let encoded = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        oplog_sequence: 1,
        command: Command::StringSet {
            key: "TemporalRaft-corrupt-fence".to_string(),
            value: b"value".to_vec(),
        },
    })
    .unwrap();
    backend.propose(encoded).unwrap();
    backend.trigger_snapshot().unwrap();

    let path = dir.path().join("temporalraft-7-1.json");
    let bytes = std::fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["storage_apply_fence"]["checksum"] = serde_json::Value::String("corrupt".to_string());
    std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

    let _ = TemporalRaftConsensusBackend::new_data_node(options, engine);
}

#[cfg(feature = "temporal-raft-engine")]
// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
#[test]
fn temporal_raft_data_node_backend_bootstraps_learner_and_auto_promotes_peer() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::default();
    let options = DataRaftConsensusOptions {
        shard_id: 8,
        replica_id: 4,
        group_id: 78,
        wal_dir: Some(dir.path().to_path_buf()),
        bootstrap_as_learner: true,
        peers: vec![
            DataRaftPeer {
                replica_id: 1,
                raft_addr: "127.0.0.1:17001".to_string(),
                snapshot_addr: "127.0.0.1:18001".to_string(),
                auto_promote: false,
            },
            DataRaftPeer {
                replica_id: 2,
                raft_addr: "127.0.0.1:17002".to_string(),
                snapshot_addr: "127.0.0.1:18002".to_string(),
                auto_promote: false,
            },
        ],
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_data_node(options.clone(), engine);
    backend.start().unwrap();

    let status = backend.status().unwrap();
    assert!(status.learner);
    assert!(!status.leader);
    assert_eq!(status.voter_count, 2);
    assert_eq!(status.learner_count, 1);
    assert!(matches!(
        backend.campaign(10, false),
        Err(RaftError::NodeNotFound(4))
    ));

    backend
        .add_learner(DataRaftPeer {
            replica_id: 5,
            raft_addr: "127.0.0.1:17005".to_string(),
            snapshot_addr: "127.0.0.1:18005".to_string(),
            auto_promote: true,
        })
        .unwrap();
    let status = backend.status().unwrap();
    assert_eq!(status.voter_count, 3);
    assert_eq!(status.learner_count, 1);

    let restored = TemporalRaftConsensusBackend::new_data_node(options, TemporalEngine::default());
    let restored_status = restored.status().unwrap();
    assert!(restored_status.learner);
    assert_eq!(restored_status.voter_count, 3);
    assert_eq!(restored_status.learner_count, 1);
}

#[cfg(feature = "temporal-raft-engine")]
// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
#[test]
fn temporal_raft_metaserver_backend_supports_membership_and_bounded_reads() {
    use super::temporal_raft_integration::TemporalRaftConsensusBackend;

    let dir = tempfile::tempdir().unwrap();
    let options = DataRaftConsensusOptions {
        shard_id: 0,
        replica_id: 10,
        group_id: 90,
        wal_dir: Some(dir.path().to_path_buf()),
        peers: vec![
            DataRaftPeer {
                replica_id: 11,
                raft_addr: "127.0.0.1:17111".to_string(),
                snapshot_addr: "127.0.0.1:18111".to_string(),
                auto_promote: false,
            },
            DataRaftPeer {
                replica_id: 12,
                raft_addr: "127.0.0.1:17112".to_string(),
                snapshot_addr: "127.0.0.1:18112".to_string(),
                auto_promote: false,
            },
        ],
        ..DataRaftConsensusOptions::default()
    };
    let mut backend = TemporalRaftConsensusBackend::new_metaserver(options.clone());
    backend.start().unwrap();
    assert_eq!(backend.status().unwrap().voter_count, 3);

    let learner = DataRaftPeer {
        replica_id: 13,
        raft_addr: "127.0.0.1:17113".to_string(),
        snapshot_addr: "127.0.0.1:18113".to_string(),
        auto_promote: false,
    };
    backend.add_learner(learner.clone()).unwrap();
    assert_eq!(backend.status().unwrap().learner_count, 1);
    backend.promote_peer(learner.replica_id).unwrap();
    assert_eq!(backend.status().unwrap().voter_count, 4);
    assert_eq!(backend.status().unwrap().learner_count, 0);
    backend.remove_peer(12).unwrap();
    assert_eq!(backend.status().unwrap().voter_count, 3);
    backend.can_serve_bounded_stale_read(0).unwrap();
    assert!(backend.report().membership_change_supported);
    assert!(backend.report().campaign_supported);

    let membership = backend.temporal_raft_membership_compat();
    let voters = membership.voter_ids().collect::<Vec<_>>();
    assert_eq!(voters, vec![10, 11, 13]);

    backend.trigger_snapshot().unwrap();
    let snapshot_meta = backend.build_temporal_raft_snapshot_meta_compat();
    assert!(snapshot_meta.last_log_id.is_some());
    assert!(backend.report().snapshot_installed);
    backend.transfer_leader(11).unwrap();
    assert!(!backend.is_leader());
    backend.campaign(10, false).unwrap();
    assert!(backend.is_leader());
    let status = backend.status().unwrap();
    assert_eq!(status.leader_replica_id, 10);
    assert_eq!(status.fatal_event_count, 0);
    assert!(!status.snapshot_creating);
    assert!(!status.snapshot_loading);

    let restored = TemporalRaftConsensusBackend::new_metaserver(options);
    let restored_status = restored.status().unwrap();
    assert_eq!(restored_status.voter_count, 3);
    assert_eq!(restored_status.leader_replica_id, 10);
    assert_eq!(restored_status.last_index, 2);
    assert!(restored.report().leader_transfer_supported);
}

#[test]
fn committed_data_raft_applier_replays_once_and_rejects_wrong_shard() {
    let engine = TemporalEngine::default();
    engine.load_shard(7);
    let committed = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        oplog_sequence: 17,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        },
    })
    .unwrap();
    let mut applier = DataRaftCommittedLogApplier::new(7);

    let response = applier.apply(11, &committed, &engine).unwrap();
    assert_eq!(response, Some(CommandResponse::Empty));
    assert_eq!(applier.applied_raft_index(), 11);
    assert_eq!(applier.applied_oplog_sequence(), 17);
    assert_eq!(applier.apply(11, &committed, &engine).unwrap(), None);

    let read = engine.execute(ExecuteRequest {
        shard_id: 7,
        command: Command::StringGet {
            key: "k".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"v".to_vec())
        }
    );

    let wrong_shard = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 8,
        raft_index: 12,
        log_id: 14,
        log_size: 0,
        oplog_sequence: 18,
        command: Command::StringSet {
            key: "k".to_string(),
            value: b"bad".to_vec(),
        },
    })
    .unwrap();
    assert!(matches!(
        applier.apply(12, &wrong_shard, &engine),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

#[test]
fn committed_data_raft_applier_forces_durable_storage_when_async_storage_enabled() {
    let dir = tempfile::tempdir().unwrap();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        dir.path().join("indexes"),
    );
    engine.load_shard(7);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 7,
                config: Config {
                    version: 2,
                    async_storage: true,
                    ..Config::default()
                },
            })
            .ok
    );
    let committed = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        oplog_sequence: 17,
        command: Command::StringSet {
            key: "raft".to_string(),
            value: b"durable".to_vec(),
        },
    })
    .unwrap();
    let mut applier = DataRaftCommittedLogApplier::new(7);

    assert_eq!(
        applier.apply(11, &committed, &engine).unwrap(),
        Some(CommandResponse::Empty)
    );
    assert_eq!(engine.block_store().stats().writes, 1);
    assert_eq!(engine.write_ahead_log_store().stats(7).writes, 1);
    assert_eq!(engine.index_log_store().stats(7).writes, 1);
    assert_eq!(
        engine
            .execute(ExecuteRequest {
                shard_id: 7,
                command: Command::StringGet {
                    key: "raft".to_string(),
                },
            })
            .response,
        CommandResponse::Bytes {
            value: Some(b"durable".to_vec())
        }
    );
}

#[test]
fn raft_replicates_committed_write_to_majority_and_followers() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    for node_id in [1, 2, 3] {
        let response = cluster
            .read_local(
                node_id,
                Command::StringGet {
                    key: "k".to_string(),
                },
            )
            .unwrap();
        assert_eq!(
            response,
            CommandResponse::Bytes {
                value: Some(b"v".to_vec())
            }
        );
        assert_eq!(cluster.commit_index(node_id).unwrap(), 1);
    }
}

#[test]
fn raft_replicates_chunked_timestamped_kv_page_format_to_followers() {
    let config = RaftConfig {
        max_memory_replicate_log_bytes: 512 * 1024,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
    let points = large_feature_points();
    let ips_points = vec![
        FeaturePoint {
            timestamp_ms: 101,
            value: b"ips-101".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 202,
            value: b"ips-202".to_vec(),
        },
    ];

    cluster
        .propose(Command::FeatureAppend {
            key: "chunked-raft-feature".to_string(),
            points: points.clone(),
        })
        .unwrap();
    cluster
        .propose(Command::IpsLoad {
            key: "chunked-raft-ips".to_string(),
            points: ips_points.clone(),
        })
        .unwrap();

    cluster.catch_up(2).unwrap();
    cluster.catch_up(3).unwrap();
    for node_id in [1, 2, 3] {
        assert_eq!(cluster.commit_index(node_id).unwrap(), 2);
        assert_eq!(
            cluster
                .read_from_replica(
                    node_id,
                    Command::FeatureQuery {
                        key: "chunked-raft-feature".to_string(),
                        start_ms: 0,
                        end_ms: 2_000,
                        count: None,
                    },
                )
                .unwrap(),
            CommandResponse::FeaturePoints {
                points: points.clone()
            }
        );
        assert_eq!(
            cluster
                .read_from_replica(
                    node_id,
                    Command::IpsQueryRange {
                        key: "chunked-raft-ips".to_string(),
                        start_ms: 0,
                        end_ms: 300,
                        count: None,
                    },
                )
                .unwrap(),
            CommandResponse::FeaturePoints {
                points: ips_points.clone()
            }
        );
    }
}

#[test]
fn raft_snapshot_install_preserves_chunked_timestamped_kv_page_format() {
    let config = RaftConfig {
        max_memory_replicate_log_bytes: 512 * 1024,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(1, [1, 2, 3], config).unwrap();
    let points = large_feature_points();
    let ips_points = vec![
        FeaturePoint {
            timestamp_ms: 303,
            value: b"ips-303".to_vec(),
        },
        FeaturePoint {
            timestamp_ms: 404,
            value: b"ips-404".to_vec(),
        },
    ];

    cluster
        .propose(Command::FeatureAppend {
            key: "snapshot-chunked-feature".to_string(),
            points: points.clone(),
        })
        .unwrap();
    cluster
        .propose(Command::IpsLoad {
            key: "snapshot-chunked-ips".to_string(),
            points: ips_points.clone(),
        })
        .unwrap();

    let snapshot = cluster.build_install_snapshot_request(3).unwrap().snapshot;
    cluster.install_snapshot(3, snapshot).unwrap();

    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::FeatureQuery {
                    key: "snapshot-chunked-feature".to_string(),
                    start_ms: 0,
                    end_ms: 2_000,
                    count: None,
                },
            )
            .unwrap(),
        CommandResponse::FeaturePoints {
            points: points.clone()
        }
    );
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::IpsQueryRange {
                    key: "snapshot-chunked-ips".to_string(),
                    start_ms: 0,
                    end_ms: 500,
                    count: None,
                },
            )
            .unwrap(),
        CommandResponse::FeaturePoints {
            points: ips_points.clone()
        }
    );

    let inner = cluster.inner.read().expect("raft cluster lock poisoned");
    let node = inner.nodes.get(&3).expect("snapshot target exists");
    let layout = node
        .engine
        .storage_recovery_report(inner.shard_id)
        .feature_page_layout;
    assert!(layout.packed_feature_pages > 1);
    assert_eq!(layout.indexed_feature_points, points.len());
    assert!(layout.corrupt_packed_feature_pages.is_empty());
    assert!(layout.missing_indexed_timestamps.is_empty());
    assert!(layout.orphan_packed_timestamps.is_empty());
    assert!(layout.duplicate_packed_timestamps.is_empty());
}

// shared-corpus: raft_rustraft_read_lease_fault_matrix
#[test]
fn raft_rejects_write_without_majority() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(2, false).unwrap();
    cluster.set_alive(3, false).unwrap();

    let err = cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap_err();
    assert_eq!(
        err,
        RaftError::NoMajority {
            live: 1,
            required: 2
        }
    );
}

// shared-corpus: raft_rustraft_read_lease_fault_matrix
#[test]
fn raft_follower_catches_up_after_outage() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v1".to_vec(),
        })
        .unwrap();

    assert_eq!(cluster.commit_index(3).unwrap(), 0);
    cluster.set_alive(3, true).unwrap();
    cluster.catch_up(3).unwrap();
    let response = cluster
        .read_local(
            3,
            Command::StringGet {
                key: "k".to_string(),
            },
        )
        .unwrap();
    assert_eq!(
        response,
        CommandResponse::Bytes {
            value: Some(b"v1".to_vec())
        }
    );
    assert_eq!(cluster.commit_index(3).unwrap(), 1);
}

#[test]
fn raft_rejects_electing_stale_replica_until_it_catches_up() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "stale-election".to_string(),
            value: b"committed".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert_eq!(
        cluster.elect_leader(3).unwrap_err(),
        RaftError::ReplicaLagging {
            replica_id: 3,
            replica_commit_index: 0,
            leader_commit_index: 1,
        }
    );
    assert_eq!(cluster.leader_id(), 1);

    cluster.catch_up(3).unwrap();
    cluster.elect_leader(3).unwrap();
    assert_eq!(cluster.leader_id(), 3);
}

#[test]
fn raft_transport_append_entries_catches_up_lagging_replica() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"transport-value".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let request = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(request.leader_id, 1);
    assert_eq!(request.target_id, 3);
    assert_eq!(request.entries.len(), 1);
    let response = cluster.append_entries(request).unwrap();
    assert!(response.success);
    assert_eq!(response.match_index, 1);
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
            value: Some(b"transport-value".to_vec())
        }
    );
}

#[test]
fn raft_transport_rejects_stale_append_entries_and_behind_vote() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "k".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    cluster.elect_leader(2).unwrap();
    let stale_append = AppendEntriesRequest {
        rpc: None,
        shard_id: 1,
        term: 1,
        leader_id: 1,
        target_id: 2,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: Vec::new(),
        leader_commit: 0,
    };
    let append_response = cluster.append_entries(stale_append).unwrap();
    assert!(!append_response.success);
    assert_eq!(append_response.reject_reason.as_deref(), Some("stale_term"));

    let vote_response = cluster
        .request_vote(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: cluster.hard_state(2).unwrap().current_term + 1,
            candidate_id: 3,
            target_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(!vote_response.vote_granted);
    assert_eq!(
        vote_response.reject_reason.as_deref(),
        Some("candidate_log_behind")
    );
}

// shared-corpus: raft_rustraft_replication_backpressure
// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_replication_backpressure
#[test]
fn append_entries_reorder_queue_records_gap_and_recovers_after_prefix_arrives() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            enable_reorder_queue: true,
            reorder_window_size: 4,
            reorder_timeout_us: 1_000,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "reorder-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "reorder-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let queued = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 3,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 3,
                shard_id: 1,
                command: Command::StringSet {
                    key: "reorder-c".to_string(),
                    value: b"c".to_vec(),
                },
            }],
            leader_commit: 3,
        })
        .unwrap();
    assert!(!queued.success);
    assert_eq!(
        queued.reject_reason.as_deref(),
        Some("out_of_order_append_queued")
    );

    let admin = cluster.byteraft_runtime_admin_report();
    let peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .unwrap();
    assert_eq!(peer.reorder_queue_depth, 1);
    assert_eq!(peer.out_of_order_append_rejections, 1);
    assert_eq!(peer.reorder_entries_rejected, 1);
    assert_eq!(peer.reorder_entry_timeouts, 0);
    assert!(admin.out_of_order_append_handling_present);

    let prefix = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(prefix.prev_log_index, 0);
    assert_eq!(
        prefix
            .entries
            .iter()
            .map(|entry| entry.index)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let recovered = cluster.receive_append_entries(prefix).unwrap();
    assert!(recovered.success);
    assert_eq!(cluster.commit_index(3).unwrap(), 2);
    assert_eq!(
        cluster
            .read_from_replica(
                3,
                Command::StringGet {
                    key: "reorder-b".to_string()
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(b"b".to_vec())
        }
    );
}

// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_replication_backpressure
#[test]
fn replication_pipeline_enforces_inflight_apply_memory_and_oversized_limits() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            max_inflights_replicate: 1,
            max_memory_replicate_log_bytes: 512,
            max_inflights_apply_task: 1,
            max_apply_batch_bytes: 16,
            enable_reorder_queue: true,
            reorder_window_size: 4,
            reorder_timeout_us: 1_000,
            ..RaftConfig::default()
        },
    )
    .unwrap();

    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "pipeline-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "pipeline-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let first = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(first.entries.len(), 1);
    let append_pressure = cluster.build_append_entries_request(3).unwrap_err();
    assert!(matches!(
        append_pressure,
        RaftError::AppendBackpressure { .. }
    ));

    let apply_pressure = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 3,
                shard_id: 1,
                command: Command::StringSet {
                    key: "apply-pressure".to_string(),
                    value: vec![b'x'; 64],
                },
            }],
            leader_commit: 3,
        })
        .unwrap();
    assert!(!apply_pressure.success);
    assert_eq!(
        apply_pressure.reject_reason.as_deref(),
        Some("apply_batch_backpressure")
    );

    let oversized = cluster
        .propose(Command::StringSet {
            key: "oversized-pipeline".to_string(),
            value: vec![b'z'; 4_096],
        })
        .unwrap_err();
    assert!(matches!(oversized, RaftError::LogEntryTooLarge { .. }));

    let admin = cluster.byteraft_runtime_admin_report();
    assert!(admin.append_backpressure_enforced);
    assert!(admin.apply_backpressure_enforced);
    assert!(admin.memory_replicate_bytes_enforced);
    assert!(admin.oversized_log_rejection_present);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "per_peer_replication_pipeline_state" && item.ready));
    let lagging_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .unwrap();
    assert_eq!(lagging_peer.append_queue_limit, 1);
    assert_eq!(lagging_peer.inflight_bytes_limit, 512);
    assert!(lagging_peer.append_queue_max_depth >= lagging_peer.append_queue_limit);
    assert!(lagging_peer.append_rejected > 0);
    let apply_peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 2)
        .unwrap();
    assert_eq!(apply_peer.apply_inflight_limit, 1);
    assert_eq!(apply_peer.apply_batch_bytes_limit, 16);
    assert!(apply_peer.apply_queue_max_depth >= apply_peer.apply_inflight_limit);
    assert!(apply_peer.apply_backpressure_rejections > 0);

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_queue_limit"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_inflight_bytes_limit"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_apply_inflight_limit"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_apply_queue_depth"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_apply_queue_max_depth"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_apply_batch_bytes_limit"));
}

// shared-corpus: raft_rustraft_replication_backpressure
// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_replication_backpressure
#[test]
fn append_entries_reorder_window_timeout_and_stale_term_are_reported() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            enable_reorder_queue: true,
            reorder_window_size: 1,
            reorder_timeout_us: 1,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "timeout-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    let queued = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 3,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 2,
                shard_id: 1,
                command: Command::StringSet {
                    key: "timeout-buffered-gap".to_string(),
                    value: b"queued".to_vec(),
                },
            }],
            leader_commit: 2,
        })
        .unwrap();
    assert!(!queued.success);
    assert_eq!(
        queued.reject_reason.as_deref(),
        Some("out_of_order_append_queued")
    );

    let timed_out = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 1,
            leader_id: 1,
            target_id: 3,
            prev_log_index: 8,
            prev_log_term: 1,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 9,
                shard_id: 1,
                command: Command::StringSet {
                    key: "timeout-gap".to_string(),
                    value: b"gap".to_vec(),
                },
            }],
            leader_commit: 9,
        })
        .unwrap();
    assert!(!timed_out.success);
    assert_eq!(
        timed_out.reject_reason.as_deref(),
        Some("reorder_window_timeout")
    );

    let stale = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 0,
            leader_id: 1,
            target_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
        .unwrap();
    assert!(!stale.success);
    assert_eq!(stale.reject_reason.as_deref(), Some("stale_term"));

    let admin = cluster.byteraft_runtime_admin_report();
    assert!(admin.out_of_order_append_handling_present);
    assert!(admin.reorder_timeout_drop_present);
    assert!(admin.stale_term_rejection_present);
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "reorder_queue_runtime" && item.ready));
    let peer = admin
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .unwrap();
    assert_eq!(peer.reorder_entry_timeouts, 1);
    assert_eq!(peer.reorder_dropped_packages, 1);
    assert!(peer.reorder_entries_rejected >= 2);
    assert_eq!(peer.stale_term_rejections, 1);
    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_entry_timeouts"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_dropped_packages"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_stale_term_rejections"));
}

#[test]
fn append_entries_updates_observed_leader_for_standalone_node_status() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    assert_eq!(cluster.status().leader_id, 1);

    let response = cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 2,
            leader_id: 2,
            target_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
        .unwrap();

    assert!(response.success);
    let status = cluster.status();
    assert_eq!(status.leader_id, 2);
    assert_eq!(
        status
            .nodes
            .iter()
            .find(|node| node.node_id == 2)
            .unwrap()
            .role,
        RaftRole::Leader
    );
    assert_eq!(
        status
            .nodes
            .iter()
            .find(|node| node.node_id == 1)
            .unwrap()
            .role,
        RaftRole::Follower
    );
}

#[test]
fn observer_apply_health_reports_local_replica_without_remote_false_lag() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "observer-health".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    {
        let mut inner = cluster.inner.write().expect("raft cluster lock poisoned");
        let node = inner.nodes.get_mut(&2).unwrap();
        node.applied_index = 0;
        node.applied.clear();
    }

    assert!(!cluster.apply_health(0).healthy);

    let local = cluster.observer_apply_health(1, 0);
    assert!(local.healthy);
    assert_eq!(local.fully_applied_nodes, vec![1]);
    assert!(local.slow_appliers.is_empty());

    let stale_observer = cluster.observer_apply_health(2, 0);
    assert!(!stale_observer.healthy);
    assert_eq!(stale_observer.slow_appliers[0].node_id, 2);
}

#[test]
fn request_vote_higher_term_resets_prior_vote_before_decision() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster.elect_leader(2).unwrap();
    assert_eq!(cluster.hard_state(1).unwrap().voted_for, None);

    let first_vote = cluster
        .request_vote(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: 3,
            candidate_id: 2,
            target_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(first_vote.vote_granted);
    assert_eq!(cluster.hard_state(1).unwrap().voted_for, Some(2));

    let higher_term_vote = cluster
        .request_vote(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: 4,
            candidate_id: 3,
            target_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(higher_term_vote.vote_granted);
    let hard_state = cluster.hard_state(1).unwrap();
    assert_eq!(hard_state.current_term, 4);
    assert_eq!(hard_state.voted_for, Some(3));
}

#[test]
fn request_vote_higher_term_updates_term_even_when_candidate_log_is_behind() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "vote-term".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let response = cluster
        .request_vote(VoteRequest {
            rpc: None,
            shard_id: 1,
            term: 5,
            candidate_id: 3,
            target_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(!response.vote_granted);
    assert_eq!(response.term, 5);
    assert_eq!(
        response.reject_reason.as_deref(),
        Some("candidate_log_behind")
    );
    let hard_state = cluster.hard_state(1).unwrap();
    assert_eq!(hard_state.current_term, 5);
    assert_eq!(hard_state.voted_for, None);
}

// shared-corpus: raft_rustraft_rpc_auth_deadline_transport_matrix
#[test]
fn raft_hard_state_membership_and_snapshot_transport_are_exposed() {
    let cluster = RaftCluster::new_single_shard(9, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "snap".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let hard_state = cluster.hard_state(1).unwrap();
    assert_eq!(hard_state.current_term, 1);
    assert_eq!(hard_state.commit_index, 1);
    assert_eq!(
        cluster.membership(),
        RaftMembership {
            shard_id: 9,
            voters: vec![1, 2, 3],
            leader_id: 1,
        }
    );
    let request = cluster.build_install_snapshot_request(3).unwrap();
    let response = RaftTransport::install_snapshot(&cluster, request).unwrap();
    assert!(response.success);
    assert_eq!(response.last_included_index, 1);
}

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix
#[test]
fn raft_install_snapshot_request_carries_external_snapshot_reference() {
    let cluster = RaftCluster::new_single_shard(19, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "external-snapshot".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();
    let snapshot_ref = RaftExternalSnapshotRef {
        uri: "s3://temporalstore-test/cluster-a/shards/19/snapshots/1-1-snap/manifest.json"
            .to_string(),
        checksum: "sha256:abc123".to_string(),
        byte_size: 512 * 1024 * 1024,
    };
    let request = cluster
        .build_install_snapshot_request_with_external_ref(3, Some(snapshot_ref.clone()))
        .unwrap();
    assert_eq!(request.external_snapshot_ref, Some(snapshot_ref));
    assert_eq!(
        request.snapshot.external_snapshot_ref,
        request.external_snapshot_ref
    );
    let response = cluster.receive_install_snapshot(request).unwrap();
    assert!(response.success);
    assert_eq!(response.last_included_index, 1);
}

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix
#[test]
fn snapshot_transfer_policy_streams_small_snapshots_to_peer() {
    let cluster = RaftCluster::new_single_shard(20, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "small-snapshot".to_string(),
            value: b"v".to_vec(),
        })
        .unwrap();

    let plan = cluster
        .plan_snapshot_bootstrap(3, RaftSnapshotTransferPolicy::default(), None)
        .unwrap();
    assert_eq!(plan.transfer.mode, RaftSnapshotTransferMode::PeerStreaming);
    assert_eq!(plan.last_included_index, 1);
    assert_eq!(plan.catch_up_from_index, 2);

    let request = cluster
        .build_install_snapshot_request_with_policy(3, RaftSnapshotTransferPolicy::default(), None)
        .unwrap();
    assert_eq!(request.external_snapshot_ref, None);
}

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix
#[test]
fn snapshot_transfer_policy_requires_external_ref_for_large_snapshot() {
    let cluster = RaftCluster::new_single_shard(21, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "large-snapshot".to_string(),
            value: vec![b'x'; 1024],
        })
        .unwrap();
    let policy = RaftSnapshotTransferPolicy {
        external_threshold_bytes: 1,
        allow_peer_streaming: true,
        allow_external_store: true,
    };

    let err = cluster
        .plan_snapshot_bootstrap(3, policy.clone(), None)
        .unwrap_err();
    assert!(matches!(
        err,
        RaftError::ExternalSnapshotRequired {
            snapshot_bytes: _,
            threshold_bytes: 1
        }
    ));

    let snapshot_ref = RaftExternalSnapshotRef {
        uri: "s3://temporalstore-test/cluster-a/shards/21/snapshots/1-1-large/manifest.json"
            .to_string(),
        checksum: "sha256:def456".to_string(),
        byte_size: 1024,
    };
    let plan = cluster
        .plan_snapshot_bootstrap(3, policy.clone(), Some(snapshot_ref.clone()))
        .unwrap();
    assert_eq!(plan.transfer.mode, RaftSnapshotTransferMode::ExternalStore);
    assert_eq!(
        plan.transfer.external_snapshot_ref.as_ref(),
        Some(&snapshot_ref)
    );

    let request = cluster
        .build_install_snapshot_request_with_policy(3, policy, Some(snapshot_ref.clone()))
        .unwrap();
    assert_eq!(request.external_snapshot_ref, Some(snapshot_ref));
    assert_eq!(
        request.snapshot.external_snapshot_ref,
        request.external_snapshot_ref
    );
}

#[tokio::test]
async fn leader_snapshot_upload_records_meta_and_bootstraps_replica_from_uri() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(temporalstore_snapshot::FileObjectStore::with_uri_scheme(
        tmp.path().join("objects"),
        "s3",
    ));
    let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
    let meta = SingleNodeMeta::default();
    meta.register(RegisterShardRequest {
        shard_id: 22,
        server_addr: "raft://node-1".to_string(),
    });
    let cluster = RaftCluster::new_single_shard_with_wal(
        tmp.path().join("wal"),
        22,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "before-snapshot".to_string(),
            value: b"snapshot-value".to_vec(),
        })
        .unwrap();

    let report = cluster
        .publish_leader_snapshot_and_record_meta(&snapshots, &meta)
        .await
        .unwrap();
    let recorded = meta.get(22).location.unwrap().latest_snapshot.unwrap();
    assert_eq!(recorded.uri, report.meta_ref.uri);
    assert_eq!(recorded.last_log_index, 1);
    assert_eq!(
        cluster.latest_external_snapshot_ref(),
        Some(report.raft_ref.clone())
    );

    let mut bad_ref = recorded.clone();
    bad_ref.checksum = "bad-checksum".to_string();
    let err = cluster
        .bootstrap_replica_from_external_snapshot(
            3,
            &snapshots,
            &bad_ref,
            tmp.path().join("restore-node-3-bad"),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(&err, RaftError::SnapshotStore(message) if message.contains("checksum")),
        "unexpected error: {err:?}"
    );

    cluster
        .propose(Command::StringSet {
            key: "after-snapshot".to_string(),
            value: b"log-value".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();
    let plan = cluster
        .bootstrap_replica_from_external_snapshot(
            3,
            &snapshots,
            &recorded,
            tmp.path().join("restore-node-3"),
        )
        .await
        .unwrap();

    assert_eq!(plan.transfer.mode, RaftSnapshotTransferMode::ExternalStore);
    assert_eq!(plan.last_included_index, 1);
    assert_eq!(
        cluster.read_local(
            3,
            Command::StringGet {
                key: "before-snapshot".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"snapshot-value".to_vec())
        })
    );
    assert_eq!(
        cluster.read_local(
            3,
            Command::StringGet {
                key: "after-snapshot".to_string()
            },
        ),
        Ok(CommandResponse::Bytes {
            value: Some(b"log-value".to_vec())
        })
    );
    let installed_ref = cluster
        .wal_records()
        .into_iter()
        .find(|(node_id, _)| *node_id == 3)
        .and_then(|(_, record)| record.installed_snapshot)
        .and_then(|snapshot| snapshot.external_snapshot_ref);
    assert_eq!(installed_ref, Some(report.raft_ref.clone()));
    let restored = RaftCluster::restore_single_shard_from_wal(
        tmp.path().join("wal"),
        22,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    assert_eq!(
        restored.latest_external_snapshot_ref(),
        Some(report.raft_ref)
    );
}

#[tokio::test]
async fn external_snapshot_bootstrap_rejects_stale_local_replica_before_download() {
    let tmp = tempfile::TempDir::new().unwrap();
    let store = Arc::new(temporalstore_snapshot::FileObjectStore::with_uri_scheme(
        tmp.path().join("objects"),
        "s3",
    ));
    let snapshots = S3SnapshotStore::new("cluster-a", "test", tmp.path().join("local"), store);
    let meta = SingleNodeMeta::default();
    meta.register(RegisterShardRequest {
        shard_id: 24,
        server_addr: "raft://node-1".to_string(),
    });
    let cluster = RaftCluster::new_single_shard_with_wal(
        tmp.path().join("wal"),
        24,
        [1, 2, 3],
        RaftConfig::default(),
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-a".to_string(),
            value: b"a".to_vec(),
        })
        .unwrap();
    let report = cluster
        .publish_leader_snapshot_and_record_meta(&snapshots, &meta)
        .await
        .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-b".to_string(),
            value: b"b".to_vec(),
        })
        .unwrap();
    cluster.catch_up(3).unwrap();
    assert_eq!(cluster.hard_state(3).unwrap().commit_index, 2);

    let mut stale_ref = report.meta_ref.clone();
    stale_ref.checksum = "bad-checksum-should-not-be-read".to_string();
    let err = cluster
        .bootstrap_replica_from_external_snapshot(
            3,
            &snapshots,
            &stale_ref,
            tmp.path().join("restore-stale-node-3"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err,
        RaftError::StaleSnapshot {
            snapshot_index: 1,
            local_commit_index: 2
        }
    );
}

// shared-corpus: raft_rustraft_rpc_auth_deadline_transport_matrix
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

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix raft_rustraft_snapshot_lifecycle_depth
#[test]
fn streaming_snapshot_chunks_install_only_after_all_chunks_arrive() {
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

// shared-corpus: raft_rustraft_snapshot_lifecycle_depth
// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix raft_rustraft_snapshot_lifecycle_depth
#[test]
fn rustraft_snapshot_chunk_retry_releases_backpressure_and_installs_chunk() {
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

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix raft_rustraft_snapshot_lifecycle_depth
#[test]
fn byteraft_snapshot_lifecycle_reports_timeout_rate_limit_rollback_membership_and_rejoin() {
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

    let admin = cluster.byteraft_runtime_admin_report();
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
    assert!(metrics.contains("temporalstore_raft_byteraft_snapshot_chunk_retry_present"));
    assert!(metrics.contains("temporalstore_raft_byteraft_snapshot_send_timeout_present"));
}

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix
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

// shared-corpus: raft_rustraft_snapshot_chunk_retry_rollback_matrix raft_rustraft_snapshot_lifecycle_depth
#[test]
fn raft_snapshot_chunk_transport_rejects_stale_term_before_buffering() {
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix raft_rustraft_rolling_restart_joint_consensus_fault_harness
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix raft_rustraft_rolling_restart_joint_consensus_fault_harness
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

// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_rpc_auth_deadline_transport_matrix
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

// shared-corpus: raft_rustraft_rpc_auth_deadline_transport_matrix
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

// shared-corpus: raft_rustraft_read_lease_fault_matrix raft_rustraft_packet_loss_fault_harness
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

#[derive(Debug, Clone)]
struct FlakyTransport {
    cluster: RaftCluster,
    failures_left: Arc<Mutex<usize>>,
}

impl RaftTransport for FlakyTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        let mut failures_left = self
            .failures_left
            .lock()
            .expect("flaky transport lock poisoned");
        if *failures_left > 0 {
            *failures_left -= 1;
            return Err(RaftError::Transport("injected retry".to_string()));
        }
        drop(failures_left);
        self.cluster.receive_append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        self.cluster.receive_install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        let mut failures_left = self
            .failures_left
            .lock()
            .expect("flaky transport lock poisoned");
        if *failures_left > 0 {
            *failures_left -= 1;
            return Err(RaftError::Transport("injected retry".to_string()));
        }
        drop(failures_left);
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

#[derive(Debug, Clone)]
struct OneFailedFollowerTransport {
    cluster: RaftCluster,
    failed_target: RaftNodeId,
    snapshot_attempts: Arc<Mutex<usize>>,
}

impl RaftTransport for OneFailedFollowerTransport {
    fn append_entries(
        &self,
        request: AppendEntriesRequest,
    ) -> Result<AppendEntriesResponse, RaftError> {
        if request.target_id == self.failed_target {
            return Err(RaftError::Transport("target unavailable".to_string()));
        }
        self.cluster.receive_append_entries(request)
    }

    fn request_vote(&self, request: VoteRequest) -> Result<VoteResponse, RaftError> {
        self.cluster.receive_vote_request(request)
    }

    fn install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        *self
            .snapshot_attempts
            .lock()
            .expect("snapshot attempts lock poisoned") += 1;
        self.cluster.receive_install_snapshot(request)
    }

    fn install_snapshot_chunk(
        &self,
        request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        self.cluster.receive_install_snapshot_chunk(request)
    }
}

// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_packet_loss_fault_harness
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
    assert!(readiness.rustraft_leader_write_authority_present);
    assert!(readiness.rustraft_operator_observability_present);
    assert!(readiness.rustraft_rpc_transport_contract_present);
    assert!(readiness.rustraft_log_retention_snapshot_trigger_present);
    assert!(readiness.rustraft_apply_snapshot_fence_present);
    assert!(readiness.raft_storage_apply_fence_present);
    assert!(readiness.rustraft_snapshot_floor_log_matching_present);
    assert!(readiness.rustraft_snapshot_tail_catchup_present);
    assert!(readiness.rustraft_compacted_entry_rejection_present);
    assert!(readiness.rustraft_metaserver_snapshot_floor_election_present);
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

fn ready_temporal_raft_process_node(node_id: RaftNodeId) -> TemporalRaftProcessNodeEvidence {
    TemporalRaftProcessNodeEvidence {
        node_id,
        addr: format!("127.0.0.1:{}", 19000 + node_id),
        wal_dir: format!("/tmp/temporalstore-temporal-raft-test/node-{node_id}"),
        snapshot_dir: format!("/tmp/temporalstore-temporal-raft-test/node-{node_id}/snapshot"),
        commit_index: 11,
        applied_index: 11,
        snapshot_id: Some("snapshot-11".to_string()),
        restarted: true,
        log_store_validated: true,
    }
}

fn ready_temporal_raft_operational_semantics() -> TemporalRaftProcessOperationalSemanticsEvidence {
    TemporalRaftProcessOperationalSemanticsEvidence {
        api_presence_only_rejected: true,
        process_path_validated: true,
        read_index_validated: true,
        leader_lease_validated: true,
        stale_leader_lease_rejection_observed: true,
        follower_lease_expiration_observed: true,
        lagging_follower_read_rejected: true,
        bounded_stale_read_acceptance_observed: true,
        bounded_stale_read_rejection_observed: true,
        minority_partition_read_rejection_observed: true,
        healed_follower_catchup_observed: true,
        stale_follower_write_rejected: true,
        leader_transfer_exact_once_validated: true,
        leader_transfer_under_load_validated: true,
        snapshot_bootstrap_validated: true,
        snapshot_install_restart_validated: true,
        membership_rescale_validated: true,
        membership_add_promote_remove_validated: true,
        follower_rejoin_after_compaction_validated: true,
        secondary_read_eligibility_validated: true,
        apply_pipeline_converged: true,
        wal_persistence_observed: true,
        fsm_apply_idempotent_replay_observed: true,
        storage_mutation_wal_fence_atomicity_observed: true,
        snapshot_install_apply_fence_atomicity_observed: true,
        process_restart_after_apply_crash_recovered: true,
        ready: true,
        blockers: Vec::new(),
    }
}

fn ready_data_node_temporal_raft_rollout_report() -> TemporalRaftDataNodeProcessRolloutReport {
    TemporalRaftDataNodeProcessRolloutReport {
        shard_id: 7,
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        nodes: vec![
            ready_temporal_raft_process_node(1),
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
        ],
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 6,
        read_index_responses_observed: 2,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        write_proposed_through_process_api: true,
        leader_transfer_validated: true,
        failover_validated: true,
        membership_change_validated: true,
        secondary_lag_observed: true,
        lagging_follower_read_rejection_observed: true,
        stale_follower_write_rejection_observed: true,
        catchup_read_eligibility_observed: true,
        minority_partition_rejection_observed: true,
        bounded_stale_read_eligibility_observed: true,
        healed_follower_catchup_observed: true,
        lagging_follower_observed_lag: 1,
        follower_lag_validated: true,
        secondary_read_validated: true,
        recovered_after_restart: true,
        restart_recovery_validated: true,
        snapshot_install_validated: true,
        applied_fence_validated: true,
        crash_after_storage_mutation_recovered: true,
        crash_after_wal_persist_recovered: true,
        crash_during_snapshot_install_recovered: true,
        apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_temporal_raft_operational_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
}

fn ready_meta_temporal_raft_rollout_report() -> TemporalRaftMetaProcessRolloutReport {
    TemporalRaftMetaProcessRolloutReport {
        voters: vec![1, 2, 3],
        learners: Vec::new(),
        nodes: vec![
            ready_temporal_raft_process_node(1),
            ready_temporal_raft_process_node(2),
            ready_temporal_raft_process_node(3),
        ],
        spawned_process_count: 3,
        independent_wal_dirs: true,
        independent_snapshot_dirs: true,
        observed_process_requests: 6,
        read_index_responses_observed: 2,
        restarted_node_count: 3,
        per_node_log_store_inspection_count: 3,
        mutation_proposed_through_process_api: true,
        applied_raft_mutations: 4,
        generated_scheduler_tasks: 2,
        scheduler_retries: 1,
        stale_scheduler_token_rejected: true,
        data_node_membership_results_ready: true,
        scheduler_mutations_proposed_through_process_api: true,
        scheduler_task_replay_from_raft_log_observed: true,
        membership_mutations_proposed_through_process_api: true,
        data_node_membership_workflow_report_attached: true,
        data_node_raft_group_results_observed: true,
        failover_validated: true,
        membership_change_validated: true,
        follower_lag_validated: true,
        secondary_read_validated: true,
        read_index_validated: true,
        snapshot_install_validated: true,
        recovered_after_restart: true,
        scheduler_task_replay_validated: true,
        crash_after_meta_mutation_recovered: true,
        crash_after_meta_wal_persist_recovered: true,
        crash_during_meta_snapshot_install_recovered: true,
        meta_apply_fence_recovered_after_restart: true,
        multi_process_log_store_validated: true,
        operational_semantics: ready_temporal_raft_operational_semantics(),
        ready: true,
        blockers: Vec::new(),
    }
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

fn wait_for_replica_value(
    runtime: &ProductionRaftRuntime,
    node_id: u64,
    key: &str,
    expected: &[u8],
) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let response = runtime
            .cluster()
            .read_from_replica(
                node_id,
                Command::StringGet {
                    key: key.to_string(),
                },
            )
            .unwrap();
        if response
            == (CommandResponse::Bytes {
                value: Some(expected.to_vec()),
            })
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "replica {node_id} did not catch up; last response: {response:?}"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn free_local_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().to_string()
}

fn topology_for_shard(
    shard_id: ShardId,
    primary: &str,
    replicas: impl IntoIterator<Item = &'static str>,
) -> TableTopologyResponse {
    TableTopologyResponse {
        status: Status::ok(),
        table: Some(TableMetaInfo {
            table_id: 1,
            namespace: "ns".to_string(),
            table_name: "table".to_string(),
            state: MetaEntityState::Normal,
            topology_version: 1,
            first_shard_id: shard_id,
            shard_count: 1,
            replica_count: 3,
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        }),
        partitions: vec![TablePartition {
            shard_id,
            start_slot: 0,
            end_slot: u64::MAX,
            primary: Some(primary.to_string()),
            replicas: replicas
                .into_iter()
                .map(|replica| replica.to_string())
                .collect(),
            primary_endpoint: None,
            replica_endpoints: Vec::new(),
        }],
        unchanged: false,
    }
}

fn server_meta(addr: &str, node_id: u64, state: MetaEntityState) -> ServerMetaInfo {
    ServerMetaInfo {
        server_addr: addr.to_string(),
        node_id,
        location: "zone-a".to_string(),
        state,
        last_heartbeat_ms: 1,
        frozen_since_ms: 0,
        freeze_cooldown_until_ms: 0,
        boot_time_ms: 1,
        binary_version: "test".to_string(),
        shard_loads: Vec::new(),
        partition_loads: Vec::new(),
        runtime_load: crate::meta::ServerRuntimeLoad::default(),
        shard_states: Vec::new(),
    }
}

fn long_sequence_rows(count: usize) -> Vec<SequenceFeatureRow> {
    (0..count)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: 1_700_000_000_000 + offset as u64,
            gid: offset as u64,
            action_type: (offset % 8) as u32,
            duration: (offset % 600) as u32,
            author_id: 42_000_000 + offset as u64,
        })
        .collect()
}

fn large_feature_points() -> Vec<FeaturePoint> {
    (0..10)
        .map(|offset| FeaturePoint {
            timestamp_ms: 1_000 + offset,
            value: vec![b'a' + offset as u8; 10 * 1024],
        })
        .collect()
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("raft http server {addr} did not start");
}

// shared-corpus: raft_rustraft_read_lease_fault_matrix
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

// shared-corpus: raft_rustraft_election_controls
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

// shared-corpus: raft_rustraft_election_controls
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
    let admin = cluster.byteraft_runtime_admin_report();
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

// shared-corpus: raft_rustraft_election_controls
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

    let admin = cluster.byteraft_runtime_admin_report();
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

// shared-corpus: raft_rustraft_read_safety_policy
// shared-corpus: raft_rustraft_read_lease_fault_matrix
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

// shared-corpus: raft_rustraft_read_lease_fault_matrix
// shared-corpus: raft_rustraft_packet_loss_fault_harness
#[test]
fn byteraft_read_safety_fault_matrix_records_partition_and_catchup_evidence() {
    let cluster = RaftCluster::new_single_shard_with_config(
        1,
        [1, 2, 3],
        RaftConfig {
            lease_duration_ms: 10,
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
        Err(RaftError::LeaderUnavailable)
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

    let admin = cluster.byteraft_runtime_admin_report();
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
    assert!(admin
        .capability_matrix
        .iter()
        .any(|item| item.capability == "lease_read_index_pre_vote_semantics" && item.ready));
}

#[test]
fn raft_config_matches_cpp_defaults_and_validates_required_limits() {
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

// shared-corpus: raft_rustraft_pipeline_reorder_backpressure_matrix raft_rustraft_election_controls
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
fn data_raft_read_policy_matches_cpp_partition_manager_modes() {
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

// shared-corpus: raft_rustraft_read_safety_policy
// shared-corpus: raft_rustraft_read_lease_fault_matrix
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
fn raft_wait_for_applied_index_matches_cpp_backend_contract() {
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
        "temporalstore_raft_node_apply_lag{kind=\"data\",node_id=\"2\",role=\"follower\"} 1"
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
#[test]
fn learner_and_witness_roles_match_cpp_membership_shape() {
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
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

// shared-corpus: raft_rustraft_membership_roles_joint_consensus_matrix
#[test]
fn byteraft_admin_reports_witness_auto_promote_and_pending_joint_consensus() {
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

    let admin = cluster.byteraft_runtime_admin_report();
    assert!(admin.witness_membership_present);
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
    let local = cluster.byteraft_local_status_report();
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
        "temporalstore_raft_byteraft_local_wal_first_log_index",
        "temporalstore_raft_byteraft_local_wal_last_log_index",
        "temporalstore_raft_byteraft_local_peer_match_index",
        "temporalstore_raft_byteraft_local_peer_next_index",
        "temporalstore_raft_byteraft_local_peer_snapshot_sending",
        "temporalstore_raft_byteraft_local_peer_snapshot_installing",
        "temporalstore_raft_byteraft_local_peer_snapshot_installed_index",
        "temporalstore_raft_byteraft_local_peer_transfer_leader_target",
        "temporalstore_raft_byteraft_local_peer_pre_vote_rejections",
        "temporalstore_raft_byteraft_local_peer_election_rejections",
        "temporalstore_raft_byteraft_learner_add_present",
        "temporalstore_raft_byteraft_learner_catchup_present",
        "temporalstore_raft_byteraft_learner_promote_present",
        "temporalstore_raft_byteraft_voter_remove_present",
        "temporalstore_raft_byteraft_leader_transfer_exact_once_present",
        "temporalstore_raft_byteraft_pending_joint_consensus_restart_present",
        "temporalstore_raft_byteraft_membership_learner_add_count",
        "temporalstore_raft_byteraft_membership_voter_remove_count",
        "temporalstore_raft_byteraft_membership_leader_transfer_exact_once_commit_count",
    ] {
        assert!(
            metrics.contains(metric),
            "missing local-status metric {metric}"
        );
    }
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

// shared-corpus: raft_rustraft_follower_rejoin_compacted_logs_fault_harness
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

// shared-corpus: raft_rustraft_follower_rejoin_compacted_logs_fault_harness
#[test]
fn rustraft_follower_rejoin_after_compaction_installs_snapshot_and_replays_tail() {
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

// shared-corpus: raft_rustraft_leader_transfer_high_write_fault_harness
#[test]
fn rustraft_leader_transfer_under_high_write_load_commits_once() {
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

    let admin = cluster.byteraft_runtime_admin_report();
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

// shared-corpus: raft_byteraft_admin_status_surface
#[test]
fn byteraft_admin_status_surface_requires_wal_and_peer_pipeline_fields() {
    let local_fixture = RaftCluster::new_single_shard(1, [1, 2, 3]);
    local_fixture
        .propose(Command::StringSet {
            key: "admin-without-wal".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let local_admin = local_fixture.byteraft_runtime_admin_report();
    assert!(!local_admin.admin_status_surface_complete);
    assert!(!local_admin.wal_segment_lifecycle_present);
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
    let durable_admin = durable.byteraft_runtime_admin_report();
    assert!(durable_admin.admin_status_surface_complete);
    assert!(durable_admin.wal_segment_lifecycle_present);
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
fn metaserver_raft_replays_scheduler_state_and_cpp_partition_set_topology() {
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
    let topology = CppPartitionSetTopology::from_replicas(
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
            use_cpp_partition_ids: false,
            partition_version: 0,
            serving_options: crate::meta::TableServingOptions::default(),
        })
        .status
        .ok
    );
    assert!(
        meta.register(RegisterShardRequest {
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
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(topology.status.ok);
    assert_eq!(topology.table.unwrap().shard_count, 2);
    assert_eq!(topology.partitions.len(), 2);
    assert_eq!(topology.partitions[0].primary.as_deref(), Some("server-a"));

    let updated = meta.update_table(UpdateTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        shard_count: Some(3),
        replica_count: Some(2),
        first_shard_id: None,
        use_cpp_partition_ids: None,
        partition_version: None,
        serving_options: None,
    });
    assert!(updated.status.ok, "{updated:?}");
    for node_id in [10, 11, 12] {
        assert_eq!(meta.commit_index(node_id).unwrap(), 5);
    }
    let updated_topology = meta.get_table_topology(GetTableTopologyRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        old_topology_version: 0,
    });
    assert!(updated_topology.status.ok, "{updated_topology:?}");
    let updated_table = updated_topology.table.unwrap();
    assert_eq!(updated_table.shard_count, 3);
    assert_eq!(updated_table.replica_count, 2);
    assert_eq!(updated_topology.partitions.len(), 3);

    let duplicate = meta.add_table(AddTableRequest {
        namespace: "feature".to_string(),
        table_name: "user_seq".to_string(),
        first_shard_id: 100,
        shard_count: 2,
        replica_count: 1,
        use_cpp_partition_ids: false,
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
fn production_meta_raft_runtime_matches_cpp_multinode_control_and_fault_contract() {
    let runtime = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
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

#[test]
fn metaserver_raft_can_read_from_any_live_committed_replica() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
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
        shard_id: 2,
        server_addr: "server-b".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    meta.add_node(13).unwrap();
    assert_eq!(
        meta.get_shard_location(13, 2).unwrap(),
        Some(ShardLocation {
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
        shard_id: 43,
        server_addr: "server-after-meta-failover".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    assert_eq!(
        meta.get_shard_location(failover.new_leader_id, 43).unwrap(),
        Some(ShardLocation {
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
        "temporalstore_raft_node_apply_lag{kind=\"meta\",node_id=\"11\",role=\"follower\"} 1"
    ));

    meta.catch_up(11).unwrap();
    assert!(meta.apply_health(0).healthy);
}

#[test]
fn metaserver_raft_membership_plan_and_apply_match_data_raft_shape() {
    let meta = MetaRaftCluster::new([10, 11, 12]);
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
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
        shard_id: 7,
        server_addr: "server-before-failover".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();

    meta.set_alive(10, false).unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
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
            shard_id: 7,
            server_addr: "server-before-failover".to_string(),
            latest_snapshot: None,
        })
    );
    assert_eq!(
        meta.get_shard_location(status.leader_id, 8).unwrap(),
        Some(ShardLocation {
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
            shard_id: 9,
            server_addr: "server-snapshot".to_string(),
            latest_snapshot: None,
        })
    );
    assert_eq!(meta.commit_index(12).unwrap(), 1);
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
        shard_id: 1,
        server_addr: "server-a".to_string(),
        latest_snapshot: None,
    }))
    .unwrap();
    let snapshot = meta.create_snapshot().unwrap();
    meta.propose(MetaCommand::PutShardLocation(ShardLocation {
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
    engine.block_store().roll_segment().unwrap();
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
    assert_eq!(recovery.oplog_records, 2);
    assert_eq!(recovery.index_log_records, 2);
    assert!(recovery.index_bytes > 0);
    assert!(recovery.index_write_atomic);
    assert!(recovery.active_page_segment_ids.len() >= 2);
    assert!(recovery.total_page_refs >= 2);
    assert_eq!(recovery.readable_page_refs, recovery.total_page_refs);
    assert!(recovery.all_live_pages_readable);
    assert!(recovery.segment_integrity.integrity_ok);
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

// shared-corpus: raft_rustraft_wal_log_codec_segment_lifecycle
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
    let admin = restored.byteraft_runtime_admin_report();
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
