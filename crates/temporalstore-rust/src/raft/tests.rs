use super::*;
use crate::control::{Config, SetConfigRequest};
use crate::http::{json_response, parse_json, post_json_with_options, serve, HttpRequestOptions};
use crate::meta::{TableMetaInfo, TablePartition};
use crate::rebalance::{
    CppPartitionSetTopology, DeterministicTaskScheduler, NetworkSchedulerTaskExecution,
    RebalanceStep, SchedulerTaskKind, SchedulerTaskResult, ShardReplica, ShardReplicaState,
    ShardRole, TaskSchedulerOptions,
};
use crate::types::{Command, FeatureFilter, FeatureFilterOp, FeaturePoint, SequenceFeatureRow};
use std::time::{Duration, Instant};

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

#[cfg(feature = "openraft-engine")]
#[test]
fn openraft_data_node_backend_persists_log_snapshot_read_index_and_leader_transfer() {
    use super::openraft_integration::OpenRaftConsensusBackend;

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
    let mut backend = OpenRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    assert!(backend.is_leader());

    let command = Command::StringSet {
        key: "openraft-k".to_string(),
        value: b"openraft-v".to_vec(),
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
            key: "openraft-k".to_string(),
        },
    });
    assert_eq!(
        read.response,
        CommandResponse::Bytes {
            value: Some(b"openraft-v".to_vec())
        }
    );

    let snapshot_index = backend.trigger_snapshot().unwrap();
    assert_eq!(snapshot_index, index);
    let meta = backend.build_openraft_snapshot_meta();
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
    let restored = OpenRaftConsensusBackend::new_data_node(options, engine);
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

#[cfg(feature = "openraft-engine")]
#[test]
#[should_panic(expected = "openraft storage fence checksum mismatch")]
fn openraft_data_node_backend_rejects_corrupt_storage_apply_fence_on_restart() {
    use super::openraft_integration::OpenRaftConsensusBackend;

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
    let mut backend = OpenRaftConsensusBackend::new_data_node(options.clone(), engine.clone());
    backend.start().unwrap();
    let encoded = serialize_data_raft_log(&DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        oplog_sequence: 1,
        command: Command::StringSet {
            key: "openraft-corrupt-fence".to_string(),
            value: b"value".to_vec(),
        },
    })
    .unwrap();
    backend.propose(encoded).unwrap();
    backend.trigger_snapshot().unwrap();

    let path = dir.path().join("openraft-7-1.json");
    let bytes = std::fs::read(&path).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["storage_apply_fence"]["checksum"] = serde_json::Value::String("corrupt".to_string());
    std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

    let _ = OpenRaftConsensusBackend::new_data_node(options, engine);
}

#[cfg(feature = "openraft-engine")]
#[test]
fn openraft_data_node_backend_bootstraps_learner_and_auto_promotes_peer() {
    use super::openraft_integration::OpenRaftConsensusBackend;

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
    let mut backend = OpenRaftConsensusBackend::new_data_node(options.clone(), engine);
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

    let restored = OpenRaftConsensusBackend::new_data_node(options, TemporalEngine::default());
    let restored_status = restored.status().unwrap();
    assert!(restored_status.learner);
    assert_eq!(restored_status.voter_count, 3);
    assert_eq!(restored_status.learner_count, 1);
}

#[cfg(feature = "openraft-engine")]
#[test]
fn openraft_metaserver_backend_supports_membership_and_bounded_reads() {
    use super::openraft_integration::OpenRaftConsensusBackend;

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
    let mut backend = OpenRaftConsensusBackend::new_metaserver(options.clone());
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

    let membership = backend.openraft_membership();
    let voters = membership.voter_ids().collect::<Vec<_>>();
    assert_eq!(voters, vec![10, 11, 13]);

    backend.trigger_snapshot().unwrap();
    let snapshot_meta = backend.build_openraft_snapshot_meta();
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

    let restored = OpenRaftConsensusBackend::new_metaserver(options);
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
    assert_eq!(engine.page_store().stats().writes, 1);
    assert_eq!(engine.oplog_store().stats(7).writes, 1);
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
    assert!(matches!(
        cluster.build_install_snapshot_chunks(3, 2),
        Err(RaftError::SnapshotBackpressure { node_id: 3 })
    ));
    let rejected = cluster.byteraft_runtime_admin_report();
    let peer3 = rejected
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert_eq!(peer3.snapshot_backpressure_rejections, 1);

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
    let in_progress = cluster.byteraft_runtime_admin_report();
    let peer3 = in_progress
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert!(peer3.snapshot_installing);
    assert_eq!(peer3.snapshot_install_received_chunks, 2);
    assert_eq!(peer3.snapshot_install_total_chunks, 3);
    assert_eq!(peer3.snapshot_install_started, 1);
    assert_eq!(peer3.snapshot_install_completed, 0);

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
    let complete = cluster.byteraft_runtime_admin_report();
    let peer3 = complete
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert!(!peer3.snapshot_installing);
    assert_eq!(peer3.snapshot_install_received_chunks, 3);
    assert_eq!(peer3.snapshot_install_total_chunks, 3);
    assert_eq!(peer3.snapshot_send_attempts, 3);
    assert_eq!(peer3.snapshot_send_completed, 1);
    assert_eq!(peer3.snapshot_send_failed, 1);
    assert_eq!(peer3.snapshot_install_started, 1);
    assert_eq!(peer3.snapshot_install_completed, 1);
    assert_eq!(peer3.snapshot_install_rejected, 0);
    assert_eq!(peer3.snapshot_install_rolled_back, 0);
}

// shared-corpus: raft_byteraft_snapshot_lifecycle_depth
#[test]
fn raft_process_snapshot_sender_completion_clears_in_progress_state() {
    let cluster = RaftCluster::new_single_shard(213, [1, 2, 3]);
    cluster
        .propose(Command::StringSet {
            key: "snapshot-sender-finish".to_string(),
            value: b"value".to_vec(),
        })
        .unwrap();
    let _request = cluster.build_install_snapshot_request(3).unwrap();
    let sending = cluster.byteraft_runtime_admin_report();
    let peer3 = sending
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert!(peer3.snapshot_sending);
    assert!(peer3.snapshot_installing);

    cluster.finish_snapshot_send(3, false).unwrap();
    let failed = cluster.byteraft_runtime_admin_report();
    let peer3 = failed
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert!(!peer3.snapshot_sending);
    assert!(!peer3.snapshot_installing);
    assert_eq!(peer3.snapshot_send_failed, 1);

    let _request = cluster.build_install_snapshot_request(3).unwrap();
    cluster.finish_snapshot_send(3, true).unwrap();
    let completed = cluster.byteraft_runtime_admin_report();
    let peer3 = completed
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert!(!peer3.snapshot_sending);
    assert!(!peer3.snapshot_installing);
    assert_eq!(peer3.snapshot_send_completed, 1);
}

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
    let report = cluster.byteraft_runtime_admin_report();
    let peer3 = report
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert_eq!(peer3.snapshot_install_rejected, 1);
    assert_eq!(peer3.snapshot_send_failed, 1);
}

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
    let report = cluster.byteraft_runtime_admin_report();
    let peer3 = report
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline state");
    assert_eq!(peer3.snapshot_install_rejected, 1);
    assert_eq!(peer3.snapshot_send_failed, 1);

    chunks[0].term = 2;
    let response = cluster
        .receive_install_snapshot_chunk(chunks[0].clone())
        .unwrap();
    assert!(response.success);
    assert!(!response.snapshot_complete);
    assert_eq!(response.received_chunks, 1);
}

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
    assert_eq!(readiness.complete, cfg!(feature = "openraft-engine"));
    assert_eq!(
        readiness.production_ready,
        cfg!(feature = "openraft-engine")
    );
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.local_model_tested);
    assert!(readiness.transport_contracts_present);
    assert!(readiness.rpc_runtime_observability_present);
    assert!(readiness.external_snapshot_refs_present);
    assert_eq!(
        readiness.openraft_engine_adapter_present,
        cfg!(feature = "openraft-engine")
    );
    assert!(readiness.openraft_data_node_process_startup_present);
    assert!(readiness.openraft_metaserver_process_startup_present);
    assert!(readiness.byteraft_leader_write_authority_present);
    assert!(readiness.byteraft_operator_observability_present);
    assert!(readiness.byteraft_rpc_transport_contract_present);
    assert!(readiness.byteraft_log_retention_snapshot_trigger_present);
    assert!(readiness.byteraft_apply_snapshot_fence_present);
    assert!(readiness.byteraft_per_peer_pipeline_state_present);
    assert!(readiness.byteraft_reorder_queue_state_present);
    assert!(readiness.byteraft_snapshot_sender_downloader_lifecycle_present);
    assert!(readiness.byteraft_wal_segment_lifecycle_present);
    assert!(readiness.byteraft_read_index_lease_semantics_present);
    assert!(readiness.byteraft_admin_status_surface_present);
    assert!(readiness.raft_storage_apply_fence_present);
    assert!(readiness.byteraft_snapshot_floor_log_matching_present);
    assert!(readiness.byteraft_snapshot_tail_catchup_present);
    assert!(readiness.byteraft_compacted_entry_rejection_present);
    assert!(readiness.byteraft_metaserver_snapshot_floor_election_present);
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
}

// shared-corpus: raft_byteraft_metrics_admin_pipeline_status server_raft_byteraft_runtime_admin_route
#[test]
fn byteraft_runtime_admin_report_exposes_process_pipeline_snapshot_wal_and_read_safety() {
    let dir = tempfile::tempdir().unwrap();
    let config = RaftConfig {
        enable_pre_vote: true,
        lease_duration_ms: 1_000,
        max_inflights_replicate: 2,
        max_segment_bytes: 512,
        min_keep_segment_num: 1,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_wal(dir.path(), 91, [1, 2, 3], config.clone())
        .expect("wal-backed cluster");
    cluster
        .propose(Command::StringSet {
            key: "byteraft-admin-snapshot".to_string(),
            value: b"seed".to_vec(),
        })
        .unwrap();
    cluster.maybe_trigger_snapshot().unwrap();
    let snapshot = cluster.build_install_snapshot_request(2).unwrap();
    cluster.receive_install_snapshot(snapshot).unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "byteraft-admin-lag".to_string(),
            value: b"lag".to_vec(),
        })
        .unwrap();
    let append_request = cluster.build_append_entries_request(3).unwrap();
    assert!(matches!(
        cluster.build_append_entries_request(3),
        Err(RaftError::AppendBackpressure { node_id: 3, .. })
    ));
    cluster.receive_append_entries(append_request).unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "byteraft-admin-lag-2".to_string(),
            value: b"lag-2".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();

    assert!(matches!(
        cluster.check_write_authority(3),
        Err(RaftError::NotLeader { node_id: 3 })
    ));
    assert!(matches!(
        cluster.read_index(3),
        Err(RaftError::ReplicaLagging { replica_id: 3, .. })
    ));
    assert!(matches!(
        cluster.check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 0,
                ..DataRaftReadPolicy::default()
            },
        ),
        Err(RaftError::ReplicaLagging { replica_id: 3, .. })
    ));
    cluster
        .check_data_raft_read_policy(
            3,
            DataRaftReadPolicy {
                mode: DataRaftReadMode::BoundedStale,
                bounded_stale_max_index_lag: 1,
                ..DataRaftReadPolicy::default()
            },
        )
        .unwrap();
    cluster.advance_time_ms(1_001);
    assert!(matches!(
        cluster.check_read(
            1,
            RaftReadOptions {
                strategy: RaftReadStrategy::LeaseRead,
                ..RaftReadOptions::default()
            },
        ),
        Err(RaftError::LeaderUnavailable)
    ));
    cluster.tick_election().unwrap();
    cluster.set_alive(2, false).unwrap();
    cluster.set_alive(3, false).unwrap();
    assert!(matches!(
        cluster.read_index(1),
        Err(RaftError::LeaderUnavailable)
    ));
    assert!(matches!(
        cluster.check_write_authority(1),
        Err(RaftError::NotLeader { node_id: 1 })
    ));
    cluster.set_alive(2, true).unwrap();
    cluster.set_alive(3, true).unwrap();
    cluster.tick_election().unwrap();
    let catchup = cluster.build_append_entries_request(3).unwrap();
    cluster.receive_append_entries(catchup).unwrap();
    cluster.read_index(3).unwrap();
    cluster
        .check_read(
            1,
            RaftReadOptions {
                strategy: RaftReadStrategy::LeaseRead,
                ..RaftReadOptions::default()
            },
        )
        .unwrap();
    assert!(matches!(
        cluster.propose(Command::StringSet {
            key: "byteraft-admin-oversized".to_string(),
            value: vec![b'x'; 64 * 1024],
        }),
        Err(RaftError::LogEntryTooLarge { .. })
    ));
    let out_of_order = AppendEntriesRequest {
        rpc: None,
        shard_id: 91,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: 999,
        prev_log_term: 1,
        entries: Vec::new(),
        leader_commit: cluster.commit_index(1).unwrap(),
    };
    let out_of_order_response = cluster.receive_append_entries(out_of_order).unwrap();
    assert!(!out_of_order_response.success);
    assert_eq!(
        out_of_order_response.reject_reason.as_deref(),
        Some("log_mismatch")
    );
    let apply_backpressure = AppendEntriesRequest {
        rpc: None,
        shard_id: 91,
        term: 1,
        leader_id: 1,
        target_id: 3,
        prev_log_index: cluster.commit_index(3).unwrap(),
        prev_log_term: 1,
        entries: vec![RaftLogEntry {
            term: 1,
            index: cluster.commit_index(3).unwrap() + 1,
            shard_id: 91,
            command: Command::StringSet {
                key: "byteraft-admin-apply-backpressure".to_string(),
                value: vec![b'y'; 96 * 1024],
            },
        }],
        leader_commit: cluster.commit_index(3).unwrap() + 1,
    };
    let apply_backpressure_response = cluster.receive_append_entries(apply_backpressure).unwrap();
    assert!(!apply_backpressure_response.success);
    assert_eq!(
        apply_backpressure_response.reject_reason.as_deref(),
        Some("apply_batch_backpressure")
    );
    cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();
    cluster.set_alive(3, false).unwrap();
    cluster
        .propose(Command::StringSet {
            key: "byteraft-admin-joint-snapshot-lag".to_string(),
            value: b"joint-lag".to_vec(),
        })
        .unwrap();
    cluster.set_alive(3, true).unwrap();
    let mut snapshot_chunks = cluster.build_install_snapshot_chunks(3, 1).unwrap();
    assert!(!snapshot_chunks.is_empty());
    let first_chunk = cluster
        .receive_install_snapshot_chunk(snapshot_chunks[0].clone())
        .unwrap();
    assert!(first_chunk.success);
    if snapshot_chunks.len() > 1 {
        let duplicate = cluster
            .receive_install_snapshot_chunk(snapshot_chunks[0].clone())
            .unwrap();
        assert!(duplicate.success);
        snapshot_chunks[1].last_included_index += 1;
        assert!(matches!(
            cluster.receive_install_snapshot_chunk(snapshot_chunks[1].clone()),
            Err(RaftError::InvalidSnapshotChunk(_))
        ));
    }
    cluster.abort_joint_consensus().unwrap();

    let report = cluster.byteraft_runtime_admin_report();
    assert!(report.ready, "{:?}", report.blockers);
    assert!(report.read_index_validated);
    assert!(report.lease_read_validated);
    assert!(report.stale_follower_read_rejected);
    assert!(report.stale_follower_write_rejected);
    assert!(report.stale_leader_lease_rejected);
    assert!(report.lagging_follower_read_rejected);
    assert!(report.bounded_stale_read_accepted);
    assert!(report.bounded_stale_read_rejected);
    assert!(report.minority_partition_rejected_reads);
    assert!(report.minority_partition_rejected_writes);
    assert!(report.healed_follower_caught_up);
    assert!(report.append_backpressure_enforced);
    assert!(report.apply_backpressure_enforced);
    assert!(report.memory_replicate_bytes_enforced);
    assert!(report.oversized_log_rejection_present);
    assert!(report.out_of_order_append_handling_present);
    assert!(report.reorder_queue_enabled);
    assert!(report.snapshot_sender_lifecycle_present);
    assert!(report.snapshot_downloader_lifecycle_present);
    assert!(report.snapshot_retry_backpressure_present);
    assert!(report.snapshot_rate_limit_present);
    assert!(report.snapshot_install_progress_present);
    assert!(report.snapshot_install_rollback_present);
    assert!(report.snapshot_membership_change_present);
    assert!(report.snapshot_rejoin_after_compacted_log_present);
    assert!(report.wal_segment_lifecycle_present);
    assert!(report.pre_vote_enforced);
    assert!(report.election_controls_enforced);
    assert!(report.read_index_requests >= 5);
    assert!(report.read_index_accepted >= 2);
    assert!(report.read_index_rejected >= 3);
    assert_eq!(report.lease_read_requests, 2);
    assert_eq!(report.lease_read_accepted, 1);
    assert_eq!(report.lease_read_rejected, 1);
    assert!(report.admin_status_surface_complete);
    assert!(report.wal_segment_count >= 1);
    assert!(report.wal_total_bytes > 0);
    assert!(report.wal_active_segment_bytes > 0);
    assert!(report.wal_total_records > 0);
    assert!(report.wal_first_sequence > 0);
    assert!(report.wal_last_sequence >= report.wal_first_sequence);
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 3
            && peer.append_requests >= 2
            && peer.append_accepted >= 1
            && peer.append_rejected >= 1));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.append_queue_depth > 0 || peer.reorder_entries_released > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.append_queue_max_depth > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.apply_backpressure_rejections > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.memory_backpressure_rejections > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.oversized_log_rejections > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.out_of_order_append_rejections > 0));
    assert!(report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 2 && peer.snapshot_installed_index > 0));
    assert!(report.peer_pipeline_states.iter().any(|peer| {
        peer.peer_id == 2
            && peer.snapshot_send_attempts > 0
            && peer.snapshot_send_completed > 0
            && peer.snapshot_install_started > 0
            && peer.snapshot_install_completed > 0
            && peer.snapshot_install_received_chunks == 1
            && peer.snapshot_install_total_chunks == 1
    }));
    assert!(report.peer_pipeline_states.iter().any(|peer| {
        peer.peer_id == 3
            && peer.snapshot_install_progress_per_mille > 0
            && peer.snapshot_chunk_retry_count > 0
            && peer.snapshot_rate_limit_rejections > 0
            && peer.snapshot_install_rolled_back > 0
            && peer.snapshot_during_membership_change
            && peer.snapshot_rejoin_after_compacted_log
    }));

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_ready{kind=\"data\"} 1"));
    assert!(metrics.contains("temporalstore_raft_byteraft_read_index_validated"));
    assert!(metrics.contains("temporalstore_raft_byteraft_read_index_requests"));
    assert!(metrics.contains("temporalstore_raft_byteraft_read_index_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_read_index_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_lease_read_validated"));
    assert!(metrics.contains("temporalstore_raft_byteraft_lease_read_requests"));
    assert!(metrics.contains("temporalstore_raft_byteraft_lease_read_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_lease_read_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_pre_vote_requests"));
    assert!(metrics.contains("temporalstore_raft_byteraft_pre_vote_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_pre_vote_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_stale_follower_write_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_requests"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_queue_depth"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_append_queue_max_depth"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_apply_backpressure_rejections"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_memory_backpressure_rejections"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_oversized_log_rejections"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_out_of_order_append_rejections"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_queue_depth"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_entries_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_entries_released"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_reorder_entries_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_installed_index"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_send_attempts"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_send_completed"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_send_failed"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_started"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_completed"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_rolled_back"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_received_chunks"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_total_chunks"));
    assert!(
        metrics.contains("temporalstore_raft_byteraft_peer_snapshot_install_progress_per_mille")
    );
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_chunk_retry_count"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_rate_limit_rejections"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_during_membership_change"));
    assert!(
        metrics.contains("temporalstore_raft_byteraft_peer_snapshot_rejoin_after_compacted_log")
    );
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_segment_count"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_requests"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_accepted"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_rejected"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_completed"));
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_total_bytes"));
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_active_segment_bytes"));
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_total_records"));
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_first_sequence"));
    assert!(metrics.contains("temporalstore_raft_byteraft_wal_last_sequence"));

    let restored =
        RaftCluster::restore_single_shard_from_wal(dir.path(), 91, [1, 2, 3], config).unwrap();
    let restored_report = restored.byteraft_runtime_admin_report();
    assert!(restored_report.ready, "{:?}", restored_report.blockers);
    assert_eq!(restored_report.wal_total_records, report.wal_total_records);
    assert_eq!(restored_report.wal_last_sequence, report.wal_last_sequence);
    assert!(restored_report.read_index_requests >= 5);
    assert!(restored_report.read_index_rejected >= 3);
    assert_eq!(restored_report.lease_read_requests, 2);
    assert_eq!(restored_report.lease_read_accepted, 1);
    assert_eq!(restored_report.lease_read_rejected, 1);
    assert!(restored_report.stale_leader_lease_rejected);
    assert!(restored_report.bounded_stale_read_accepted);
    assert!(restored_report.bounded_stale_read_rejected);
    assert!(restored_report.minority_partition_rejected_reads);
    assert!(restored_report.minority_partition_rejected_writes);
    assert!(restored_report.healed_follower_caught_up);
    assert!(restored_report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 3
            && peer.append_requests >= 2
            && peer.append_accepted >= 1
            && peer.append_rejected >= 1));
    assert!(restored_report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 2 && peer.snapshot_installed_index > 0));
    assert!(restored_report
        .peer_pipeline_states
        .iter()
        .any(|peer| peer.peer_id == 2
            && peer.snapshot_send_attempts > 0
            && peer.snapshot_send_completed > 0
            && peer.snapshot_install_completed > 0));
}

// shared-corpus: raft_byteraft_replication_backpressure raft_byteraft_metrics_admin_pipeline_status
#[test]
fn raft_reorder_queue_rejects_batches_beyond_window_and_reports_counters() {
    let config = RaftConfig {
        reorder_window_size: 1,
        ..RaftConfig::default()
    };
    let cluster = RaftCluster::new_single_shard_with_config(214, [1, 2, 3], config).unwrap();
    cluster.set_alive(3, false).unwrap();
    for index in 0..2 {
        cluster
            .propose(Command::StringSet {
                key: format!("reorder-window-{index}"),
                value: vec![index as u8],
            })
            .unwrap();
    }
    cluster.set_alive(3, true).unwrap();

    let request = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(request.entries.len(), 2);
    let response = cluster.receive_append_entries(request).unwrap();
    assert!(!response.success);
    assert_eq!(
        response.reject_reason.as_deref(),
        Some("reorder_window_exceeded")
    );

    let report = cluster.byteraft_runtime_admin_report();
    let peer3 = report
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert_eq!(peer3.reorder_entries_accepted, 0);
    assert_eq!(peer3.reorder_entries_released, 0);
    assert_eq!(peer3.reorder_entries_rejected, 2);
    assert_eq!(cluster.commit_index(3).unwrap(), 0);
}

// shared-corpus: raft_byteraft_metrics_admin_pipeline_status server_raft_byteraft_runtime_admin_route
#[test]
fn byteraft_runtime_readiness_is_backed_by_admin_report_evidence() {
    let readiness = raft_byteraft_runtime_readiness();
    assert!(readiness.runtime_report_present);
    assert!(readiness.per_peer_pipeline_state_present);
    assert!(readiness.reorder_queue_state_present);
    assert!(readiness.snapshot_sender_downloader_lifecycle_present);
    assert!(readiness.wal_segment_lifecycle_present);
    assert!(readiness.read_index_lease_semantics_present);
    assert!(readiness.stale_follower_write_rejection_present);
    assert!(readiness.admin_status_surface_present);
    assert!(readiness.process_path_operational_semantics_ready);
    assert!(readiness.missing.is_empty(), "{:?}", readiness.missing);
    assert!(readiness.report.ready);
}

// shared-corpus: raft_byteraft_election_controls raft_byteraft_metrics_admin_pipeline_status
#[test]
fn byteraft_offline_timeout_state_is_reported_per_peer() {
    let cluster = RaftCluster::new_single_shard_with_config(
        214,
        [1, 2, 3],
        RaftConfig {
            enable_pre_vote: true,
            offline_timeout_tick: 10,
            ..RaftConfig::default()
        },
    )
    .unwrap();

    cluster.set_alive(3, false).unwrap();
    cluster.advance_time_ms(9);
    let before = cluster.byteraft_runtime_admin_report();
    let peer3 = before
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert_eq!(peer3.offline_elapsed_ms, 9);
    assert!(!peer3.offline_timeout_reached);
    assert_eq!(peer3.offline_timeout_rejections, 0);

    cluster.advance_time_ms(1);
    let after = cluster.byteraft_runtime_admin_report();
    let peer3 = after
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert_eq!(peer3.offline_elapsed_ms, 10);
    assert!(peer3.offline_timeout_reached);
    assert_eq!(peer3.offline_timeout_rejections, 1);

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_offline_elapsed_ms"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_offline_timeout_reached"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_offline_timeout_rejections"));

    cluster.set_alive(3, true).unwrap();
    let recovered = cluster.byteraft_runtime_admin_report();
    let peer3 = recovered
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert_eq!(peer3.offline_elapsed_ms, 0);
    assert!(!peer3.offline_timeout_reached);
    assert_eq!(peer3.offline_timeout_rejections, 1);
}

// shared-corpus: raft_byteraft_election_controls raft_byteraft_metrics_admin_pipeline_status
#[test]
fn byteraft_leader_transfer_timeout_is_reported_per_peer() {
    let cluster = RaftCluster::new_single_shard_with_config(
        216,
        [1, 2, 3],
        RaftConfig {
            transfer_timeout_tick: 3,
            ..RaftConfig::default()
        },
    )
    .unwrap();

    cluster.begin_leader_transfer(3).unwrap();
    cluster.advance_time_ms(2);
    let before = cluster.byteraft_runtime_admin_report();
    let peer3 = before
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert!(peer3.transfer_leader_target);
    assert_eq!(peer3.transfer_leader_elapsed_ms, 2);
    assert_eq!(peer3.transfer_leader_timeouts, 0);
    assert_eq!(peer3.transfer_leader_rejected, 0);

    cluster.advance_time_ms(1);
    let after = cluster.byteraft_runtime_admin_report();
    let peer3 = after
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert!(!peer3.transfer_leader_target);
    assert_eq!(peer3.transfer_leader_elapsed_ms, 0);
    assert_eq!(peer3.transfer_leader_timeouts, 1);
    assert_eq!(peer3.transfer_leader_rejected, 1);

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_elapsed_ms"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_transfer_leader_timeouts"));

    cluster.transfer_leader(3).unwrap();
    let completed = cluster.byteraft_runtime_admin_report();
    let peer3 = completed
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert_eq!(completed.leader_id, 3);
    assert!(!peer3.transfer_leader_target);
    assert_eq!(peer3.transfer_leader_completed, 1);
    assert_eq!(peer3.transfer_leader_elapsed_ms, 0);
}

// shared-corpus: raft_byteraft_snapshot_lifecycle_depth raft_byteraft_metrics_admin_pipeline_status
#[test]
fn byteraft_snapshot_sender_timeout_clears_pipeline_and_reports_retry() {
    let cluster = RaftCluster::new_single_shard_with_config(
        215,
        [1, 2, 3],
        RaftConfig {
            send_snapshot_timeout_ms: 10,
            ..RaftConfig::default()
        },
    )
    .unwrap();
    cluster
        .propose(Command::StringSet {
            key: "snapshot-timeout".to_string(),
            value: b"seed".to_vec(),
        })
        .unwrap();
    let _request = cluster.build_install_snapshot_request(3).unwrap();

    cluster.advance_time_ms(9);
    let before = cluster.byteraft_runtime_admin_report();
    let peer3 = before
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert!(peer3.snapshot_sending);
    assert_eq!(peer3.snapshot_send_elapsed_ms, 9);
    assert_eq!(peer3.snapshot_send_timeouts, 0);

    cluster.advance_time_ms(1);
    let after = cluster.byteraft_runtime_admin_report();
    let peer3 = after
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 pipeline");
    assert!(!peer3.snapshot_sending);
    assert!(!peer3.snapshot_installing);
    assert_eq!(peer3.snapshot_send_elapsed_ms, 10);
    assert_eq!(peer3.snapshot_send_timeouts, 1);
    assert_eq!(peer3.snapshot_retry_count, 1);
    assert_eq!(peer3.snapshot_send_failed, 1);
    assert_eq!(peer3.snapshot_backpressure_rejections, 1);

    let metrics = cluster.prometheus_metrics();
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_send_elapsed_ms"));
    assert!(metrics.contains("temporalstore_raft_byteraft_peer_snapshot_send_timeouts"));

    let retry = cluster.build_install_snapshot_request(3);
    assert!(retry.is_ok(), "timed-out sender should allow retry");
}

#[test]
fn raft_openraft_rollout_readiness_reports_real_process_rollout_evidence() {
    let readiness = raft_openraft_rollout_readiness();
    assert_eq!(readiness.adapter_present, cfg!(feature = "openraft-engine"));
    assert!(readiness.data_node_process_startup_selects_openraft);
    assert!(readiness.metaserver_process_startup_selects_openraft);
    assert!(readiness.durable_log_state_present);
    assert_eq!(
        readiness.local_rollout_ready,
        cfg!(feature = "openraft-engine")
    );
    assert!(!readiness.data_node_real_process_rollout_validated);
    assert!(!readiness.metaserver_real_process_rollout_validated);
    assert!(!readiness.multi_process_log_store_validation_present);
    assert!(!readiness.production_ready);
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("data-node process rollout")));
    assert!(readiness
        .missing
        .iter()
        .any(|item| item.contains("metaserver process rollout")));
    if !cfg!(feature = "openraft-engine") {
        assert!(readiness
            .missing
            .iter()
            .any(|item| item.contains("OpenRaft production engine adapter")));
    }

    let distributed = distributed_raft_readiness();
    assert_eq!(
        distributed.openraft_engine_adapter_present,
        cfg!(feature = "openraft-engine")
    );
    assert!(distributed.openraft_data_node_process_startup_present);
    assert!(distributed.openraft_metaserver_process_startup_present);
    assert!(distributed
        .missing
        .iter()
        .any(|item| item.contains("OpenRaft data-node process rollout")));
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
fn production_raft_mode_uses_openraft_ready_path_when_adapter_is_enabled() {
    if cfg!(feature = "openraft-engine") {
        let readiness = validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed)
            .expect("default readiness build should enable OpenRaft production adapter");
        assert!(readiness.production_ready);
        assert!(readiness.missing.is_empty());
        require_production_raft_ready().expect("production Raft gate should pass");
    } else {
        let err =
            validate_raft_deployment_mode(RaftDeploymentMode::ProductionDistributed).unwrap_err();
        assert_eq!(err.mode, RaftDeploymentMode::ProductionDistributed);
        assert!(!err
            .missing
            .iter()
            .any(|item| item.contains("applied Raft index")));
        assert!(!err.missing.iter().any(|item| item.contains("learner add")));
        assert!(err
            .missing
            .iter()
            .any(|item| item.contains("OpenRaft production engine adapter")));
        assert_eq!(require_production_raft_ready().unwrap_err(), err);
    }
}

#[test]
fn production_raft_runtime_validates_security_timer_and_chaos_contracts() {
    let dir = tempfile::tempdir().unwrap();
    let options = ProductionRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
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
            engine: ProductionRaftEngineKind::OpenRaft,
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
    let read_safety = cluster.read_safety_runtime_state();
    assert_eq!(read_safety.pre_vote_requests, 1);
    assert_eq!(read_safety.pre_vote_accepted, 1);
    assert_eq!(read_safety.pre_vote_rejected, 0);
}

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
    let read_safety = cluster.read_safety_runtime_state();
    assert_eq!(read_safety.pre_vote_requests, 1);
    assert_eq!(read_safety.pre_vote_accepted, 0);
    assert_eq!(read_safety.pre_vote_rejected, 1);
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
    let rejected = cluster.byteraft_runtime_admin_report();
    let peer3 = rejected
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 transfer state");
    assert_eq!(peer3.transfer_leader_requests, 1);
    assert_eq!(peer3.transfer_leader_rejected, 1);
    assert_eq!(peer3.transfer_leader_accepted, 0);
    cluster.catch_up(3).unwrap();
    assert!(cluster.read_index(3).is_ok());
    assert!(cluster.transfer_leader(3).is_ok());
    let completed = cluster.byteraft_runtime_admin_report();
    let peer3 = completed
        .peer_pipeline_states
        .iter()
        .find(|peer| peer.peer_id == 3)
        .expect("peer 3 transfer state");
    assert_eq!(peer3.transfer_leader_requests, 2);
    assert_eq!(peer3.transfer_leader_rejected, 1);
    assert_eq!(peer3.transfer_leader_accepted, 1);
    assert_eq!(peer3.transfer_leader_completed, 1);
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

// shared-corpus: raft_byteraft_membership_roles
#[test]
fn learner_and_witness_roles_match_cpp_membership_shape() {
    let cluster = RaftCluster::new_single_shard(1, [1, 2, 3]);
    cluster
        .add_node_with_role(4, RaftReplicaRole::Learner)
        .unwrap();
    cluster
        .add_node_with_role(5, RaftReplicaRole::Witness)
        .unwrap();
    cluster.add_learner_with_auto_promote(6, true).unwrap();

    let status = cluster.status();
    assert_eq!(status.majority, 3);
    assert_eq!(status.live_voters, 5);
    assert_eq!(
        cluster.local_status(4).unwrap().replica_role,
        RaftReplicaRole::Learner
    );
    assert_eq!(
        cluster.local_status(5).unwrap().replica_role,
        RaftReplicaRole::Witness
    );
    assert_eq!(
        cluster.local_status(6).unwrap().replica_role,
        RaftReplicaRole::Voter
    );
    assert_eq!(cluster.membership().voters, vec![1, 2, 3, 5, 6]);
    let local_status = cluster.byteraft_local_status_report();
    assert!(local_status.witness_membership_present);
    assert!(local_status.learner_membership_present);
    assert!(local_status.learner_auto_promote_present);
    assert!(local_status.peers.iter().any(|peer| {
        peer.status.node_id == 5 && peer.participates_in_quorum && !peer.can_serve_data
    }));
    assert!(local_status.peers.iter().any(|peer| {
        peer.status.node_id == 6
            && peer.can_be_leader
            && peer.pipeline_state.auto_promoted_from_learner
    }));
    let admin = cluster.byteraft_runtime_admin_report();
    assert!(admin.witness_membership_present);
    assert!(admin.learner_auto_promote_present);

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

// shared-corpus: raft_byteraft_rolling_restart_joint_consensus_fault_harness
#[test]
fn pending_joint_consensus_survives_rolling_restore_and_completes() {
    let dir = tempfile::tempdir().unwrap();
    let cluster =
        RaftCluster::new_single_shard_with_wal(dir.path(), 1, [1, 2, 3], RaftConfig::default())
            .unwrap();
    cluster.begin_joint_consensus([1, 2, 3, 4]).unwrap();
    let pending = cluster.byteraft_local_status_report();
    assert!(pending.pending_joint_consensus.is_some());
    assert!(pending
        .peers
        .iter()
        .any(|peer| peer.status.node_id == 4 && peer.can_be_leader));

    let restored = RaftCluster::restore_single_shard_from_wal(
        dir.path(),
        1,
        [1, 2, 3, 4],
        RaftConfig::default(),
    )
    .unwrap();
    let restored_pending = restored.byteraft_local_status_report();
    assert!(restored_pending.pending_joint_consensus.is_some());
    assert!(restored_pending
        .pending_joint_consensus
        .as_ref()
        .unwrap()
        .new_voters
        .contains(&4));
    restored.commit_joint_consensus().unwrap();
    assert!(restored
        .byteraft_local_status_report()
        .pending_joint_consensus
        .is_none());
    assert_eq!(restored.membership().voters, vec![1, 2, 3, 4]);
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
        engine: ProductionRaftEngineKind::OpenRaft,
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
        engine: ProductionRaftEngineKind::OpenRaft,
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
        engine: ProductionRaftEngineKind::OpenRaft,
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
    assert!(report.learner_added);
    assert!(report.catch_up_verified);
    assert!(report.promoted_to_voter);
    assert!(report.membership_committed);
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

#[cfg(feature = "openraft-engine")]
#[test]
fn production_raft_readiness_requires_openraft_process_and_meta_owned_membership_evidence() {
    let rollout = raft_openraft_rollout_readiness();
    assert!(rollout.adapter_present);
    assert!(!rollout.data_node_real_process_rollout_validated);
    assert!(!rollout.metaserver_real_process_rollout_validated);
    assert!(!rollout.multi_process_log_store_validation_present);
    assert!(!rollout.production_ready);

    let membership = raft_metaserver_membership_readiness();
    assert!(membership.networked_scheduler_transport_present);
    assert!(membership.persisted_scheduler_task_state_present);
    assert!(membership.real_data_node_group_execution_present);
    assert!(membership.production_ready);

    let readiness = distributed_raft_readiness();
    assert_eq!(readiness.mode, RaftDeploymentMode::ProductionDistributed);
    assert!(readiness.metaserver_driven_membership_present);
    assert!(readiness.production_ready);
    assert!(readiness.missing.is_empty());
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
        learner_added: true,
        catch_up_verified: true,
        promoted_to_voter: true,
        membership_committed: true,
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
        follower_lag_validated: true,
        failover_validated: true,
        scale_up_validated: true,
        scale_down_validated: true,
        secondary_replication_validated: true,
        networked_process_api_used: true,
        persisted_through_meta_raft_replay: true,
        ready: true,
        blockers: Vec::new(),
    };
    assert!(report.ready);
    assert!(report.networked_process_api_used);
    assert!(report.persisted_through_meta_raft_replay);
    assert!(report.stale_scheduler_token_rejected);
    assert_eq!(report.workflow.final_voters, vec![2, 3, 4]);
}

#[test]
fn metaserver_membership_workflow_requires_meta_majority() {
    let meta = ProductionMetaRaftRuntime::start(ProductionMetaRaftRuntimeOptions {
        engine: ProductionRaftEngineKind::OpenRaft,
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
    for index in 0..8 {
        cluster
            .propose(Command::StringSet {
                key: "segmented-wal".to_string(),
                value: format!("v{index}").into_bytes(),
            })
            .unwrap();
        for (node_id, record) in cluster.wal_records() {
            wal.persist_node_segmented(7, node_id, &record, 256, 2)
                .unwrap();
        }
    }

    let report = wal.segment_report(7, 1).unwrap();
    assert_eq!(report.segments.len(), 2);
    assert!(report.active_segment_id >= 2);
    assert!(report.segments.iter().all(|segment| segment.bytes > 0));
    assert!(report
        .segments
        .iter()
        .all(|segment| segment.record_count > 0));
    assert!(report
        .segments
        .iter()
        .all(|segment| segment.last_sequence >= segment.first_sequence));
    assert!(report.segments.last().unwrap().last_sequence > 0);

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
fn local_recovery_proof_covers_raft_wal_oplog_indexlog_and_pages() {
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
    engine.page_store().roll_segment().unwrap();
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
    assert!(report
        .segments
        .iter()
        .all(|segment| segment.record_count > 0));
    assert!(report.segments.last().unwrap().last_sequence > 0);
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
