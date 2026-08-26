// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Test part 1, split from tests.rs.
#![allow(clippy::all)]
use super::*;
use super::helpers::*;

// shared-corpus: raft_temporal_raft_process_rollout_evidence
#[test]
fn matrixraft_parity_contract_is_library_consumable_and_openraft_free() {
    let contract = matrixraft_parity_contract();
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

    let cargo_toml = include_str!("../../../Cargo.toml").to_ascii_lowercase();
    assert!(!cargo_toml.contains("openraft"));
}

// shared-corpus: raft_temporal_raft_process_rollout_evidence raft_temporal_raft_process_read_safety_and_membership_matrix
#[test]
fn matrixraft_parity_report_tracks_distributed_readiness_fields() {
    let readiness = distributed_raft_readiness();
    let report = matrixraft_parity_report(&readiness);
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

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_round_trips_native_style_header() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        wal_sequence: 17,
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
    assert_eq!(decoded.wal_sequence, entry.wal_sequence);
    assert_eq!(decoded.command, entry.command);
    assert!(decoded.log_size > 0);
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn data_raft_log_codec_rejects_bad_header_and_sequence() {
    let entry = DataRaftLogCodecEntry {
        shard_id: 7,
        raft_index: 11,
        log_id: 13,
        log_size: 0,
        wal_sequence: 17,
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
        wal_sequence: 0,
        ..entry
    };
    assert!(matches!(
        serialize_data_raft_log(&zero_sequence),
        Err(RaftError::InvalidDataRaftLog(_))
    ));
}

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn native_data_raft_replication_rejects_corrupt_log_payload() {
    assert!(matches!(
        parse_data_raft_log(b"bad"),
        Err(RaftError::InvalidDataRaftLog(_))
    ));

    let entry = DataRaftLogCodecEntry {
        shard_id: 1,
        raft_index: 1,
        log_id: 1,
        log_size: 0,
        wal_sequence: 1,
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

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
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

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
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

// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle
#[test]
fn native_data_raft_replication_rejects_invalid_command_payload() {
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
fn native_data_raft_unavailable_consensus_fails_closed_for_safety_operations() {
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
// shared-corpus: raft_matrixraft_wal_log_codec_segment_lifecycle raft_temporal_raft_process_rollout_evidence
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
        wal_sequence: 1,
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
        wal_sequence: 1,
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
// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
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
// shared-corpus: raft_matrixraft_membership_roles_joint_consensus_matrix
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
        wal_sequence: 17,
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
    assert_eq!(applier.applied_wal_sequence(), 17);
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
        wal_sequence: 18,
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
        wal_sequence: 17,
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

    cluster
        .propose(Command::FeatureAppend {
            key: "chunked-raft-feature".to_string(),
            points: points.clone(),
        })
        .unwrap();

    cluster.catch_up(2).unwrap();
    cluster.catch_up(3).unwrap();
    for node_id in [1, 2, 3] {
        assert_eq!(cluster.commit_index(node_id).unwrap(), 1);
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

    cluster
        .propose(Command::FeatureAppend {
            key: "snapshot-chunked-feature".to_string(),
            points: points.clone(),
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

// shared-corpus: raft_matrixraft_read_lease_fault_matrix
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

// shared-corpus: raft_matrixraft_read_lease_fault_matrix
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

    // A stale candidate is rejected because no voter will grant it -- its log is behind, so
    // every up-to-date voter refuses and the candidate cannot reach a majority. (It used to be
    // rejected by a local `candidate_log_would_win` precheck that reported ReplicaLagging; that
    // precheck read cached peer state and was part of the god-view promotion path, so rejection
    // is now the real vote round's outcome.) Either way it must NOT become leader.
    assert_eq!(
        cluster.elect_leader(3).unwrap_err(),
        RaftError::NoMajority {
            live: 1, // its own self-vote only
            required: 2,
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
            pre_vote: false,
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
fn append_entry_only_truncates_on_term_conflict_and_never_reapplies() {
    // Raft §5.3 + exactly-once: a stale/duplicate/reordered AppendEntries that is a
    // prefix of the committed log must NOT truncate committed entries or re-execute
    // them (double-applying non-idempotent commands). Truncation happens only on a
    // genuine term conflict, and the monotonic applied floor blocks re-execution.
    let mut node = new_node(1, RaftRole::Follower, 7);
    for index in 1..=5 {
        append_entry(
            &mut node,
            RaftLogEntry {
                term: 1,
                index,
                shard_id: 7,
                command: Command::StringSet {
                    key: format!("k{index}"),
                    value: b"v".to_vec(),
                },
            },
        );
    }
    node.commit_index = 5;
    apply_committed(&mut node);
    assert_eq!(node.applied_index, 5);
    assert_eq!(node.max_applied_index, 5);
    assert_eq!(node.log.len(), 5);

    // Stale SAME-term entry at an already-committed index: no truncation, no re-apply.
    append_entry(
        &mut node,
        RaftLogEntry {
            term: 1,
            index: 3,
            shard_id: 7,
            command: Command::StringSet {
                key: "k3".to_string(),
                value: b"v".to_vec(),
            },
        },
    );
    assert_eq!(
        node.log.len(),
        5,
        "a same-term stale entry must not truncate the committed log"
    );
    assert_eq!(node.applied_index, 5);
    assert!(
        apply_committed(&mut node).is_none(),
        "already-applied indices must not re-execute (monotonic floor)"
    );

    // A genuine term conflict at an UNCOMMITTED index truncates and replaces.
    append_entry(
        &mut node,
        RaftLogEntry {
            term: 1,
            index: 6,
            shard_id: 7,
            command: Command::StringSet {
                key: "k6".to_string(),
                value: b"a".to_vec(),
            },
        },
    );
    assert_eq!(node.log.len(), 6);
    append_entry(
        &mut node,
        RaftLogEntry {
            term: 2,
            index: 6,
            shard_id: 7,
            command: Command::StringSet {
                key: "k6".to_string(),
                value: b"b".to_vec(),
            },
        },
    );
    assert_eq!(node.log.len(), 6, "term conflict replaces, not appends");
    assert_eq!(node.log.last().map(|entry| entry.term), Some(2));
}

#[test]
fn election_up_to_date_check_is_snapshot_aware() {
    // After log compaction a caught-up replica's committed tail lives in its installed
    // snapshot, not `log`. The election "up-to-date" comparison must use the snapshot tail.
    // Otherwise a fully-snapshotted-but-caught-up replica advertises (0,0), loses to peers
    // that still hold uncompacted logs, and cannot win an election it is fully eligible for
    // (liveness) -- while also, symmetrically, granting votes to genuinely lagging
    // candidates (safety). The sibling meta-raft election path already does this correctly.
    let cluster =
        RaftCluster::new_single_shard_with_config(1, [1, 2, 3], RaftConfig::default()).unwrap();
    cluster.elect_leader(1).unwrap();
    for i in 0..6 {
        cluster
            .propose(Command::StringSet {
                key: format!("k{i}"),
                value: b"v".to_vec(),
            })
            .unwrap();
    }
    let commit = cluster.commit_index(1).unwrap();
    assert!(commit >= 6, "cluster should have committed the proposals");

    // Compact ONLY node 2 up to the committed index: its `log` empties while its committed
    // tail moves into the installed snapshot. It remains fully caught up (commit == leader).
    let snapshot_req = cluster.build_install_snapshot_request(2).unwrap();
    let snapshot_resp = cluster.receive_install_snapshot(snapshot_req).unwrap();
    assert!(snapshot_resp.success);
    assert_eq!(cluster.commit_index(2).unwrap(), commit);

    // The snapshotted-but-caught-up replica must be electable (pre-fix: ReplicaLagging
    // because its empty log advertised tail (0,0)).
    cluster
        .elect_leader(2)
        .expect("a fully-snapshotted, fully-caught-up replica must be electable");
    assert_eq!(cluster.leader_id(), 2);
}

#[test]
fn append_entries_higher_term_clears_stale_vote() {
    // votedFor is per-term (Raft Fig-2): observing a higher term in AppendEntries must
    // reset it, else a stale vote from the old term wrongly suppresses this node's vote in
    // the new term (split-vote liveness bug). Mirrors the vote/snapshot-install paths.
    let cluster =
        RaftCluster::new_single_shard_with_config(1, [1, 2, 3], RaftConfig::default()).unwrap();

    // Node 3 grants its term-5 vote to candidate 1.
    let granted = cluster
        .receive_vote_request(VoteRequest {
            pre_vote: false,
            rpc: None,
            shard_id: 1,
            term: 5,
            candidate_id: 1,
            target_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(granted.vote_granted, "term-5 vote for candidate 1 should be granted");

    // A term-6 AppendEntries (from leader 2) advances node 3's term; the stale term-5 vote
    // for candidate 1 must be cleared.
    cluster
        .receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id: 1,
            term: 6,
            leader_id: 2,
            target_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        })
        .unwrap();

    // Node 3 must now be free to vote for a DIFFERENT candidate (2) in term 6. Pre-fix the
    // carried-over voted_for=1 rejected this as "already_voted".
    let regrant = cluster
        .receive_vote_request(VoteRequest {
            pre_vote: false,
            rpc: None,
            shard_id: 1,
            term: 6,
            candidate_id: 2,
            target_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(
        regrant.vote_granted,
        "a higher-term AppendEntries must clear the stale per-term vote so a new term's \
         election is not blocked (got reject: {:?})",
        regrant.reject_reason
    );
}

#[test]
fn install_snapshot_clears_stale_vote_on_term_raise() {
    // votedFor is per-term (Raft Fig-2). Installing a snapshot that raises the term via the
    // public install_snapshot path (not the receive_install_snapshot RPC wrapper, which
    // pre-clears) must reset a stale vote, else a same-new-term candidate is wrongly rejected
    // as already_voted -> split-vote liveness stall.
    let cluster =
        RaftCluster::new_single_shard_with_config(1, [1, 2, 3], RaftConfig::default()).unwrap();
    // Node 3 grants a term-3 vote to candidate 1.
    let granted = cluster
        .receive_vote_request(VoteRequest {
            pre_vote: false,
            rpc: None,
            shard_id: 1,
            term: 3,
            candidate_id: 1,
            target_id: 3,
            last_log_index: 0,
            last_log_term: 0,
        })
        .unwrap();
    assert!(granted.vote_granted);
    assert_eq!(cluster.hard_state(3).unwrap().voted_for, Some(1));

    // Install a term-5 snapshot directly on node 3.
    cluster
        .install_snapshot(
            3,
            RaftSnapshot {
                shard_id: 1,
                last_included_term: 5,
                last_included_index: 1,
                external_snapshot_ref: None,
                entries: vec![RaftLogEntry {
                    term: 5,
                    index: 1,
                    shard_id: 1,
                    command: Command::StringSet {
                        key: "k".to_string(),
                        value: b"v".to_vec(),
                    },
                }],
                state_image: None,
                state_image_externalized: false,
            },
        )
        .unwrap();

    // The stale term-3 vote must be cleared by the term raise to 5.
    assert_eq!(
        cluster.hard_state(3).unwrap().voted_for,
        None,
        "snapshot term-raise must clear the per-term vote"
    );
    assert_eq!(cluster.hard_state(3).unwrap().current_term, 5);

    // A DIFFERENT candidate can now win node 3's vote in term 5.
    let regrant = cluster
        .receive_vote_request(VoteRequest {
            pre_vote: false,
            rpc: None,
            shard_id: 1,
            term: 5,
            candidate_id: 2,
            target_id: 3,
            last_log_index: 1,
            last_log_term: 5,
        })
        .unwrap();
    assert!(
        regrant.vote_granted,
        "cleared vote must let a new term-5 candidate win (got reject: {:?})",
        regrant.reject_reason
    );
}

#[test]
fn append_entries_commit_clamps_to_last_new_entry_not_whole_log_tail() {
    // Raft Figure 2: a follower advances commitIndex only to min(leaderCommit, index of
    // the last NEW entry in THIS AppendEntries) -- never to its whole-log tail. A divergent
    // UNCOMMITTED suffix left by a failed prior-term leader must not be committed just
    // because it still sits in the log. Regression for the safety hole where the commit
    // line used node_last_log_or_snapshot_index (whole log) instead of the batch endpoint,
    // committing an entry the cluster never agreed on.
    let cluster =
        RaftCluster::new_single_shard_with_config(1, [1, 2, 3], RaftConfig::default()).unwrap();

    let seed = |term: u64,
                prev_index: u64,
                prev_term: u64,
                entries: Vec<RaftLogEntry>,
                leader_commit: u64| {
        cluster
            .receive_append_entries(AppendEntriesRequest {
                rpc: None,
                shard_id: 1,
                term,
                leader_id: 1,
                target_id: 3,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries,
                leader_commit,
            })
            .unwrap()
    };
    let entry = |term: u64, index: u64, key: &str| RaftLogEntry {
        term,
        index,
        shard_id: 1,
        command: Command::StringSet {
            key: key.to_string(),
            value: b"v".to_vec(),
        },
    };

    // Committed prefix 1..=3 @ term 1.
    seed(
        1,
        0,
        0,
        vec![entry(1, 1, "k1"), entry(1, 2, "k2"), entry(1, 3, "k3")],
        3,
    );
    // Divergent UNCOMMITTED suffix from a failed term-3 then term-5 leader (leader_commit
    // stays 3, so these never commit).
    seed(3, 3, 1, vec![entry(3, 4, "k4-stale")], 3);
    seed(5, 4, 3, vec![entry(5, 5, "k5-stale")], 3);
    assert_eq!(cluster.commit_index(3).unwrap(), 3);

    // The real leader now re-sends a matching-term entry 4 with a HIGH leader_commit (6).
    // Entry 4 matches by term so append is a no-op; the divergent entry 5 survives in the
    // log. commitIndex must clamp to 4 (this batch's last new entry), NOT 5 (whole-log tail).
    seed(5, 3, 1, vec![entry(3, 4, "k4-stale")], 6);

    assert_eq!(
        cluster.commit_index(3).unwrap(),
        4,
        "commitIndex must clamp to the last new entry (4), not the divergent whole-log tail (5)"
    );
}

// shared-corpus: raft_matrixraft_replication_backpressure
// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_replication_backpressure
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

    let admin = cluster.matrixraft_runtime_admin_report();
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

// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_replication_backpressure
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
    // A charged window no longer refuses -- it degrades to a single-entry PROBE, because the
    // refusal blocked the very acknowledgements that drain the window (a follower past the
    // window was cut off for good). The limit still binds: the batch never grows past one
    // entry while the window is charged.
    let probe = cluster.build_append_entries_request(3).unwrap();
    assert_eq!(
        probe.entries.len(),
        1,
        "a charged window must degrade to a single-entry probe, not a bigger batch"
    );

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

    let admin = cluster.matrixraft_runtime_admin_report();
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
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_append_queue_limit"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_inflight_bytes_limit"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_apply_inflight_limit"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_apply_queue_depth"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_apply_queue_max_depth"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_apply_batch_bytes_limit"));
}

// shared-corpus: raft_matrixraft_replication_backpressure
// shared-corpus: raft_matrixraft_pipeline_reorder_backpressure_matrix raft_matrixraft_replication_backpressure
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

    let admin = cluster.matrixraft_runtime_admin_report();
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
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_reorder_entry_timeouts"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_reorder_dropped_packages"));
    assert!(metrics.contains("temporalstore_raft_matrixraft_peer_stale_term_rejections"));
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
    // Node 1 granted its vote in the election that promoted node 2, so its per-term `voted_for`
    // records that grant. (The old promotion path wiped every follower's `voted_for` instead,
    // which is exactly the per-term vote bookkeeping Raft relies on.)
    assert_eq!(cluster.hard_state(1).unwrap().voted_for, Some(2));

    let first_vote = cluster
        .request_vote(VoteRequest {
            pre_vote: false,
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
            pre_vote: false,
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
            pre_vote: false,
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

// shared-corpus: raft_matrixraft_rpc_auth_deadline_transport_matrix
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

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix
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

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix
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

// shared-corpus: raft_matrixraft_snapshot_chunk_retry_rollback_matrix
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

