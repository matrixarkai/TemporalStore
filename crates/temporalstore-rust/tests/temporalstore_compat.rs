use std::sync::{Arc, Mutex};

use temporalstore_rust::{
    execute_redis_command, Command, CommandResponse, EndToEndWorkflow, ExecuteRequest,
    FeaturePoint, RespValue, ScanStreamRequest, SharedStoreOplogEntry, SharedStoreReplicator,
    StreamKind, StreamReadRequest, TemporalEngine,
};
use temporalstore_snapshot::object_store::FileObjectStore;

fn execute(engine: &TemporalEngine, command: Command) -> CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "{}", response.status.message);
    response.response
}

#[test]
fn cxx_basic_smoketest_string_reload_ttl_expire_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");

    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    execute(
        &engine,
        Command::StringSet {
            key: "hinata_set_key".to_string(),
            value: b"hinata_set_value".to_vec(),
        },
    );
    assert_eq!(
        execute(
            &engine,
            Command::StringGet {
                key: "hinata_set_key".to_string(),
            },
        ),
        CommandResponse::Bytes {
            value: Some(b"hinata_set_value".to_vec())
        }
    );
    execute(
        &engine,
        Command::StringSetEx {
            key: "hinata_set_key".to_string(),
            value: b"test_value_setex".to_vec(),
            ttl_ms: 10_000,
        },
    );

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        execute(
            &restarted,
            Command::StringGet {
                key: "hinata_set_key".to_string(),
            },
        ),
        CommandResponse::Bytes {
            value: Some(b"test_value_setex".to_vec())
        }
    );
    let CommandResponse::Integer { value: ttl } = execute(
        &restarted,
        Command::CommonTtl {
            key: "hinata_set_key".to_string(),
        },
    ) else {
        panic!("expected ttl");
    };
    assert!(ttl > 0 && ttl <= 10_000);

    execute(
        &restarted,
        Command::CommonExpire {
            key: "hinata_set_key".to_string(),
            ttl_ms: 0,
        },
    );
    assert_eq!(
        execute(
            &restarted,
            Command::StringGet {
                key: "hinata_set_key".to_string(),
            },
        ),
        CommandResponse::Bytes { value: None }
    );

    execute(
        &restarted,
        Command::StringSetEx {
            key: "hinata_set_key".to_string(),
            value: b"test_value_setex".to_vec(),
            ttl_ms: 10_000,
        },
    );
    execute(
        &restarted,
        Command::CommonDelete {
            key: "hinata_set_key".to_string(),
        },
    );
    assert_eq!(
        execute(
            &restarted,
            Command::StringGet {
                key: "hinata_set_key".to_string(),
            },
        ),
        CommandResponse::Bytes { value: None }
    );
}

#[test]
fn cxx_basic_smoketest_hash_reload_delete_getall_and_len() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");

    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);
    execute(
        &engine,
        Command::HashSet {
            key: "hinata_key".to_string(),
            field: "hinata_field".to_string(),
            value: b"hinata_value".to_vec(),
        },
    );
    assert_eq!(
        execute(
            &engine,
            Command::HashGet {
                key: "hinata_key".to_string(),
                field: "hinata_field".to_string(),
            },
        ),
        CommandResponse::Bytes {
            value: Some(b"hinata_value".to_vec())
        }
    );

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    restarted.load_shard(1);
    assert_eq!(
        execute(
            &restarted,
            Command::HashGet {
                key: "hinata_key".to_string(),
                field: "hinata_field".to_string(),
            },
        ),
        CommandResponse::Bytes {
            value: Some(b"hinata_value".to_vec())
        }
    );
    execute(
        &restarted,
        Command::HashSet {
            key: "hinata_key".to_string(),
            field: "field222".to_string(),
            value: b"value222".to_vec(),
        },
    );
    assert_eq!(
        execute(
            &restarted,
            Command::HashLen {
                key: "hinata_key".to_string(),
            },
        ),
        CommandResponse::Integer { value: 2 }
    );
    assert_eq!(
        execute(
            &restarted,
            Command::HashGetAll {
                key: "hinata_key".to_string(),
            },
        ),
        CommandResponse::HashEntries {
            entries: vec![
                ("field222".to_string(), b"value222".to_vec()),
                ("hinata_field".to_string(), b"hinata_value".to_vec()),
            ]
        }
    );
    execute(
        &restarted,
        Command::CommonDelete {
            key: "hinata_key".to_string(),
        },
    );
    assert_eq!(
        execute(
            &restarted,
            Command::HashGet {
                key: "hinata_key".to_string(),
                field: "hinata_field".to_string(),
            },
        ),
        CommandResponse::Bytes { value: None }
    );
}

#[test]
fn onebox_proxy_hash_multi_command_parity_over_redis_resp() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let run = |args: Vec<&str>| {
        execute_redis_command(
            args.into_iter()
                .map(|arg| arg.as_bytes().to_vec())
                .collect(),
            1,
            |command| {
                let response = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
                if response.status.ok {
                    Ok(response.response)
                } else {
                    Err(response.status.message)
                }
            },
        )
    };

    assert_eq!(
        run(vec![
            "HMSET",
            "test_hmget_key1",
            "field111",
            "value111",
            "field222",
            "value222",
        ]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec!["HMGET", "test_hmget_key1", "field111", "field200"]),
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"value111".to_vec())),
            RespValue::Bulk(None),
        ])
    );
    assert_eq!(
        run(vec!["HGETALL", "test_hmget_key1"]),
        RespValue::Array(vec![
            RespValue::Bulk(Some(b"field111".to_vec())),
            RespValue::Bulk(Some(b"value111".to_vec())),
            RespValue::Bulk(Some(b"field222".to_vec())),
            RespValue::Bulk(Some(b"value222".to_vec())),
        ])
    );
    assert_eq!(run(vec!["HLEN", "test_hmget_key1"]), RespValue::Integer(2));
}

#[test]
fn consistency_bench_style_hash_writes_are_linearizable_through_raft() {
    let workflow = EndToEndWorkflow::new(1, [1, 2, 3]);
    let checker = Arc::new(Mutex::new(SimpleKvChecker::default()));

    for _ in 0..16 {
        let value = checker.lock().unwrap().next_write_value();
        workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashSet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                    value: value.to_string().into_bytes(),
                },
            })
            .unwrap();
        checker.lock().unwrap().finish_write(value);

        let response = workflow
            .proxy()
            .execute(ExecuteRequest {
                shard_id: 1,
                command: Command::HashGet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                },
            })
            .unwrap();
        let CommandResponse::Bytes { value: Some(value) } = response.response else {
            panic!("expected hash value");
        };
        let value = std::str::from_utf8(&value).unwrap().parse::<u64>().unwrap();
        assert!(checker.lock().unwrap().finish_read(value));
    }

    workflow.set_data_node_alive(1, false).unwrap();
    let value = checker.lock().unwrap().next_write_value();
    workflow
        .proxy()
        .execute(ExecuteRequest {
            shard_id: 1,
            command: Command::HashSet {
                key: "consistent-key".to_string(),
                field: "field".to_string(),
                value: value.to_string().into_bytes(),
            },
        })
        .unwrap();
    checker.lock().unwrap().finish_write(value);
    assert_eq!(
        workflow
            .read_data_node(
                2,
                Command::HashGet {
                    key: "consistent-key".to_string(),
                    field: "field".to_string(),
                },
            )
            .unwrap(),
        CommandResponse::Bytes {
            value: Some(value.to_string().into_bytes())
        }
    );
}

#[test]
fn feature_module_smoke_matches_temporal_feature_flow() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    execute(
        &engine,
        Command::FeatureAppend {
            key: "feature-key".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 100,
                    value: b"2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 200,
                    value: b"3".to_vec(),
                },
            ],
        },
    );
    assert_eq!(
        execute(
            &engine,
            Command::FeatureAggQuery {
                key: "feature-key".to_string(),
                start_ms: 0,
                end_ms: 300,
                aggregator: "sum".to_string(),
                count: None,
            },
        ),
        CommandResponse::Aggregate { value: 5 }
    );
}

#[test]
fn cxx_stream_random_size_reopen_and_scan_matches_stream_test() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);

    let mut expected = Vec::new();
    for i in 0..24usize {
        let len = 1 + ((i * 7919) % 32_768);
        let value = deterministic_bytes(i as u64, len);
        let key = format!("stream-random-{i:03}");
        execute(
            &engine,
            Command::StringSet {
                key: key.clone(),
                value: value.clone(),
            },
        );
        expected.push((key, value));
    }

    let reopened = TemporalEngine::with_local_dirs(
        8 * 1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    reopened.load_shard(1);
    for (key, value) in &expected {
        assert_eq!(
            execute(
                &reopened,
                Command::StringGet {
                    key: key.clone(),
                },
            ),
            CommandResponse::Bytes {
                value: Some(value.clone())
            }
        );
    }

    let page = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_segment_id: 0,
        offset: 0,
        size: 1024 * 1024,
    });
    assert!(page.status.ok, "{}", page.status.message);
    assert!(!page.data.is_empty());
    for (_, value) in expected.iter().take(8) {
        assert!(
            page.data.windows(value.len()).any(|window| window == value),
            "page stream should contain deterministic value of len {}",
            value.len()
        );
    }

    let scan = reopened.scan_stream(ScanStreamRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_segment_id: 0,
        start_offset: 0,
        end_offset: u64::MAX,
        max_bytes: 1024 * 1024,
    });
    assert!(scan.status.ok, "{}", scan.status.message);
    assert!(!scan.records.is_empty());
    assert!(scan.end_of_stream);
}

#[test]
fn cxx_stream_cross_block_large_values_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let page_dir = dir.path().join("pages");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache-a"),
        &page_dir,
        &index_dir,
    );
    engine.load_shard(1);

    let large = deterministic_bytes(42, 512 * 1024);
    for i in 0..3 {
        execute(
            &engine,
            Command::StringSet {
                key: format!("stream-cross-block-{i}"),
                value: large.clone(),
            },
        );
    }

    let reopened = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        dir.path().join("cache-b"),
        &page_dir,
        &index_dir,
    );
    reopened.load_shard(1);
    for i in 0..3 {
        assert_eq!(
            execute(
                &reopened,
                Command::StringGet {
                    key: format!("stream-cross-block-{i}"),
                },
            ),
            CommandResponse::Bytes {
                value: Some(large.clone())
            }
        );
    }

    let first_chunk = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_segment_id: 0,
        offset: 0,
        size: 256 * 1024,
    });
    assert!(first_chunk.status.ok, "{}", first_chunk.status.message);
    assert_eq!(first_chunk.data, large[..256 * 1024].to_vec());

    let second_chunk = reopened.read_stream(StreamReadRequest {
        shard_id: 1,
        stream_kind: StreamKind::Page,
        page_segment_id: 0,
        offset: 256 * 1024,
        size: 256 * 1024,
    });
    assert!(second_chunk.status.ok, "{}", second_chunk.status.message);
    assert_eq!(second_chunk.data, large[256 * 1024..].to_vec());
}

#[tokio::test]
async fn cxx_shared_store_replication_bootstrap_and_oplog_replay_matches_primary_pull_model() {
    let dir = tempfile::tempdir().unwrap();
    let primary = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("primary-cache"),
        dir.path().join("primary-pages"),
        dir.path().join("primary-index"),
    );
    primary.load_shard(1);

    for command in [
        Command::StringSet {
            key: "shared-string".to_string(),
            value: b"checkpoint-value".to_vec(),
        },
        Command::HashSet {
            key: "shared-hash".to_string(),
            field: "field111".to_string(),
            value: b"value111".to_vec(),
        },
        Command::FeatureAppend {
            key: "shared-feature".to_string(),
            points: vec![
                FeaturePoint {
                    timestamp_ms: 100,
                    value: b"2".to_vec(),
                },
                FeaturePoint {
                    timestamp_ms: 200,
                    value: b"3".to_vec(),
                },
            ],
        },
    ] {
        execute(&primary, command);
    }

    let replicator = SharedStoreReplicator::new(
        "cluster-a",
        Arc::new(FileObjectStore::new(dir.path().join("shared-store"))),
    );
    let checkpoint = replicator
        .publish_checkpoint(1, 3, &primary, &primary.page_store())
        .await
        .unwrap();
    assert_eq!(checkpoint.checkpoint_oplog_index, 3);
    assert_eq!(checkpoint.page_segments.len(), 1);
    assert!(!checkpoint.index_sha256.is_empty());

    let later_commands = vec![
        Command::StringSet {
            key: "shared-string".to_string(),
            value: b"post-oplog-value".to_vec(),
        },
        Command::HashSet {
            key: "shared-hash".to_string(),
            field: "field222".to_string(),
            value: b"value222".to_vec(),
        },
        Command::FeatureAppend {
            key: "shared-feature".to_string(),
            points: vec![FeaturePoint {
                timestamp_ms: 300,
                value: b"4".to_vec(),
            }],
        },
    ];
    for (idx, command) in later_commands.iter().cloned().enumerate() {
        execute(&primary, command.clone());
        replicator
            .publish_oplog_entry(SharedStoreOplogEntry {
                shard_id: 1,
                oplog_index: 4 + idx as u64,
                command,
            })
            .await
            .unwrap();
    }

    let follower = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("follower-cache"),
        dir.path().join("follower-pages"),
        dir.path().join("follower-index"),
    );
    let restored_checkpoint = replicator
        .restore_latest_checkpoint(1, &follower, &follower.page_store())
        .await
        .unwrap();
    assert_eq!(restored_checkpoint.checkpoint_id, checkpoint.checkpoint_id);
    follower.load_shard(1);

    assert_eq!(
        execute(
            &follower,
            Command::StringGet {
                key: "shared-string".to_string(),
            },
        ),
        CommandResponse::Bytes {
            value: Some(b"checkpoint-value".to_vec())
        }
    );
    assert_eq!(
        execute(
            &follower,
            Command::HashLen {
                key: "shared-hash".to_string(),
            },
        ),
        CommandResponse::Integer { value: 1 }
    );

    let replay = replicator
        .replay_oplog(1, restored_checkpoint.checkpoint_oplog_index, &follower)
        .await
        .unwrap();
    assert_eq!(replay.applied, 3);
    assert_eq!(replay.last_oplog_index, 6);

    for command in [
        Command::StringGet {
            key: "shared-string".to_string(),
        },
        Command::HashGetAll {
            key: "shared-hash".to_string(),
        },
        Command::FeatureAggQuery {
            key: "shared-feature".to_string(),
            start_ms: 0,
            end_ms: 400,
            aggregator: "sum".to_string(),
            count: None,
        },
    ] {
        assert_eq!(
            execute(&follower, command.clone()),
            execute(&primary, command),
        );
    }

    assert!(follower.page_store().stats().reads > 0);
}

#[derive(Default)]
struct SimpleKvChecker {
    version: u64,
    committed: u64,
}

impl SimpleKvChecker {
    fn next_write_value(&mut self) -> u64 {
        self.version += 1;
        self.version
    }

    fn finish_write(&mut self, value: u64) {
        self.committed = self.committed.max(value);
    }

    fn finish_read(&self, value: u64) -> bool {
        value > 0 && value <= self.committed
    }
}

fn deterministic_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed ^ 0x9e37_79b9_7f4a_7c15;
    (0..len)
        .map(|_| {
            x ^= x << 7;
            x ^= x >> 9;
            x ^= x << 8;
            (x & 0xff) as u8
        })
        .collect()
}
