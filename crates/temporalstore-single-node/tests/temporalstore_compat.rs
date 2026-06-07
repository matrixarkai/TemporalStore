use std::sync::{Arc, Mutex};

use temporalstore_single_node::{
    execute_redis_command, Command, CommandResponse, EndToEndWorkflow, ExecuteRequest,
    FeaturePoint, RespValue, TemporalEngine,
};

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
