use std::sync::{Arc, Mutex};

use temporalstore_rust::types::{
    FeatureFilter, FeatureFilterOp, FeatureWritePolicy, SequenceFeatureRow, SequenceQuerySpec,
};
use temporalstore_rust::{
    execute_redis_command, production_readiness_report, Command, CommandResponse, Config,
    EndToEndWorkflow, ExecuteRequest, FeaturePoint, RespValue, ScanStreamRequest,
    ServiceReadinessSummary, SetConfigRequest, SharedStoreOplogEntry, SharedStoreReplicator,
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
fn production_readiness_service_summary_is_public_api() {
    let report = production_readiness_report();
    assert_eq!(
        report.known_services(),
        vec!["client", "proxy", "ingestion", "data_node", "metaserver"]
    );
    let gates = report.service_gate_reports();
    assert_eq!(gates.len(), 5);
    assert_eq!(
        gates
            .iter()
            .map(|gate| (
                gate.remediation_order,
                gate.service.as_str(),
                gate.owner.as_str()
            ))
            .collect::<Vec<_>>(),
        vec![
            (1, "client", "client_sdk"),
            (2, "proxy", "proxy_runtime"),
            (3, "ingestion", "ingestion_connectors"),
            (4, "data_node", "data_node_runtime"),
            (5, "metaserver", "metaserver_control_plane")
        ]
    );
    assert!(gates.iter().all(|gate| gate.gate_status == "blocked"));
    assert_eq!(
        gates
            .iter()
            .map(|gate| (gate.service.as_str(), gate.severity.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("client", "critical"),
            ("proxy", "warning"),
            ("ingestion", "critical"),
            ("data_node", "critical"),
            ("metaserver", "critical")
        ]
    );
    let data_node: ServiceReadinessSummary = report
        .service_summary("data_node")
        .expect("data node service summary should be exported")
        .clone();
    assert!(!data_node.ready);
    assert!(data_node.areas.contains(&"dataserver".to_string()));
    assert!(data_node
        .areas
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node
        .blocker_classes
        .contains(&"data_node_local_lifecycle".to_string()));
    assert!(data_node
        .blocker_classes
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node.next_action.contains("Raft"));
    let typed_blockers = report.failed_capabilities_for_service("data_node");
    assert_eq!(typed_blockers.len(), data_node.blocker_count);
    assert!(typed_blockers
        .iter()
        .any(|blocker| blocker.area == "dataserver"));
    assert!(typed_blockers
        .iter()
        .any(|blocker| blocker.area == "data_node_distributed_raft"));
    assert!(!report.service_ready("data_node"));
    assert!(report
        .blocked_services()
        .iter()
        .any(|summary| summary.service == "data_node"));
    let gate = report
        .service_gate_report("data_node")
        .expect("data node service gate report should be exported");
    assert_eq!(gate.service, "data_node");
    assert!(!gate.ready);
    assert_eq!(gate.gate_status, "blocked");
    assert_eq!(gate.severity, "critical");
    assert_eq!(gate.remediation_order, 4);
    assert_eq!(gate.owner, "data_node_runtime");
    assert_eq!(
        gate.areas,
        vec![
            "dataserver".to_string(),
            "data_node_distributed_raft".to_string()
        ]
    );
    assert_eq!(gate.blocker_count, data_node.blocker_count);
    let primary = gate
        .primary_blocker
        .as_ref()
        .expect("blocked data node gate should expose a primary blocker");
    assert_eq!(primary.area, "data_node_distributed_raft");
    assert!(primary.capability.contains("OpenRaft") || primary.capability.contains("raft-rs"));
    assert_eq!(
        gate.primary_blocker.as_ref(),
        gate.failed_capabilities.first()
    );
    assert_eq!(gate.failed_capabilities.len(), data_node.blocker_count);
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
fn cxx_feature_module_simple_missing_truncate_policy_replace_and_delete() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    assert!(
        engine
            .set_config(SetConfigRequest {
                shard_id: 1,
                config: Config {
                    version: 2,
                    feature_max_size: 5,
                    ..Config::default()
                },
            })
            .ok
    );

    assert_eq!(
        execute(
            &engine,
            Command::FeatureQuery {
                key: "missing-key".to_string(),
                start_ms: 0,
                end_ms: 1_000,
                count: Some(100),
            },
        ),
        CommandResponse::FeaturePoints { points: Vec::new() }
    );

    let points = (0..8_u64)
        .map(|offset| FeaturePoint {
            timestamp_ms: 10_000 + offset,
            value: (10_000 + offset).to_string().into_bytes(),
        })
        .collect::<Vec<_>>();
    execute(
        &engine,
        Command::FeatureAppend {
            key: "key1".to_string(),
            points,
        },
    );

    let CommandResponse::FeaturePoints { points } = execute(
        &engine,
        Command::FeatureQuery {
            key: "key1".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(100),
        },
    ) else {
        panic!("expected feature points");
    };
    assert_eq!(points.len(), 5);
    assert_eq!(
        points
            .iter()
            .map(|point| point.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![10_003, 10_004, 10_005, 10_006, 10_007]
    );

    assert_eq!(
        execute(
            &engine,
            Command::FeatureAppendWithPolicy {
                key: "key1".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 10_007,
                    value: b"ignored".to_vec(),
                }],
                policy: FeatureWritePolicy::InsertIfAbsent,
            },
        ),
        CommandResponse::Integer { value: 0 }
    );
    assert_eq!(
        execute(
            &engine,
            Command::FeatureAppendWithPolicy {
                key: "key1".to_string(),
                points: vec![FeaturePoint {
                    timestamp_ms: 10_007,
                    value: b"replaced-existing".to_vec(),
                }],
                policy: FeatureWritePolicy::ReplaceExisting,
            },
        ),
        CommandResponse::Integer { value: 1 }
    );
    assert_eq!(
        execute(
            &engine,
            Command::FeatureQuery {
                key: "key1".to_string(),
                start_ms: 10_007,
                end_ms: 10_007,
                count: Some(1),
            },
        ),
        CommandResponse::FeaturePoints {
            points: vec![FeaturePoint {
                timestamp_ms: 10_007,
                value: b"replaced-existing".to_vec(),
            }]
        }
    );

    execute(
        &engine,
        Command::FeatureReplace {
            key: "key1".to_string(),
            start_ms: 10_004,
            end_ms: 10_006,
            points: vec![FeaturePoint {
                timestamp_ms: 10_004,
                value: b"replacement-window".to_vec(),
            }],
        },
    );
    let CommandResponse::FeaturePoints { points } = execute(
        &engine,
        Command::FeatureQuery {
            key: "key1".to_string(),
            start_ms: 0,
            end_ms: u64::MAX,
            count: Some(100),
        },
    ) else {
        panic!("expected feature points");
    };
    assert_eq!(
        points
            .iter()
            .map(|point| (point.timestamp_ms, point.value.clone()))
            .collect::<Vec<_>>(),
        vec![
            (10_003, b"10003".to_vec()),
            (10_004, b"replacement-window".to_vec()),
            (10_007, b"replaced-existing".to_vec()),
        ]
    );

    execute(
        &engine,
        Command::FeatureDelete {
            key: "key1".to_string(),
        },
    );
    assert_eq!(
        execute(
            &engine,
            Command::FeatureQuery {
                key: "key1".to_string(),
                start_ms: 0,
                end_ms: u64::MAX,
                count: Some(100),
            },
        ),
        CommandResponse::FeaturePoints { points: Vec::new() }
    );
}

#[test]
fn cxx_feature_filter_count_is_scan_bound_before_filtering() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let rows = (0..6_u64)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: 1_000 + offset,
            gid: 100 + offset,
            action_type: if offset >= 3 { 3 } else { 1 },
            duration: 10 + offset as u32,
            author_id: 7,
        })
        .collect::<Vec<_>>();
    execute(
        &engine,
        Command::FeatureAppend {
            key: "feature-sequence".to_string(),
            points: rows
                .iter()
                .map(|row| FeaturePoint {
                    timestamp_ms: row.timestamp_ms,
                    value: row.encode_cpp_feature_value(),
                })
                .collect(),
        },
    );

    assert_eq!(
        execute(
            &engine,
            Command::FeatureQueryFiltered {
                key: "feature-sequence".to_string(),
                start_ms: 1_000,
                end_ms: 2_000,
                count: Some(3),
                filters: vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
            },
        ),
        CommandResponse::FeaturePoints { points: Vec::new() }
    );

    let CommandResponse::FeaturePoints { points } = execute(
        &engine,
        Command::FeatureQueryFiltered {
            key: "feature-sequence".to_string(),
            start_ms: 1_000,
            end_ms: 2_000,
            count: Some(6),
            filters: vec![
                FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                },
                FeatureFilter {
                    field: "duration".to_string(),
                    op: FeatureFilterOp::GreaterOrEqual,
                    value: 14,
                },
            ],
        },
    ) else {
        panic!("expected feature points");
    };
    assert_eq!(
        points
            .iter()
            .map(|point| point.timestamp_ms)
            .collect::<Vec<_>>(),
        vec![1_004, 1_005]
    );
}

#[test]
fn cxx_sequence_feature_sdk_filters_batch_and_count_scan_bound() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = "cpp:user:42:sequence".to_string();
    let rows = vec![
        SequenceFeatureRow {
            timestamp_ms: 1_700_000_000_000,
            gid: 900,
            action_type: 1,
            duration: 31,
            author_id: 7_000,
        },
        SequenceFeatureRow {
            timestamp_ms: 1_700_000_001_000,
            gid: 901,
            action_type: 3,
            duration: 120,
            author_id: 7_001,
        },
        SequenceFeatureRow {
            timestamp_ms: 1_700_000_002_000,
            gid: 902,
            action_type: 3,
            duration: 40,
            author_id: 7_002,
        },
    ];
    execute(
        &engine,
        Command::SequenceAdd {
            key: key.clone(),
            rows: vec![rows[2].clone(), rows[0].clone(), rows[1].clone()],
        },
    );

    assert_eq!(
        execute(
            &engine,
            Command::SequenceQuery {
                key: key.clone(),
                start_ms: 1_700_000_000_000,
                end_ms: 1_700_000_003_000,
                count: 1,
                filters: vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
            },
        ),
        CommandResponse::SequenceRows { rows: Vec::new() }
    );
    assert_eq!(
        execute(
            &engine,
            Command::SequenceQuery {
                key: key.clone(),
                start_ms: 1_700_000_000_000,
                end_ms: 1_700_000_003_000,
                count: 3,
                filters: vec![FeatureFilter {
                    field: "action_type".to_string(),
                    op: FeatureFilterOp::Equal,
                    value: 3,
                }],
            },
        ),
        CommandResponse::SequenceRows {
            rows: vec![rows[1].clone(), rows[2].clone()]
        }
    );

    assert_eq!(
        execute(
            &engine,
            Command::SequenceBatchQuery {
                queries: vec![
                    SequenceQuerySpec {
                        key: key.clone(),
                        start_ms: 1_700_000_000_000,
                        end_ms: 1_700_000_003_000,
                        count: 3,
                        filters: vec![FeatureFilter {
                            field: "duration".to_string(),
                            op: FeatureFilterOp::GreaterThan,
                            value: 50,
                        }],
                    },
                    SequenceQuerySpec {
                        key: "missing-sequence".to_string(),
                        start_ms: 0,
                        end_ms: u64::MAX,
                        count: 10,
                        filters: Vec::new(),
                    },
                ],
            },
        ),
        CommandResponse::SequenceRowGroups {
            groups: vec![
                (key, vec![rows[1].clone()]),
                ("missing-sequence".to_string(), Vec::new()),
            ]
        }
    );
}

#[test]
fn cxx_long_sequence_feature_5k_ordered_windows_and_random_filters() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let key = "cpp-long-sequence".to_string();
    let base_ts = 1_700_000_000_000_u64;
    let rows = (0..5_000_u64)
        .map(|offset| SequenceFeatureRow {
            timestamp_ms: base_ts + offset,
            gid: 1_000_000 + (offset % 4096),
            action_type: (offset % 5) as u32,
            duration: ((offset * 17) % 300) as u32,
            author_id: 500_000 + (offset % 113),
        })
        .collect::<Vec<_>>();
    let shuffled = (0..rows.len())
        .map(|i| rows[(i * 2_919) % rows.len()].clone())
        .collect::<Vec<_>>();
    execute(
        &engine,
        Command::SequenceAdd {
            key: key.clone(),
            rows: shuffled,
        },
    );

    for seed in 0..16_u64 {
        let start_offset = (seed * 313) % 4_400;
        let window_rows = 100 + ((seed * 97) % 900);
        let count = window_rows as usize;
        let end_offset = (start_offset + window_rows).min(4_999);
        let filters = vec![
            FeatureFilter {
                field: "action_type".to_string(),
                op: FeatureFilterOp::NotEqual,
                value: seed % 5,
            },
            FeatureFilter {
                field: "duration".to_string(),
                op: FeatureFilterOp::GreaterThan,
                value: 80 + (seed * 13) % 120,
            },
            FeatureFilter {
                field: "gid".to_string(),
                op: FeatureFilterOp::LessThan,
                value: 1_003_500 + seed * 17,
            },
        ];
        let CommandResponse::SequenceRows { rows: actual } = execute(
            &engine,
            Command::SequenceQuery {
                key: key.clone(),
                start_ms: base_ts + start_offset,
                end_ms: base_ts + end_offset,
                count,
                filters: filters.clone(),
            },
        ) else {
            panic!("expected sequence rows");
        };
        let expected = rows
            .iter()
            .filter(|row| row.timestamp_ms >= base_ts + start_offset)
            .filter(|row| row.timestamp_ms <= base_ts + end_offset)
            .take(count)
            .filter(|row| {
                filters
                    .iter()
                    .all(|filter| test_sequence_filter_matches(row, filter))
            })
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert!(actual
            .windows(2)
            .all(|pair| pair[0].timestamp_ms < pair[1].timestamp_ms));
    }
}

#[test]
fn cxx_redis_feature_commands_cover_module_flow() {
    let engine = TemporalEngine::default();
    engine.load_shard(1);
    let run = |args: Vec<Vec<u8>>| {
        execute_redis_command(args, 1, |command| {
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command,
            });
            if response.status.ok {
                Ok(response.response)
            } else {
                Err(response.status.message)
            }
        })
    };
    let s = |value: &str| value.as_bytes().to_vec();

    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("100"), s("2")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("200"), s("3")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("300"), s("sum")]),
        RespValue::Integer(5)
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("300"), s("10")]),
        RespValue::Array(vec![
            RespValue::Array(vec![RespValue::Integer(100), RespValue::Bulk(Some(s("2")))]),
            RespValue::Array(vec![RespValue::Integer(200), RespValue::Bulk(Some(s("3")))]),
        ])
    );

    let encoded = SequenceFeatureRow {
        timestamp_ms: 300,
        gid: 42,
        action_type: 3,
        duration: 90,
        author_id: 7,
    }
    .encode_cpp_feature_value();
    assert_eq!(
        run(vec![s("FAPPEND"), s("rf"), s("300"), encoded.clone()]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![
            s("FQUERYFILTERSTR"),
            s("rf"),
            s("0"),
            s("400"),
            s("10"),
            s("action_type = 3"),
            s("duration > 80"),
        ]),
        RespValue::Array(vec![RespValue::Array(vec![
            RespValue::Integer(300),
            RespValue::Bulk(Some(encoded)),
        ])])
    );

    assert_eq!(
        run(vec![
            s("FAPPENDPOLICY"),
            s("rf"),
            s("300"),
            s("ignored"),
            s("insert_if_absent"),
        ]),
        RespValue::Integer(0)
    );
    assert_eq!(
        run(vec![
            s("FREPLACE"),
            s("rf"),
            s("0"),
            s("250"),
            s("150"),
            s("10")
        ]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FAGG"), s("rf"), s("0"), s("400"), s("sum")]),
        RespValue::Integer(10)
    );
    assert_eq!(
        run(vec![s("FDEL"), s("rf")]),
        RespValue::SimpleString("OK".to_string())
    );
    assert_eq!(
        run(vec![s("FQUERY"), s("rf"), s("0"), s("400"), s("10")]),
        RespValue::Array(Vec::new())
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
            execute(&reopened, Command::StringGet { key: key.clone() },),
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

fn test_sequence_filter_matches(row: &SequenceFeatureRow, filter: &FeatureFilter) -> bool {
    let lhs = match filter.field.as_str() {
        "gid" => row.gid,
        "action_type" => row.action_type as u64,
        "duration" => row.duration as u64,
        "author_id" => row.author_id,
        _ => return false,
    };
    match filter.op {
        FeatureFilterOp::Equal => lhs == filter.value,
        FeatureFilterOp::NotEqual => lhs != filter.value,
        FeatureFilterOp::GreaterThan => lhs > filter.value,
        FeatureFilterOp::GreaterOrEqual => lhs >= filter.value,
        FeatureFilterOp::LessThan => lhs < filter.value,
        FeatureFilterOp::LessOrEqual => lhs <= filter.value,
    }
}
