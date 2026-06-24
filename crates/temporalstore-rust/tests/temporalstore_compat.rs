use std::sync::{Arc, Mutex};

use temporalstore_rust::types::{
    FeatureFilter, FeatureFilterOp, FeatureWritePolicy, SequenceFeatureRow, SequenceQuerySpec,
};
use temporalstore_rust::{
    execute_redis_command, production_readiness_report, Command, CommandResponse, Config,
    EndToEndWorkflow, ExecuteRequest, FeaturePoint, RespValue, ServiceReadinessSummary,
    SetConfigRequest, SharedStoreOplogEntry, SharedStoreReplicator, TemporalEngine,
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
        vec![
            "client",
            "proxy",
            "ingestion",
            "data_node",
            "metaserver",
            "storage_cache",
            "feature_modules",
            "context_workflow",
            "fault_tolerance",
            "deployment_ops",
            "scale_testing",
            "raft_replication"
        ]
    );
    let gates = report.service_gate_reports();
    assert_eq!(gates.len(), 12);
    for (order, service, owner) in [
        (1, "client", "client_sdk"),
        (2, "proxy", "proxy_runtime"),
        (3, "ingestion", "ingestion_connectors"),
        (4, "data_node", "data_node_runtime"),
        (5, "metaserver", "metaserver_control_plane"),
        (6, "storage_cache", "storage_runtime"),
        (7, "feature_modules", "feature_api"),
        (8, "context_workflow", "context_ai_workflow"),
        (9, "fault_tolerance", "reliability"),
        (10, "deployment_ops", "platform_ops"),
        (11, "scale_testing", "performance"),
        (12, "raft_replication", "consensus_runtime"),
    ] {
        assert!(
            gates.iter().any(|gate| gate.remediation_order == order
                && gate.service == service
                && gate.owner == owner),
            "missing service gate {order}/{service}/{owner}"
        );
    }
    assert!(gates.iter().all(|gate| gate.gate_status == "ready"));
    assert_eq!(
        gates
            .iter()
            .map(|gate| (gate.service.as_str(), gate.severity.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("client", "ready"),
            ("proxy", "ready"),
            ("ingestion", "ready"),
            ("data_node", "ready"),
            ("metaserver", "ready"),
            ("storage_cache", "ready"),
            ("feature_modules", "ready"),
            ("context_workflow", "ready"),
            ("fault_tolerance", "ready"),
            ("deployment_ops", "ready"),
            ("scale_testing", "ready"),
            ("raft_replication", "ready")
        ]
    );
    assert!(report.next_blocked_service().is_none());
    let data_node: ServiceReadinessSummary = report
        .service_summary("data_node")
        .expect("data node service summary should be exported")
        .clone();
    assert!(data_node.ready);
    assert!(data_node.areas.contains(&"dataserver".to_string()));
    assert!(data_node
        .areas
        .contains(&"data_node_distributed_raft".to_string()));
    assert!(data_node.blocker_classes.is_empty());
    assert!(data_node.next_action.contains("ready"));
    let typed_blockers = report.failed_capabilities_for_service("data_node");
    assert_eq!(typed_blockers.len(), data_node.blocker_count);
    assert!(typed_blockers.is_empty());
    assert!(report.service_ready("data_node"));
    assert!(!report
        .blocked_services()
        .iter()
        .any(|summary| summary.service == "data_node"));
    let gate = report
        .service_gate_report("data_node")
        .expect("data node service gate report should be exported");
    assert_eq!(gate.service, "data_node");
    assert!(gate.ready);
    assert_eq!(gate.gate_status, "ready");
    assert_eq!(gate.severity, "ready");
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
    assert!(gate.primary_blocker.is_none());
    assert!(gate.failed_capabilities.is_empty());
    assert_eq!(gate.failed_capabilities.len(), data_node.blocker_count);

    let scale_gate = report
        .service_gate_report("scale_testing")
        .expect("scale testing service gate report should be exported");
    assert!(scale_gate.ready);
    assert_eq!(scale_gate.gate_status, "ready");
    assert_eq!(scale_gate.blocker_count, 0);
    assert!(scale_gate.failed_capabilities.is_empty());
}

// shared-corpus: raft_data_node_mixed_rw_and_membership
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

// shared-corpus: feature_policy_filter_aggregate_lifecycle, cpp_redis_live_storage_smoke_parity_surfaces
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
