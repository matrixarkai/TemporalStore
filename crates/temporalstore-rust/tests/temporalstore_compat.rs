use std::sync::{Arc, Mutex};

use temporalstore_rust::types::{
    FeatureFilter, FeatureFilterOp, FeatureWritePolicy, SequenceFeatureRow, SequenceQuerySpec,
};
use temporalstore_rust::{
    Command, CommandResponse, Config, EndToEndWorkflow, ExecuteRequest, FeaturePoint,
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
