// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::meta::{MetaEntityState, ServerEndpoint, TableMetaInfo, TableShard};
use crate::sdk::{TemporalStoreTonicAdapter, v1};
use crate::types::{FeatureReducer as R, FeatureRounding as F};
use prost::Message;
use tonic::transport::{server::TcpIncoming, Server};

struct Rpc {
    table: TemporalStoreTable,
    client: v1::temporal_store_service_client::TemporalStoreServiceClient<tonic::transport::Channel>,
    worker: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    _dir: tempfile::TempDir,
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Rpc {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let engine = crate::engine::TemporalEngine::with_local_dirs(
            1 << 20, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
        );
        engine.load_shard(75);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let worker = tokio::spawn(Server::builder()
            .add_service(v1::temporal_store_service_server::TemporalStoreServiceServer::new(
                TemporalStoreTonicAdapter::new(engine),
            ))
            .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()));
        let mut client = v1::temporal_store_service_client::TemporalStoreServiceClient::connect(
            format!("http://{address}"),
        ).await.unwrap();
        let seeded = client.execute(v1::ExecuteRequest {
            shard_id: 75, command: Some(v1::Command {
                kind: Some(v1::command::Kind::FeatureAppend(v1::FeatureAppend {
                    key: "k".to_owned(),
                    points: [(1000, "2"), (2000, "4"), (2500, "abc"), (3000, "4"), (4000, "6")]
                        .into_iter().map(|(timestamp_ms, value)| v1::FeaturePoint {
                            timestamp_ms, value: value.as_bytes().to_vec(),
                        }).collect(),
                })),
            }), ..Default::default()
        }).await.unwrap().into_inner();
        assert!(seeded.status.unwrap().ok);
        let sdk = TemporalStoreClient::with_options(ClientOptions {
            meta_grpc_target: Some("127.0.0.1:1".to_owned()),
            io_timeout_ms: 3000, connect_timeout_ms: 3000,
            ..ClientOptions::proxy("127.0.0.1:1")
        });
        let table = sdk.open_table("feature", "reducers", TableOptions::default());
        sdk.update_grpc_routes("feature/reducers", &TableMetaInfo {
            table_id: 1, namespace: "feature".to_owned(), table_name: "reducers".to_owned(),
            state: MetaEntityState::Normal, topology_version: 1, first_shard_id: 75,
            shard_count: 1, replica_count: 1, partition_version: 0,
            serving_options: Default::default(),
        }, &[TableShard {
            shard_id: 75, start_bucket: 0, end_bucket: 1,
            primary: None, replicas: Vec::new(),
            primary_endpoint: Some(ServerEndpoint {
                server_addr: String::new(), grpc_addr: Some(address), location: String::new(),
            }), replica_endpoints: Vec::new(), load_version: 0, raft_route: None,
        }], false);
        Self { table, client, worker, _dir: dir }
    }

    async fn execute(&self, command: Command) -> ExecuteResponse {
        let table = self.table.clone();
        tokio::task::spawn_blocking(move || table.execute(command).unwrap()).await.unwrap()
    }
}

fn query(reducer: R, rounding: F, scale: u64) -> Command {
    Command::FeatureReduceQuery {
        key: "k".to_owned(), start_ms: 0, end_ms: 4999, reducer, rounding, scale, count: None,
    }
}

#[tokio::test]
async fn every_typed_reducer_crosses_single_rpc_with_exact_parameters_and_numbers() {
    let rpc = Rpc::new().await;
    let cases = [
        (R::Mean, F::Nearest, 1000, 4000),
        (R::Variance { sample: false }, F::Nearest, 1000, 2000),
        (R::Variance { sample: true }, F::Floor, 1000, 2666),
        (R::Variance { sample: true }, F::Ceil, 1000, 2667),
        (R::StdDev { sample: false }, F::Nearest, 1000, 1414),
        (R::StdDev { sample: false }, F::Ceil, 1000, 1415),
        (R::DecayedSum { half_life_ms: 1000, as_of_ms: 4000 }, F::Nearest, 100, 925),
        (R::Ewma { alpha: 0.5 }, F::Nearest, 100, 475),
        (R::Rate, F::Nearest, 1000, 3200),
        (R::Ratio { at_least: 4 }, F::Nearest, 1_000_000, 750_000),
        (R::Ratio { at_least: i64::MIN }, F::Nearest, 1000, 1000),
        (R::Ratio { at_least: i64::MAX }, F::Nearest, 1000, 0),
        (R::Wilson { at_least: 4, z: 1.96 }, F::Nearest, 10_000, 3006),
        (R::Weighted { weights: vec![0.5, 1.0, 2.0] }, F::Nearest, 1, 18),
        (R::Weighted { weights: vec![-0.5, -1.0, -2.0] }, F::Nearest, 1, -18),
    ];
    for (reducer, rounding, scale, expected) in cases {
        let command = query(reducer, rounding, scale);
        assert!(!super::super::commands::is_write(&command));
        assert!(!crate::engine::is_write_command(&command));
        let encoded = encode_command(&command).unwrap().encode_to_vec();
        assert_eq!(&encoded[..2], &[0xc2, 0x05], "typed query uses its own command tag 88");
        assert_eq!(super::super::protobuf_transport::protobuf_command_key(&encoded).unwrap(), ("k", true));
        assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), command);
        let response = rpc.execute(command).await;
        assert!(response.status.ok);
        assert_eq!(response.response, CommandResponse::Aggregate { value: expected });
    }
}

#[tokio::test]
async fn typed_rpc_batch_preserves_errors_caps_empty_windows_and_named_family_results() {
    let rpc = Rpc::new().await;
    let mut capped = query(R::Mean, F::Nearest, 1000);
    if let Command::FeatureReduceQuery { count, .. } = &mut capped { *count = Some(2); }
    let mut empty = query(R::Mean, F::Nearest, 1000);
    if let Command::FeatureReduceQuery { start_ms, end_ms, .. } = &mut empty {
        *start_ms = 10_000; *end_ms = 20_000;
    }
    let mut absent = query(R::Ewma { alpha: 0.5 }, F::Nearest, 1000);
    if let Command::FeatureReduceQuery { key, .. } = &mut absent { *key = "absent".to_owned(); }
    let mut zero_count = query(R::Mean, F::Nearest, 1000);
    if let Command::FeatureReduceQuery { count, .. } = &mut zero_count { *count = Some(0); }
    let named = Command::FeatureAggQuery { key: "k".to_owned(), start_ms: 0, end_ms: 4999, aggregator: "count".to_owned(), count: None };
    let proto = Command::FeatureProtoAggQuery { key: "k".to_owned(), start_ms: 0, end_ms: 4999, aggregator: "count".to_owned(), count: None };
    let commands = vec![capped, query(R::Ewma { alpha: 0.0 }, F::Nearest, 100), empty, named, absent, proto, zero_count];
    let table = rpc.table.clone();
    let batch = tokio::task::spawn_blocking(move || table.batch_execute(commands).unwrap()).await.unwrap();
    assert!(batch.status.ok, "ordinary engine batch keeps per-command failures in their rows");
    assert_eq!(batch.responses.len(), 7);
    assert_eq!(batch.responses[1].status.code, "invalid_argument");
    assert!(batch.responses[1].status.message.contains("alpha"));
    assert_eq!(batch.responses[1].response, CommandResponse::Empty);
    for (index, expected) in [(0, 3000), (2, 0), (3, 5), (4, 0), (5, 5), (6, 0)] {
        assert!(batch.responses[index].status.ok);
        assert_eq!(batch.responses[index].response, CommandResponse::Aggregate { value: expected });
    }
    for (reducer, scale, field) in [
        (R::Mean, 0, "scale"),
        (R::Ewma { alpha: f64::NAN }, 1, "alpha"),
        (R::DecayedSum { half_life_ms: 0, as_of_ms: 4000 }, 1, "half_life_ms"),
        (R::Wilson { at_least: 4, z: f64::INFINITY }, 1, "z"),
        (R::Weighted { weights: vec![1.0; 65] }, 1, "weights"),
    ] {
        let response = rpc.execute(query(reducer, F::Nearest, scale)).await;
        assert_eq!(response.status.code, "invalid_argument");
        assert!(response.status.message.contains(field));
    }
}

#[tokio::test]
async fn serialized_typed_rpc_refuses_unknown_tags_and_preserves_default_presence() {
    let mut rpc = Rpc::new().await;
    let valid = v1::FeatureReduceQuery {
        key: "k".to_owned(), start_ms: 0, end_ms: 4999,
        reducer: Some(v1::feature_reduce_query::Reducer::Mean(v1::FeatureReduceUnit {})),
        ..Default::default()
    };
    let command = |query| v1::Command { kind: Some(v1::command::Kind::FeatureReduceQuery(query)) };
    let request = |command| v1::ExecuteRequest { shard_id: 75, command: Some(command), ..Default::default() };
    let reply = rpc.client.execute(request(command(valid.clone()))).await.unwrap().into_inner();
    assert!(reply.status.unwrap().ok);
    assert_eq!(reply.response.unwrap().integer, Some(4));
    let malformed = [
        v1::FeatureReduceQuery { reducer: None, ..valid.clone() },
        v1::FeatureReduceQuery { rounding: 99, ..valid.clone() },
    ];
    for bad in malformed {
        let error = rpc.client.execute(request(command(bad.clone()))).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        let error = rpc.client.batch_execute(v1::BatchExecuteRequest {
            shard_id: 75, commands: vec![command(valid.clone()), command(bad)], ..Default::default()
        }).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    // Unknown nested oneof alternative must not become Mean: remove its known tag first.
    let mut missing = valid.clone(); missing.reducer = None;
    let mut unknown = missing.encode_to_vec();
    unknown.extend_from_slice(&[0x82, 0x01, 0x00]);
    let decoded = v1::FeatureReduceQuery::decode(unknown.as_slice()).unwrap();
    assert!(decoded.reducer.is_none());
    let error = rpc.client.execute(request(command(decoded))).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let reply = rpc.client.execute(request(command(valid))).await.unwrap().into_inner();
    assert_eq!(reply.response.unwrap().integer, Some(4));
    let wide = Command::FeatureReduceQuery {
        key: "absent".to_owned(), start_ms: u64::MAX - 1, end_ms: u64::MAX,
        reducer: R::DecayedSum { half_life_ms: u64::MAX, as_of_ms: u64::MAX },
        rounding: F::Floor, scale: u64::MAX, count: Some(usize::MAX),
    };
    let encoded = encode_command(&wide).unwrap().encode_to_vec();
    assert_eq!(crate::sdk::sdk_command_to_types(v1::Command::decode(encoded.as_slice()).unwrap()).unwrap(), wide);
    let reply = rpc.execute(wide).await;
    assert_eq!(reply.response, CommandResponse::Aggregate { value: 0 });
    let empty = rpc.client.batch_execute(v1::BatchExecuteRequest {
        shard_id: 75, ..Default::default()
    }).await.unwrap().into_inner();
    assert!(empty.status.unwrap().ok);
    assert!(empty.responses.is_empty());
}
