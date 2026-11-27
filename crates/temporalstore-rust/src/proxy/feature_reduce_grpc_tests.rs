// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::meta::{MetaEntityState, ServerEndpoint, TableMetaInfo, TableShard, TableTopologyResponse};
use crate::sdk::{TemporalStoreSdkExecutor, TemporalStoreTonicAdapter};
use crate::types::{FeatureReducer, FeatureRounding};

struct CheckedEngine(crate::engine::TemporalEngine);

impl TemporalStoreSdkExecutor for CheckedEngine {
    fn execute_sdk(&self, _request: ExecuteRequest) -> ExecuteResponse {
        panic!("proxy must forward the metadata generation on the checked endpoint");
    }

    fn execute_checked_sdk(&self, request: ExecuteRequest, load_version: u64) -> ExecuteResponse {
        assert_eq!((request.shard_id, load_version), (75, 17));
        self.0.execute(request)
    }

    fn batch_execute_checked_sdk(&self, request: BatchExecuteRequest, load_version: u64) -> BatchExecuteResponse {
        assert_eq!((request.shard_id, load_version), (75, 17));
        self.0.batch_execute(request)
    }
}

#[tokio::test]
async fn readonly_proxy_forwards_typed_single_and_batch_rpc_without_losing_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = crate::engine::TemporalEngine::with_local_dirs(
        1 << 20, dir.path().join("cache"), dir.path().join("blocks"), dir.path().join("indexes"),
    );
    engine.load_shard(75);
    let datanode = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = datanode.local_addr().unwrap().to_string();
    let backend = tokio::spawn(Server::builder()
        .add_service(v1::temporal_store_service_server::TemporalStoreServiceServer::new(
            TemporalStoreTonicAdapter::new(CheckedEngine(engine)),
        ))
        .serve_with_incoming(TcpIncoming::from_listener(datanode, true, None).unwrap()));
    let mut seed = v1::temporal_store_service_client::TemporalStoreServiceClient::connect(format!("http://{address}")).await.unwrap();
    let seeded = seed.execute_checked(v1::ExecuteRequest {
        shard_id: 75, load_version: Some(17),
        command: Some(v1::Command { kind: Some(v1::command::Kind::FeatureAppend(v1::FeatureAppend {
            key: "k".to_owned(),
            points: [(1000, "2"), (2000, "4"), (2500, "abc"), (3000, "4"), (4000, "6")]
                .into_iter().map(|(timestamp_ms, value)| v1::FeaturePoint {
                    timestamp_ms, value: value.as_bytes().to_vec(),
                }).collect(),
        })) }), ..Default::default()
    }).await.unwrap().into_inner();
    assert!(seeded.status.unwrap().ok);
    let meta = TcpListener::bind("127.0.0.1:0").unwrap();
    let meta_addr = meta.local_addr().unwrap().to_string();
    let endpoint = ServerEndpoint { server_addr: address.clone(), grpc_addr: Some(address), location: String::new() };
    // Use the same real HTTP metadata fixture as the existing proxy stream test.
    thread::spawn(move || crate::http::serve_listener(meta, move |request| {
        match request.path.as_str() {
            "/tables/topology" => crate::http::json_response(200, &TableTopologyResponse {
                status: Status::ok(),
                table: Some(TableMetaInfo {
                    table_id: 1, namespace: "feature".to_owned(), table_name: "reducers".to_owned(),
                    state: MetaEntityState::Normal, topology_version: 3,
                    first_shard_id: 75, shard_count: 1, replica_count: 1,
                    partition_version: 0, serving_options: Default::default(),
                }),
                shards: vec![TableShard {
                    shard_id: 75, start_bucket: 0, end_bucket: 1, primary: None, replicas: Vec::new(),
                    primary_endpoint: Some(endpoint.clone()), replica_endpoints: Vec::new(),
                    load_version: 17, raft_route: None,
                }], unchanged: false,
            }),
            "/meta/topology_version" => crate::http::json_response(200, &serde_json::json!({
                "status": Status::ok(), "current_topology_version": 3, "old_topology_version": 0,
                "unchanged": false, "server_count": 1, "proxy_count": 0, "table_count": 1,
                "shard_route_count": 1, "normal_servers": 1, "frozen_servers": 0, "dropped_servers": 0,
                "normal_proxies": 0, "frozen_proxies": 0, "dropped_proxies": 0,
                "normal_tables": 1, "frozen_tables": 0, "dropped_tables": 0, "changed_tables": [],
            })),
            _ => crate::http::json_response(404, &Status::error("not_found", request.path)),
        }
    }).unwrap());
    let proxy = ProxyService::new(ProxyOptions {
        meta_addr, serving_mode: crate::proxy::ProxyServingMode::Readonly,
        ingestion_account: "feature".to_owned(), enforce_ingestion_account: true,
        io_timeout_ms: 3000, connect_timeout_ms: 3000, ..Default::default()
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let relay = tokio::spawn(Server::builder().add_service(relay_service(proxy))
        .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()));
    let mut client = v1::proxy_service_client::ProxyServiceClient::connect(format!("http://{address}")).await.unwrap();
    let query = |reducer| crate::shared_store::command_to_sdk_proto(&Command::FeatureReduceQuery {
        key: "k".to_owned(), start_ms: 0, end_ms: 4999, reducer,
        rounding: FeatureRounding::Nearest, scale: 1000, count: None,
    }).unwrap();
    let single = v1::ProxyStreamRequest {
        namespace_name: "feature".to_owned(), table_name: "reducers".to_owned(),
        kind: Some(v1::proxy_stream_request::Kind::Execute(v1::ExecuteRequest {
            shard_id: 999, load_version: Some(999), command: Some(query(FeatureReducer::Mean)),
            ..Default::default()
        })),
    };
    let batch = v1::ProxyStreamRequest {
        namespace_name: "feature".to_owned(), table_name: "reducers".to_owned(),
        kind: Some(v1::proxy_stream_request::Kind::BatchExecute(v1::BatchExecuteRequest {
            commands: vec![query(FeatureReducer::Variance { sample: false }),
                query(FeatureReducer::Ewma { alpha: 0.0 }), query(FeatureReducer::Mean)],
            ..Default::default()
        })),
    };
    let write = v1::ProxyStreamRequest {
        namespace_name: "feature".to_owned(), table_name: "reducers".to_owned(),
        kind: Some(v1::proxy_stream_request::Kind::Execute(v1::ExecuteRequest {
            command: Some(v1::Command { kind: Some(v1::command::Kind::StringSet(v1::StringSet {
                key: "k".to_owned(), value: b"must not be written".to_vec(), ttl_ms: 0,
            })) }), ..Default::default()
        })),
    };
    let mut replies = client.proxy_execute_stream(tonic::codegen::tokio_stream::iter([single, batch, write]))
        .await.unwrap().into_inner();
    let first = replies.message().await.unwrap().unwrap();
    assert!(first.status.unwrap().ok);
    let first = first.execute.unwrap().response.unwrap();
    assert_eq!(first.result_kind, v1::CommandResultKind::Aggregate as i32);
    assert_eq!(first.integer, Some(4000));
    let second = replies.message().await.unwrap().unwrap();
    assert!(second.status.unwrap().ok);
    let second = second.batch_execute.unwrap();
    assert_eq!(second.responses.len(), 3);
    assert!(second.responses[0].status.as_ref().unwrap().ok);
    assert_eq!(second.responses[0].result_kind, v1::CommandResultKind::Aggregate as i32);
    assert_eq!(second.responses[0].integer, Some(2000));
    assert_eq!(second.responses[1].status.as_ref().unwrap().code, "invalid_argument");
    assert!(second.responses[1].status.as_ref().unwrap().message.contains("alpha"));
    assert_eq!(second.responses[2].integer, Some(4000));
    assert!(second.responses[2].status.as_ref().unwrap().ok);
    let last = replies.message().await.unwrap().unwrap();
    let last_status = last.status.unwrap();
    assert!(!last_status.ok, "readonly proxy must still refuse writes");
    assert_eq!(last_status.code, "proxy_write_disabled");
    assert!(last.execute.is_none());
    assert!(replies.message().await.unwrap().is_none());
    relay.abort(); backend.abort();
}
