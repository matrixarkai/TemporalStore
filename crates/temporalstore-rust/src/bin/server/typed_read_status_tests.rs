// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use temporalstore_rust::{
    DataNodeRuntimeOptions, LoadShardRequest, ProductionRaftEngineKind, ProductionRaftNode,
    ProductionRaftRuntime, ProductionRaftRuntimeOptions, ProductionRaftSecurity, RaftCluster,
    RaftConfig, RaftRpcRuntimeOptions, TemporalEngine,
};
use temporalstore_rust::sdk::v1;

struct Rpc {
    client: v1::temporal_store_service_client::TemporalStoreServiceClient<tonic::transport::Channel>,
    cluster: RaftCluster,
    worker: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    _dir: tempfile::TempDir,
}

impl Drop for Rpc {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl Rpc {
    async fn new(local_node_id: u64, mode: DataRaftReadMode, seed: bool) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let voters = if local_node_id == 1 { vec![1] } else { vec![1, 2, 3] };
        let nodes = voters.into_iter().map(|node_id| {
            let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
            ProductionRaftNode { node_id, addr: reserved.local_addr().unwrap().to_string() }
        }).collect();
        // Same production runtime setup as the server's existing Raft tests. No unsafe
        // read policy bypasses ReadIndex; the leader is seeded through RPC.
        let engine_root = dir.path().join("replica");
        let factory_generation = std::sync::atomic::AtomicU64::new(0);
        let raft_runtime = ProductionRaftRuntime::start_with_local_engine_factory(ProductionRaftRuntimeOptions {
            snapshot_check_interval_ms: 30_000,
            engine: ProductionRaftEngineKind::TemporalRaft, shard_id: 1, local_node_id, nodes,
            wal_dir: dir.path().join("raft").display().to_string(),
            config: RaftConfig {
                enable_pre_vote: true, lease_duration_ms: 1000,
                max_segment_bytes: 512, min_keep_segment_num: 1, ..RaftConfig::default()
            },
            rpc: RaftRpcRuntimeOptions { max_retries: 1, deadline_ms: 100, ..Default::default() },
            security: ProductionRaftSecurity::plaintext_for_local_chaos("test-token"),
            heartbeat_interval_ms: 20, election_tick_ms: 10,
            max_catchup_entries_per_heartbeat: 32, allow_plaintext_for_local_chaos: true,
        }, move || {
            let generation = factory_generation.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let root = engine_root.join(generation.to_string());
            Ok(TemporalEngine::with_local_dirs(
                1 << 20, root.join("cache"), root.join("blocks"), root.join("indexes"),
            ))
        }).unwrap();
        let cluster = raft_runtime.cluster();
        // Use the published local Raft engine for the actual lifecycle generation guard.
        let runtime = DataNodeRuntime::new_with_lifecycle_snapshot_path(
            cluster.local_replica_engine(1).expect("published data replica"),
            DataNodeRuntimeOptions::default(), dir.path().join("lifecycle.json"),
        );
        // Raft restore already loaded this owner. Normal reload updates its metadata
        // without replacing the published state or inventing a second engine.
        let loaded = runtime.reload_shard_with(LoadShardRequest {
            shard_id: 1, load_version: 17, local_node_id: Some(local_node_id),
            shard_uri: "local://feature/1".to_owned(), start_routing_bucket: 0,
            end_routing_bucket: 4095, readonly: false, table_name: "feature".to_owned(),
        });
        assert!(loaded.status.ok, "{loaded:?}");
        let raft = ServerRaftState {
            runtime: raft_runtime, local_node_id, raft_shard_id: 1,
            read_policy: temporalstore_rust::DataRaftReadPolicy {
                mode, read_index_timeout_ms: 100, ..Default::default()
            },
            local_admin_enabled: false, blocked_peers: Arc::new(Mutex::new(BTreeSet::new())),
        };
        let executor = DataNodeExecutor { runtime, raft: Some(raft) };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let worker = tokio::spawn(Server::builder()
            .add_service(TemporalStoreServiceServer::new(TemporalStoreTonicAdapter::new(executor)))
            .serve_with_incoming(TcpIncoming::from_listener(listener, true, None).unwrap()));
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap().connect_timeout(std::time::Duration::from_secs(3))
            .timeout(std::time::Duration::from_secs(5)).connect().await.unwrap();
        let client = v1::temporal_store_service_client::TemporalStoreServiceClient::new(channel);
        let mut fixture = Self { client, cluster, worker, _dir: dir };
        if seed {
            let response = fixture.client.execute_checked(v1::ExecuteRequest {
                shard_id: 1, load_version: Some(17),
                command: Some(v1::Command { kind: Some(v1::command::Kind::FeatureAppend(v1::FeatureAppend {
                    key: "k".to_owned(), points: vec![
                        v1::FeaturePoint { timestamp_ms: 1000, value: b"2".to_vec() },
                        v1::FeaturePoint { timestamp_ms: 2000, value: b"4".to_vec() },
                    ],
                })) }), ..Default::default()
            }).await.unwrap().into_inner();
            assert!(response.status.as_ref().unwrap().ok, "{response:?}");
            assert!(fixture.cluster.status().commit_index > 0);
        }
        fixture
    }
}

fn mean(scale: u64) -> v1::Command {
    query(v1::feature_reduce_query::Reducer::Mean(v1::FeatureReduceUnit {}), scale)
}

fn query(reducer: v1::feature_reduce_query::Reducer, scale: u64) -> v1::Command {
    v1::Command { kind: Some(v1::command::Kind::FeatureReduceQuery(v1::FeatureReduceQuery {
        key: "k".to_owned(), start_ms: 0, end_ms: 4999,
        reducer: Some(reducer), scale: Some(scale), ..Default::default()
    })) }
}

fn invalid(response: &v1::CommandResponse, field: &str) {
    let status = response.status.as_ref().unwrap();
    assert!(!status.ok, "validation error must not become success plus Empty: {response:?}");
    assert_eq!(status.code, "invalid_argument");
    assert!(status.message.contains(field), "{status:?}");
    assert_eq!(response.result_kind, v1::CommandResultKind::Empty as i32);
    assert!(response.integer.is_none());
}

fn invalid_single(response: &v1::ExecuteResponse, field: &str) {
    // Single RPC errors live in ExecuteResponse.status; the nested empty payload
    // does not independently carry that status. Batch errors live in each row.
    let status = response.status.as_ref().unwrap();
    assert!(!status.ok, "validation error must not become success plus Empty: {response:?}");
    assert_eq!(status.code, "invalid_argument");
    assert!(status.message.contains(field), "{status:?}");
    let payload = response.response.as_ref().unwrap();
    assert_eq!(payload.result_kind, v1::CommandResultKind::Empty as i32);
    assert!(payload.integer.is_none());
}

#[tokio::test]
async fn raft_executor_single_rpc_preserves_scale_and_alpha_validation_status() {
    let mut rpc = Rpc::new(1, DataRaftReadMode::Linearizable, true).await;
    let commit = rpc.cluster.status().commit_index;
    for (command, field) in [(mean(0), "scale"),
        (query(v1::feature_reduce_query::Reducer::Ewma(0.0), 1000), "alpha")] {
        let before = rpc.cluster.read_safety_runtime_state();
        let response = rpc.client.execute(v1::ExecuteRequest {
            shard_id: 1, command: Some(command), ..Default::default()
        }).await.unwrap().into_inner();
        invalid_single(&response, field);
        let after = rpc.cluster.read_safety_runtime_state();
        assert_eq!(after.read_index_requests, before.read_index_requests + 1);
        assert_eq!(after.read_index_accepted, before.read_index_accepted + 1);
        assert_eq!(rpc.cluster.status().commit_index, commit);
    }
    let response = rpc.client.execute_checked(v1::ExecuteRequest {
        shard_id: 1, load_version: Some(17), command: Some(mean(1000)), ..Default::default()
    }).await.unwrap().into_inner();
    assert!(response.status.unwrap().ok);
    let row = response.response.unwrap();
    assert_eq!(row.result_kind, v1::CommandResultKind::Aggregate as i32);
    assert_eq!(row.integer, Some(3000));
    assert_eq!(rpc.cluster.status().commit_index, commit);
}

#[tokio::test]
async fn raft_executor_all_read_batch_rpc_keeps_valid_invalid_valid_rows() {
    let mut rpc = Rpc::new(1, DataRaftReadMode::Linearizable, true).await;
    let commit = rpc.cluster.status().commit_index;
    for (middle, field) in [(mean(0), "scale"),
        (query(v1::feature_reduce_query::Reducer::Ewma(0.0), 1000), "alpha")] {
        let before = rpc.cluster.read_safety_runtime_state();
        let response = rpc.client.batch_execute_checked(v1::BatchExecuteRequest {
            shard_id: 1, load_version: Some(17), commands: vec![mean(1000), middle, mean(1000)],
            ..Default::default()
        }).await.unwrap().into_inner();
        assert!(response.status.unwrap().ok, "per-command refusal is not a Raft gate error");
        assert_eq!(response.responses.len(), 3);
        invalid(&response.responses[1], field);
        for index in [0, 2] {
            assert!(response.responses[index].status.as_ref().unwrap().ok);
            assert_eq!(response.responses[index].result_kind, v1::CommandResultKind::Aggregate as i32);
            assert_eq!(response.responses[index].integer, Some(3000));
        }
        let after = rpc.cluster.read_safety_runtime_state();
        assert_eq!(after.read_index_requests, before.read_index_requests + 1);
        assert_eq!(after.read_index_accepted, before.read_index_accepted + 1);
        assert_eq!(rpc.cluster.status().commit_index, commit);
    }
}

#[tokio::test]
async fn raft_executor_rpc_retains_generation_and_primary_read_rejections() {
    let mut leader = Rpc::new(1, DataRaftReadMode::Linearizable, true).await;
    let before = leader.cluster.read_safety_runtime_state().read_index_requests;
    let response = leader.client.execute_checked(v1::ExecuteRequest {
        shard_id: 1, load_version: Some(16), command: Some(mean(0)), ..Default::default()
    }).await.unwrap().into_inner();
    assert_eq!(response.status.unwrap().code, "load_version_mismatch");
    let response = leader.client.batch_execute_checked(v1::BatchExecuteRequest {
        shard_id: 1, load_version: Some(16), commands: vec![mean(1000), mean(0), mean(1000)],
        ..Default::default()
    }).await.unwrap().into_inner();
    assert_eq!(response.status.unwrap().code, "load_version_mismatch");
    assert!(response.responses.is_empty());
    let response = leader.client.execute(v1::ExecuteRequest {
        shard_id: 999, command: Some(mean(0)), ..Default::default()
    }).await.unwrap().into_inner();
    assert_eq!(response.status.unwrap().code, "shard_mismatch");
    assert_eq!(leader.cluster.read_safety_runtime_state().read_index_requests, before);
    let response = leader.client.execute_checked(v1::ExecuteRequest {
        shard_id: 1, load_version: Some(17), command: Some(mean(0)), ..Default::default()
    }).await.unwrap().into_inner();
    invalid_single(&response, "scale");
    for mode in [DataRaftReadMode::Leader, DataRaftReadMode::Linearizable] {
        let mut follower = Rpc::new(2, mode, false).await;
        assert_eq!(follower.cluster.leader_id(), 1);
        assert!(!follower.cluster.is_local_leader(2));
        let before = follower.cluster.read_safety_runtime_state().read_index_requests;
        let response = follower.client.execute_checked(v1::ExecuteRequest {
            shard_id: 1, load_version: Some(17), command: Some(mean(0)), ..Default::default()
        }).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, "raft_not_leader");
        assert_eq!(response.response.unwrap().result_kind, v1::CommandResultKind::Empty as i32);
        let response = follower.client.batch_execute_checked(v1::BatchExecuteRequest {
            shard_id: 1, load_version: Some(17), commands: vec![mean(1000), mean(0), mean(1000)],
            ..Default::default()
        }).await.unwrap().into_inner();
        assert_eq!(response.status.unwrap().code, "raft_not_leader");
        assert_eq!(response.responses.len(), 3);
        for row in response.responses {
            assert_eq!(row.status.unwrap().code, "raft_not_leader");
            assert_eq!(row.result_kind, v1::CommandResultKind::Empty as i32);
        }
        assert_eq!(follower.cluster.read_safety_runtime_state().read_index_requests, before);
    }
}
