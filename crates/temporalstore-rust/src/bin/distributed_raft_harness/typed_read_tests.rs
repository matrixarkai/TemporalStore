// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use temporalstore_rust::FeaturePoint;
use temporalstore_rust::types::{FeatureReducer, FeatureRounding};

#[test]
fn distributed_handler_serialized_typed_reads_keep_engine_status() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
        snapshot_check_interval_ms: 30_000, engine: ProductionRaftEngineKind::TemporalRaft,
        shard_id: 1, local_node_id: 1,
        nodes: vec![ProductionRaftNode { node_id: 1, addr: "127.0.0.1:1".to_owned() }],
        wal_dir: dir.path().display().to_string(), config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions::default(),
        security: temporalstore_rust::ProductionRaftSecurity::plaintext_for_local_chaos("test-token"),
        heartbeat_interval_ms: 100, election_tick_ms: 50,
        max_catchup_entries_per_heartbeat: 256, allow_plaintext_for_local_chaos: true,
    }).unwrap();
    runtime.propose(Command::FeatureAppend { key: "k".to_owned(), points: vec![
        FeaturePoint { timestamp_ms: 1000, value: b"2".to_vec() },
        FeaturePoint { timestamp_ms: 2000, value: b"4".to_vec() },
    ] }).unwrap();
    let commit = runtime.status().commit_index;
    assert!(commit > 0);
    let before = runtime.cluster().read_safety_runtime_state();
    let read = |node_id, reducer, scale| {
        let command = Command::FeatureReduceQuery { key: "k".to_owned(), start_ms: 0, end_ms: 4999,
            reducer, rounding: FeatureRounding::Nearest, scale, count: None };
        let request = HttpRequest { method: "POST".to_owned(), path: "/raft/read".to_owned(),
            body: serde_json::to_vec(&DistributedRaftReadRequest { node_id, command }).unwrap() };
        let (code, body) = handle(&runtime, request);
        assert_eq!(code, 200);
        serde_json::from_slice::<DistributedRaftCommandResponse>(&body).unwrap()
    };
    for (reducer, scale, field) in [(FeatureReducer::Mean, 0, "scale"),
        (FeatureReducer::Ewma { alpha: 0.0 }, 1000, "alpha")] {
        let before = read(1, FeatureReducer::Mean, 1000);
        assert!(before.status.ok);
        assert_eq!(before.response, CommandResponse::Aggregate { value: 3000 });
        let response = read(1, reducer, scale);
        assert!(!response.status.ok);
        assert_eq!(response.status.code, "invalid_argument");
        assert!(response.status.message.contains(field));
        assert_eq!(response.response, CommandResponse::Empty);
        let after = read(1, FeatureReducer::Mean, 1000);
        assert!(after.status.ok);
        assert_eq!(after.response, CommandResponse::Aggregate { value: 3000 });
    }
    let gated = read(99, FeatureReducer::Mean, 0);
    assert!(!gated.status.ok);
    assert_eq!(gated.status.code, "raft_error");
    assert_eq!(gated.response, CommandResponse::Empty);
    let after = runtime.cluster().read_safety_runtime_state();
    assert_eq!(after.read_index_requests, before.read_index_requests + 7);
    assert_eq!(after.read_index_accepted, before.read_index_accepted + 6);
    assert_eq!(runtime.status().commit_index, commit);
}

#[test]
fn distributed_proposal_handler_refuses_reads_before_any_admission() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ProductionRaftRuntime::start(ProductionRaftRuntimeOptions {
        snapshot_check_interval_ms: 30_000, engine: ProductionRaftEngineKind::TemporalRaft,
        shard_id: 1, local_node_id: 1,
        nodes: vec![ProductionRaftNode { node_id: 1, addr: "127.0.0.1:1".to_owned() }],
        wal_dir: dir.path().display().to_string(), config: RaftConfig::default(),
        rpc: RaftRpcRuntimeOptions::default(),
        security: temporalstore_rust::ProductionRaftSecurity::plaintext_for_local_chaos("test-token"),
        heartbeat_interval_ms: 100, election_tick_ms: 50,
        max_catchup_entries_per_heartbeat: 256, allow_plaintext_for_local_chaos: true,
    }).unwrap();
    let before_commit = runtime.status().commit_index;
    let before_reads = runtime.cluster().read_safety_runtime_state().read_index_requests;
    for scale in [1000, 0] {
        let command = Command::FeatureReduceQuery { key: "k".to_owned(), start_ms: 0, end_ms: 4999,
            reducer: FeatureReducer::Mean, rounding: FeatureRounding::Nearest, scale, count: None };
        let (code, bytes) = handle(&runtime, HttpRequest {
            method: "POST".to_owned(), path: "/raft/propose".to_owned(),
            body: serde_json::to_vec(&DistributedRaftProposeRequest { command }).unwrap(),
        });
        assert_eq!(code, 200);
        let response: DistributedRaftCommandResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(!response.status.ok);
        assert_eq!(response.status.code, "invalid_argument");
        assert!(response.status.message.contains("write commands only"));
        assert_eq!(response.response, CommandResponse::Empty);
        assert_eq!(runtime.status().commit_index, before_commit);
        assert_eq!(runtime.cluster().read_safety_runtime_state().read_index_requests, before_reads);
    }
    let write = Command::StringSet { key: "k".to_owned(), value: b"write still admitted".to_vec() };
    let (code, bytes) = handle(&runtime, HttpRequest { method: "POST".to_owned(), path: "/raft/propose".to_owned(),
        body: serde_json::to_vec(&DistributedRaftProposeRequest { command: write }).unwrap() });
    assert_eq!(code, 200);
    let response: DistributedRaftCommandResponse = serde_json::from_slice(&bytes).unwrap();
    assert!(response.status.ok);
    assert!(runtime.status().commit_index > before_commit);
}
