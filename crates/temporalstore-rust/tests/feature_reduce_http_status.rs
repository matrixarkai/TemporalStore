// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::fs::{self, File};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::time::{Duration, Instant};
use serde::{de::DeserializeOwned, Serialize};
use temporalstore_rust::http::{get_json_with_options, post_json_with_options, HttpRequestOptions};
use temporalstore_rust::{
    BatchExecuteRequest, BatchExecuteResponse, Command, CommandResponse, DistributedRaftCommandResponse, DistributedRaftProposeRequest,
    DistributedRaftReadRequest, ExecuteRequest, ExecuteResponse, FeaturePoint, Status,
};
use temporalstore_rust::types::{FeatureReducer, FeatureRounding};

struct Server {
    child: Child,
    dir: tempfile::TempDir,
    addr: String,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn options() -> HttpRequestOptions {
    HttpRequestOptions { connect_timeout_ms: 500, io_timeout_ms: 3000, max_retries: 0 }
}

impl Server {
    fn start(binary: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = reserved.local_addr().unwrap().to_string();
        drop(reserved);
        let log = File::create(dir.path().join("server.log")).unwrap();
        let mut process = ProcessCommand::new(binary);
        for (name, _) in std::env::vars_os() {
            if name.to_string_lossy().starts_with("TS_") { process.env_remove(name); }
        }
        process.current_dir(dir.path())
            .env("TS_STANDALONE", "1").env("TS_META_ADDR", "none")
            .env("TS_STORAGE_BACKEND", "raft").env("TS_SERVER_RAFT", "1")
            .env("TS_SERVER_BIND_ADDR", &addr).env("TS_SERVER_ADVERTISE_ADDR", &addr)
            .env("TS_RAFT_BIND_ADDR", &addr).env("TS_RAFT_NODES", format!("1={addr}"))
            .env("TS_RAFT_NODE_ID", "1").env("TS_SERVER_NODE_ID", "1")
            .env("TS_SHARD_ID", "1").env("TS_RAFT_SHARD_ID", "1")
            .env("TS_DATA_RAFT_READ_MODE", "linearizable")
            .env("TS_RAFT_ALLOW_PLAINTEXT", "1").env("TS_RAFT_AUTH_TOKEN", "test-token")
            .env("TS_RAFT_WAL_DIR", dir.path().join("raft"))
            .env("TS_CACHE_MEMORY_BYTES", "1048576").env("TS_CACHE_DISK_TIER", "0")
            .env("TS_CACHE_DIR", dir.path().join("cache"))
            .env("TS_BLOCK_STORE_DIR", dir.path().join("blocks"))
            .env("TS_INDEX_DIR", dir.path().join("indexes"))
            .stdout(Stdio::from(log.try_clone().unwrap())).stderr(Stdio::from(log));
        let child = process.spawn().expect("spawn exact Cargo-built production binary");
        let mut server = Self { child, dir, addr };
        let deadline = Instant::now() + Duration::from_secs(20);
        let socket: SocketAddr = server.addr.parse().unwrap();
        loop {
            if let Some(status) = server.child.try_wait().unwrap() {
                panic!("server exited {status}: {}", server.log());
            }
            if TcpStream::connect_timeout(&socket, Duration::from_millis(100)).is_ok() { break; }
            assert!(Instant::now() < deadline, "server did not listen: {}", server.log());
            std::thread::sleep(Duration::from_millis(20));
        }
        server
    }

    fn log(&self) -> String {
        fs::read_to_string(self.dir.path().join("server.log")).unwrap_or_default()
            .chars().take(65536).collect()
    }

    fn post<Q: Serialize, R: DeserializeOwned>(&self, path: &str, query: &Q) -> R {
        post_json_with_options(&self.addr, path, query, options())
            .unwrap_or_else(|error| panic!("{path}: {error}; {}", self.log()))
    }

    fn commit(&self) -> u64 {
        let status: serde_json::Value = get_json_with_options(&self.addr, "/raft/status", options()).unwrap();
        status["commit_index"].as_u64().expect("actual Raft commit index")
    }
}

fn seed() -> Command {
    Command::FeatureAppend { key: "k".to_owned(), points: vec![
        FeaturePoint { timestamp_ms: 1000, value: b"2".to_vec() },
        FeaturePoint { timestamp_ms: 2000, value: b"4".to_vec() },
    ] }
}

fn typed(reducer: FeatureReducer, scale: u64) -> Command {
    Command::FeatureReduceQuery { key: "k".to_owned(), start_ms: 0, end_ms: 4999,
        reducer, rounding: FeatureRounding::Nearest, scale, count: None }
}

fn refused(status: &Status, response: &CommandResponse, field: &str) {
    assert!(!status.ok, "semantic refusal must not be successful Empty");
    assert_eq!(status.code, "invalid_argument");
    assert!(status.message.contains(field), "{status:?}");
    assert_eq!(*response, CommandResponse::Empty);
}

fn read(server: &Server, path: &str, command: Command) -> ExecuteResponse {
    match path {
        "/execute" => server.post(path, &ExecuteRequest { shard_id: 1, command }),
        "/raft/read" => {
            let result: DistributedRaftCommandResponse = server.post(path, &DistributedRaftReadRequest { node_id: 1, command });
            ExecuteResponse { status: result.status, response: result.response }
        }
        _ => panic!("unexpected test route"),
    }
}

fn valid(status: &Status, response: &CommandResponse) {
    assert!(status.ok, "{status:?}");
    assert_eq!(*response, CommandResponse::Aggregate { value: 3000 });
}

fn triplet(server: &Server, path: &str, command: Command, field: &str) {
    let before = read(server, path, typed(FeatureReducer::Mean, 1000));
    valid(&before.status, &before.response);
    let bad = read(server, path, command);
    refused(&bad.status, &bad.response, field);
    let after = read(server, path, typed(FeatureReducer::Mean, 1000));
    valid(&after.status, &after.response);
}

#[test]
fn production_datanode_http_and_distributed_reads_preserve_typed_errors() {
    let server = Server::start(env!("CARGO_BIN_EXE_matrixark_rust_datanode"));
    let written: ExecuteResponse = server.post("/execute", &ExecuteRequest { shard_id: 1, command: seed() });
    assert!(written.status.ok, "{written:?}");
    let commit = server.commit();
    assert!(commit > 0);
    for path in ["/execute", "/raft/read"] {
        for (command, field) in [(typed(FeatureReducer::Mean, 0), "scale"),
            (typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), "alpha")] {
            triplet(&server, path, command, field);
        }
    }
    let gated: DistributedRaftCommandResponse = server.post("/raft/read", &DistributedRaftReadRequest {
        node_id: 99, command: typed(FeatureReducer::Mean, 0),
    });
    assert!(!gated.status.ok);
    assert_eq!(gated.status.code, "raft_error");
    assert_eq!(gated.response, CommandResponse::Empty);
    assert_eq!(server.commit(), commit, "queries must not become proposals");
}

#[test]
fn production_raft_node_http_read_preserves_typed_errors_and_read_gates() {
    let server = Server::start(env!("CARGO_BIN_EXE_raft_node"));
    let written: DistributedRaftCommandResponse = server.post("/raft/propose", &DistributedRaftProposeRequest { command: seed() });
    assert!(written.status.ok, "{written:?}");
    let commit = server.commit();
    assert!(commit > 0);
    for (command, field) in [(typed(FeatureReducer::Mean, 0), "scale"),
        (typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), "alpha")] {
        triplet(&server, "/raft/read", command, field);
    }
    let gated: DistributedRaftCommandResponse = server.post("/raft/read", &DistributedRaftReadRequest {
        node_id: 99, command: typed(FeatureReducer::Mean, 0),
    });
    assert!(!gated.status.ok);
    assert_eq!(gated.status.code, "raft_error");
    assert_eq!(gated.response, CommandResponse::Empty);
    assert_eq!(server.commit(), commit);
}

#[test]
fn production_datanode_http_batch_keeps_valid_invalid_valid_rows() {
    let server = Server::start(env!("CARGO_BIN_EXE_matrixark_rust_datanode"));
    // This existing route executes against DataNodeRuntime. Seed through that same
    // production route, rather than substituting a direct engine or another protocol.
    let seeded: BatchExecuteResponse = server.post("/batch_execute", &BatchExecuteRequest {
        shard_id: 1, commands: vec![seed()],
    });
    assert!(seeded.status.ok);
    assert_eq!(seeded.responses.len(), 1);
    assert!(seeded.responses[0].status.ok);
    for (command, field) in [(typed(FeatureReducer::Mean, 0), "scale"),
        (typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), "alpha")] {
        let batch: BatchExecuteResponse = server.post("/batch_execute", &BatchExecuteRequest {
            shard_id: 1, commands: vec![typed(FeatureReducer::Mean, 1000), command, typed(FeatureReducer::Mean, 1000)],
        });
        assert!(batch.status.ok, "semantic failures remain per-command rows");
        assert_eq!(batch.responses.len(), 3);
        valid(&batch.responses[0].status, &batch.responses[0].response);
        refused(&batch.responses[1].status, &batch.responses[1].response, field);
        valid(&batch.responses[2].status, &batch.responses[2].response);
    }
}

fn proposal_refused(server: &Server, command: Command) {
    let response: DistributedRaftCommandResponse = server.post("/raft/propose", &DistributedRaftProposeRequest { command });
    assert!(!response.status.ok, "a query must never be admitted as a write");
    assert_eq!(response.status.code, "invalid_argument");
    assert!(response.status.message.contains("write commands only"));
    assert_eq!(response.response, CommandResponse::Empty);
}

#[test]
fn proposal_rejects_reads_before_admission_and_normal_http_client_still_forwards_queries() {
    let server = Server::start(env!("CARGO_BIN_EXE_matrixark_rust_datanode"));
    let written: ExecuteResponse = server.post("/execute", &ExecuteRequest { shard_id: 1, command: seed() });
    assert!(written.status.ok);
    let commit = server.commit();
    assert!(commit > 0);
    for command in [typed(FeatureReducer::Mean, 1000), typed(FeatureReducer::Mean, 0),
        typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), Command::StringGet { key: "k".to_owned() }] {
        proposal_refused(&server, command);
        assert_eq!(server.commit(), commit, "wrong-route reads must not append a log entry");
    }
    let client = temporalstore_rust::TemporalStoreClient::with_options(temporalstore_rust::ClientOptions {
        default_shard_id: 1, connect_timeout_ms: 500, io_timeout_ms: 3000,
        ..temporalstore_rust::ClientOptions::proxy(server.addr.as_str())
    });
    let table = client.open_table("feature", "typed", temporalstore_rust::TableOptions::default());
    // Actual table-client forwarding goes to /execute, not /raft/propose. The
    // backend enforces the proposal boundary, so a hidden read fallback fails.
    let before = table.execute(typed(FeatureReducer::Mean, 1000)).unwrap();
    valid(&before.status, &before.response);
    let bad = read(&server, "/execute", typed(FeatureReducer::Mean, 0));
    refused(&bad.status, &bad.response, "scale");
    match table.execute(typed(FeatureReducer::Mean, 0)).unwrap_err() {
        temporalstore_rust::ClientError::Status(message) => assert_eq!(message, bad.status.message),
        other => panic!("expected the table API's semantic status error, got {other:?}"),
    }
    let after = table.execute(typed(FeatureReducer::Mean, 1000)).unwrap();
    valid(&after.status, &after.response);
    assert_eq!(server.commit(), commit);
    let seeded = table.batch_execute(vec![seed()]).unwrap();
    assert!(seeded.status.ok && seeded.responses[0].status.ok);
    let batch = table.batch_execute(vec![typed(FeatureReducer::Mean, 1000),
        typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), typed(FeatureReducer::Mean, 1000)]).unwrap();
    assert!(batch.status.ok);
    assert_eq!(batch.responses.len(), 3);
    valid(&batch.responses[0].status, &batch.responses[0].response);
    refused(&batch.responses[1].status, &batch.responses[1].response, "alpha");
    valid(&batch.responses[2].status, &batch.responses[2].response);
}

#[test]
fn raft_node_proposal_rejects_reads_and_keeps_normal_write_admission() {
    let server = Server::start(env!("CARGO_BIN_EXE_raft_node"));
    let written: DistributedRaftCommandResponse = server.post("/raft/propose", &DistributedRaftProposeRequest { command: seed() });
    assert!(written.status.ok);
    let commit = server.commit();
    assert!(commit > 0);
    for command in [typed(FeatureReducer::Mean, 1000), typed(FeatureReducer::Mean, 0),
        typed(FeatureReducer::Ewma { alpha: 0.0 }, 1000), Command::StringGet { key: "k".to_owned() }] {
        proposal_refused(&server, command);
        assert_eq!(server.commit(), commit);
    }
    let query: DistributedRaftCommandResponse = server.post("/raft/read", &DistributedRaftReadRequest {
        node_id: 1, command: typed(FeatureReducer::Mean, 1000),
    });
    valid(&query.status, &query.response);
    let write: DistributedRaftCommandResponse = server.post("/raft/propose", &DistributedRaftProposeRequest {
        command: Command::StringSet { key: "after".to_owned(), value: b"written".to_vec() },
    });
    assert!(write.status.ok, "write admission was not disabled: {write:?}");
    assert!(server.commit() > commit);
    let read: DistributedRaftCommandResponse = server.post("/raft/read", &DistributedRaftReadRequest {
        node_id: 1, command: Command::StringGet { key: "after".to_owned() },
    });
    assert!(read.status.ok);
    assert_eq!(read.response, CommandResponse::Bytes { value: Some(b"written".to_vec()) });
}
