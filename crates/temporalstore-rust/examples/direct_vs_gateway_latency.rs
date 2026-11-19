// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Append and GET latency of the networked MatrixObject store, one mode per run.
//!
//! ```text
//! MODE=gateway TARGET=matrixobject://127.0.0.1:22110 K=64 OPS=2000 VALUE=1024 \
//!   cargo run --release -p temporalstore-rust --features matrixobject --example direct_vs_gateway_latency
//! MODE=direct  TARGET=127.0.0.1:22111,127.0.0.1:22112,127.0.0.1:22113 K=64 OPS=2000 ...
//! ```
//!
//! `K` objects are created, then `OPS` appends and `OPS` reads go round-robin across them. The
//! loop is sequential (closed loop): it measures the cost of one request, not a rate, so run it
//! on a quiet machine and against the same cluster for both modes. It prints one line per
//! operation kind with p50/p99/max in microseconds and the number of requests the store issued.

use std::time::Instant;

use bytes::Bytes;
use temporalstore_rust::matrixobject_direct::DirectConfig;
use temporalstore_snapshot::object_store::{
    MatrixObjectMode, NetworkedMatrixObjectStore, ObjectStore,
};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[index]
}

fn report(label: &str, mode: MatrixObjectMode, k: usize, mut micros: Vec<u128>, requests: u64) {
    micros.sort_unstable();
    println!(
        "{label} mode={mode:?} k={k} n={} p50_us={} p99_us={} max_us={} requests={requests}",
        micros.len(),
        percentile(&micros, 0.50),
        percentile(&micros, 0.99),
        micros.last().copied().unwrap_or(0),
    );
}

#[tokio::main]
async fn main() {
    let mode = match env("MODE", "gateway").as_str() {
        "direct" => MatrixObjectMode::Direct,
        "gateway" => MatrixObjectMode::Gateway,
        other => panic!("MODE must be gateway or direct, not {other}"),
    };
    let target = std::env::var("TARGET").expect("TARGET: a gateway URI, or the metadata endpoints");
    let k: usize = env("K", "64").parse().expect("K");
    let ops: usize = env("OPS", "2000").parse().expect("OPS");
    let value: usize = env("VALUE", "1024").parse().expect("VALUE");

    let store = match mode {
        MatrixObjectMode::Gateway => {
            NetworkedMatrixObjectStore::connect(mode, &target).expect("connect to the gateway")
        }
        MatrixObjectMode::Direct => DirectConfig::from_env(&target)
            .and_then(|config| config.connect())
            .expect("connect directly"),
    };
    assert_eq!(store.mode(), mode);

    let run = std::process::id();
    let keys: Vec<String> = (0..k).map(|i| format!("latency-{run}/obj-{i:05}")).collect();
    let payload = Bytes::from(vec![0x5a_u8; value]);
    for key in &keys {
        store.put(key, payload.clone()).await.expect("create object");
    }

    let before = store.requests_issued();
    let mut appends = Vec::with_capacity(ops);
    for i in 0..ops {
        let started = Instant::now();
        store.append_blob(&keys[i % k], payload.clone()).await.expect("append");
        appends.push(started.elapsed().as_micros());
    }
    let append_requests = store.requests_issued() - before;

    let before = store.requests_issued();
    let mut reads = Vec::with_capacity(ops);
    for i in 0..ops {
        let started = Instant::now();
        let bytes = store.get(&keys[i % k]).await.expect("get");
        assert!(bytes.len() >= value);
        reads.push(started.elapsed().as_micros());
    }
    let read_requests = store.requests_issued() - before;

    report("append", mode, k, appends, append_requests);
    report("get", mode, k, reads, read_requests);
}
