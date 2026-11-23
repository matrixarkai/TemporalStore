// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The object surface a TemporalStore node uses, checked through a gateway and directly against
//! one real MatrixObject cluster, plus an append loop to run while a server is killed.
//!
//! LEG 1 (`LEG=surface`, the default) runs the same calls through both modes and compares every
//! answer: put, get, ranged get (inside, ending at, and past the end), list and list-after,
//! delete and a read of the deleted object, and compare-and-swap.
//!
//! LEG 2 (`LEG=append`) appends `APPENDS` records one at a time to one object and prints a
//! `PROGRESS n` line as each is acknowledged. A script kills and restarts a chunk server or the
//! metadata leader while it runs (`scripts/direct_kill_leg.sh`). At the end it reads the object
//! back and requires it to equal the acknowledged records, in order, each exactly once. A record
//! whose append returned an error is not acknowledged and is retried by the caller, as a node
//! does; a record that landed although the reply was lost must not appear twice.
//!
//! The run is INCONCLUSIVE, and says so, when no append needed more than one request, or when the
//! kills never produced both a `landed` and an `absent` answer from the gateway (printed as a
//! `RESOLUTIONS` line): a kill that never interrupted an append has tested nothing.
//!
//! ```text
//! MODE=direct META=127.0.0.1:24404,127.0.0.1:24405,127.0.0.1:24406 TS_MATRIXOBJECT_META_GROUP=1 \
//!   LEG=surface cargo run --release -p temporalstore-rust \
//!   --example direct_surface_cluster
//! ```
//!
//! Run each surface leg with a fresh `PREFIX`. Its output has 26 `OBS` lines,
//! checked against expected values by `scripts/check_direct_surface_cluster.py`.

use std::time::Instant;

use bytes::Bytes;
use temporalstore_rust::matrixobject_direct::DirectConfig;
use temporalstore_snapshot::object_store::{
    MatrixObjectMode, NetworkedMatrixObjectStore, ObjectStore,
};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

fn connect(mode: MatrixObjectMode) -> NetworkedMatrixObjectStore {
    match mode {
        MatrixObjectMode::Gateway => {
            let uri = std::env::var("GATEWAY").expect("GATEWAY: the gateway URI");
            NetworkedMatrixObjectStore::connect(mode, &uri).expect("connect to the gateway")
        }
        MatrixObjectMode::Direct => {
            let meta = std::env::var("META").expect("META: the metadata endpoints");
            DirectConfig::from_env(&meta)
                .and_then(|config| config.connect())
                .expect("connect directly")
        }
    }
}

/// One observation, printed in a form two runs can be compared on. Errors are reduced to the
/// kind of failure, because the wording of a transport error differs between the two paths.
fn obs<T: std::fmt::Debug, E: std::fmt::Display>(label: &str, result: Result<T, E>) {
    match result {
        Ok(value) => println!("OBS {label} => ok {value:?}"),
        Err(error) => {
            let text = error.to_string().to_lowercase();
            let kind = if text.contains("not found") || text.contains("404") {
                "not-found"
            } else if text.contains("range") || text.contains("416") {
                "range"
            } else if text.contains("precondition") || text.contains("condition failed") || text.contains("412") {
                "precondition"
            } else {
                "other"
            };
            println!("OBS {label} => err {kind}");
        }
    }
}

async fn surface(store: &NetworkedMatrixObjectStore, prefix: &str) {
    let key = |name: &str| format!("{prefix}/{name}");
    let body: Vec<u8> = (0..100u8).collect();
    let (a, b, c) = (key("a"), key("b"), key("c"));

    obs("get absent", store.get(&a).await.map(|v| v.len()));
    obs("put a", store.put(&a, Bytes::from(body.clone())).await);
    obs("put b", store.put(&b, Bytes::from_static(b"second")).await);
    obs("put c", store.put(&c, Bytes::from_static(b"third")).await);
    obs("get a", store.get(&a).await.map(|v| v.to_vec()));
    obs("range 10..20", store.get_range(&a, 10, 10).await.map(|v| v.to_vec()));
    obs("range ending at the end", store.get_range(&a, 90, 10).await.map(|v| v.to_vec()));
    obs("range past the end", store.get_range(&a, 100, 10).await.map(|v| v.len()));
    obs("range_from 95", store.get_range_from(&a, 95).await.map(|v| v.map(|b| b.to_vec())));
    obs("range_from at the end", store.get_range_from(&a, 100).await.map(|v| v.map(|b| b.len())));
    obs("range_from past the end", store.get_range_from(&a, 101).await.map(|v| v.map(|b| b.len())));
    obs("list prefix", store.list(&format!("{prefix}/")).await);
    obs("list_after a", store.list_after(&format!("{prefix}/"), &a).await);
    obs("append to b", store.append_blob(&b, Bytes::from_static(b"+tail")).await.map(|r| (r.start_offset, r.end_offset, r.object_length)));
    obs("get b after append", store.get(&b).await.map(|v| v.to_vec()));
    obs("delete c", store.delete(&c).await);
    obs("get c after delete", store.get(&c).await.map(|v| v.len()));
    obs("list after delete", store.list(&format!("{prefix}/")).await);
    obs("delete absent", store.delete(&key("never")).await);
    let cas = key("cas");
    obs("CAS create", store.compare_and_swap(&cas, None, Bytes::from_static(b"first")).await);
    obs("CAS wrong value", store.compare_and_swap(&cas, Some(Bytes::from_static(b"wrong")), Bytes::from_static(b"must-not-land")).await);
    obs("get CAS after rejection", store.get(&cas).await.map(|v| v.to_vec()));
    obs("CAS update", store.compare_and_swap(&cas, Some(Bytes::from_static(b"first")), Bytes::from_static(b"second")).await);
    obs("get CAS after update", store.get(&cas).await.map(|v| v.to_vec()));
    obs("CAS duplicate create", store.compare_and_swap(&cas, None, Bytes::from_static(b"must-not-land")).await);
    obs("get CAS after duplicate create", store.get(&cas).await.map(|v| v.to_vec()));
}

async fn append_leg(store: &NetworkedMatrixObjectStore, prefix: &str) {
    let appends: usize = env("APPENDS", "2000").parse().expect("APPENDS");
    let key = format!("{prefix}/kill-leg");
    let mut acknowledged: Vec<u8> = Vec::new();
    let before = store.requests_issued();
    let started = Instant::now();
    let mut errors = 0usize;
    for n in 0..appends {
        let record = format!("<{n:06}>").into_bytes();
        loop {
            match store.append_blob(&key, Bytes::from(record.clone())).await {
                Ok(receipt) => {
                    assert_eq!(
                        receipt.start_offset as usize,
                        acknowledged.len(),
                        "record {n} landed at {} but {} bytes were acknowledged before it",
                        receipt.start_offset,
                        acknowledged.len()
                    );
                    acknowledged.extend_from_slice(&record);
                    break;
                }
                // The store settles an unknown outcome itself; an error that comes back is a
                // refusal to guess. Read the object to learn whether the record landed, and only
                // retry when it did not.
                Err(error) => {
                    errors += 1;
                    println!("ERROR record {n}: {error}");
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    let present = store.get(&key).await.map(|b| b.to_vec()).unwrap_or_default();
                    if present.len() == acknowledged.len() + record.len()
                        && present.ends_with(&record)
                    {
                        acknowledged.extend_from_slice(&record);
                        break;
                    }
                }
            }
        }
        if n % 100 == 0 {
            println!("PROGRESS {n}");
        }
    }
    let requests = store.requests_issued() - before;
    let object = store.get(&key).await.expect("read the object back").to_vec();
    println!(
        "APPENDS n={appends} errors={errors} requests={requests} seconds={:.1}",
        started.elapsed().as_secs_f64()
    );
    assert_eq!(object, acknowledged, "the object is not exactly the acknowledged records");
    let text = String::from_utf8(object).expect("records are ascii");
    for n in 0..appends {
        assert_eq!(text.matches(&format!("<{n:06}>")).count(), 1, "record {n} is not stored exactly once");
    }
    // What the gateway answered when asked whether an interrupted append landed. A kill leg proves
    // the settlement only if BOTH answers were seen: landed (the append committed before the
    // reply was lost) and absent (it did not, and a retry as a new attempt was safe).
    let answers = store.append_resolutions();
    println!(
        "RESOLUTIONS landed={} absent={} ambiguous={}",
        answers.landed, answers.absent, answers.ambiguous
    );
    if requests as usize <= appends {
        println!("RESULT INCONCLUSIVE no append needed a second request, so nothing interrupted one");
    } else if answers.landed == 0 || answers.absent == 0 {
        println!(
            "RESULT INCONCLUSIVE every record is stored once, but the kills produced no {} answer, so that settlement was not exercised",
            if answers.landed == 0 { "landed" } else { "absent" }
        );
    } else {
        println!(
            "RESULT ok {} extra requests were needed, both a landed and an absent answer were seen, and every record is stored once",
            requests as usize - appends
        );
    }
}

#[tokio::main]
async fn main() {
    let mode = match env("MODE", "direct").as_str() {
        "direct" => MatrixObjectMode::Direct,
        "gateway" => MatrixObjectMode::Gateway,
        other => panic!("MODE must be gateway or direct, not {other}"),
    };
    let prefix = env("PREFIX", &format!("surface-{}", std::process::id()));
    let store = connect(mode);
    assert_eq!(store.mode(), mode);
    match env("LEG", "surface").as_str() {
        "surface" => surface(&store, &prefix).await,
        "append" => append_leg(&store, &prefix).await,
        other => panic!("LEG must be surface or append, not {other}"),
    }
}
