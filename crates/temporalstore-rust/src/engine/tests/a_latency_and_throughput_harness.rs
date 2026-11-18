// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MEASUREMENT APPARATUS: per-operation latency over object sizes, and write and read
//! throughput at 1, 4 and 16 threads, for string, hash and feature commands. Release build.
//!
//!   cargo test --release --lib latency_table_by_size -- --ignored --nocapture --test-threads=1
//!   cargo test --release --lib throughput_table_by_thread_count -- --ignored --nocapture --test-threads=1
//!
//! rust-internal: measures this crate's own request paths, no product behaviour

#![allow(clippy::all)]
use super::*;

use crate::types::FeaturePoint;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const BASE: u64 = 1_787_270_070_000;

fn engine_on(dir: &std::path::Path, bound: usize) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        256 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    assert!(
        engine
            .set_config(crate::control::SetConfigRequest {
                shard_id: 1,
                config: crate::control::Config {
                    version: 2,
                    feature_max_size: bound,
                    ..Default::default()
                },
            })
            .ok
    );
    engine
}

fn run(engine: &TemporalEngine, command: Command) -> bool {
    engine.execute(ExecuteRequest { shard_id: 1, command }).status.ok
}

fn pt(i: u64) -> FeaturePoint {
    FeaturePoint { timestamp_ms: BASE + i * 1000, value: format!("{}", 1000 + i).into_bytes() }
}

fn latency(engine: &TemporalEngine, name: &str, param: &str, ops: Vec<Command>) {
    let mut times = Vec::with_capacity(ops.len());
    let mut ok = 0usize;
    for command in ops {
        let started = Instant::now();
        if run(engine, command) {
            ok += 1;
        }
        times.push(started.elapsed().as_nanos() as f64 / 1000.0);
    }
    let n = times.len();
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "LAT|{name}|{param}|p50us={:.1}|p99us={:.1}|ok={ok}/{n}",
        times[n / 2],
        times[(n * 99 / 100).min(n - 1)]
    );
}

#[test]
#[ignore = "measurement: run with --release --ignored --nocapture"]
fn latency_table_by_size() {
    let n = 200usize;
    // STRING by value size
    for size in [64usize, 4096, 65536, 1 << 20] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine_on(dir.path(), 100_000);
        let v = vec![b'v'; size];
        for i in 0..n {
            run(&e, Command::StringSet { key: format!("s{i}"), value: v.clone() });
            run(&e, Command::StringGet { key: format!("s{i}") });
        }
        let p = format!("size={size}");
        latency(&e, "string_set", &p, (0..n).map(|i| Command::StringSet { key: format!("s{i}"), value: v.clone() }).collect());
        latency(&e, "string_get", &p, (0..n).map(|i| Command::StringGet { key: format!("s{i}") }).collect());
        latency(&e, "string_strlen", &p, (0..n).map(|i| Command::StringLen { key: format!("s{i}") }).collect());
        latency(&e, "string_getrange16", &p, (0..n).map(|i| Command::StringGetRange { key: format!("s{i}"), start: 0, end: 15 }).collect());
    }
    // HASH by field count
    for fields in [100usize, 1000, 4000] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine_on(dir.path(), 100_000);
        let v = vec![b'v'; 64];
        for f in 0..fields {
            run(&e, Command::HashSet { key: "h".into(), field: format!("f{f:06}"), value: v.clone() });
        }
        let p = format!("fields={fields}");
        latency(&e, "hash_get", &p, (0..n).map(|i| Command::HashGet { key: "h".into(), field: format!("f{:06}", i % fields) }).collect());
        latency(&e, "hash_set_over", &p, (0..n).map(|i| Command::HashSet { key: "h".into(), field: format!("f{:06}", i % fields), value: v.clone() }).collect());
        latency(&e, "hash_set_new", &p, (0..n).map(|i| Command::HashSet { key: "h".into(), field: format!("n{i:06}"), value: v.clone() }).collect());
        latency(&e, "hash_len", &p, (0..n).map(|_| Command::HashLen { key: "h".into() }).collect());
        latency(&e, "hash_getall", &p, (0..20).map(|_| Command::HashGetAll { key: "h".into() }).collect());
    }
    // FEATURE by series length (below the cap, and at it)
    for len in [100usize, 1000, 4000] {
        for at_cap in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let e = engine_on(dir.path(), if at_cap { len } else { 100_000 });
            for i in 0..len as u64 {
                run(&e, Command::FeatureAppend { key: "s".into(), points: vec![pt(i)] });
            }
            let p = format!("len={len} at_cap={at_cap}");
            latency(&e, "feature_append", &p, (0..100u64).map(|j| Command::FeatureAppend { key: "s".into(), points: vec![pt(len as u64 + j)] }).collect());
            if !at_cap {
                latency(&e, "feature_query10", &p, (0..100usize).map(|j| Command::FeatureQuery { key: "s".into(), start_ms: BASE + ((len / 2 + j) as u64) * 1000, end_ms: BASE + ((len / 2 + j + 9) as u64) * 1000, count: None }).collect());
                latency(&e, "feature_query_all", &p, (0..20usize).map(|j| Command::FeatureQuery { key: "s".into(), start_ms: 0, end_ms: u64::MAX, count: Some(len + 1 + j) }).collect());
            }
        }
    }
}

fn throughput(name: &str, threads: usize, make: impl Fn(&TemporalEngine, usize, u64) -> bool + Send + Sync + 'static, engine: Arc<TemporalEngine>) {
    let stop = Arc::new(AtomicBool::new(false));
    let count = Arc::new(AtomicU64::new(0));
    let make = Arc::new(make);
    let mut handles = Vec::new();
    for t in 0..threads {
        let (e, stop, count, make) = (engine.clone(), stop.clone(), count.clone(), make.clone());
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                if make(&e, t, i) {
                    count.fetch_add(1, Ordering::Relaxed);
                }
                i += 1;
            }
        }));
    }
    let started = Instant::now();
    std::thread::sleep(Duration::from_millis(2500));
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let secs = started.elapsed().as_secs_f64();
    println!("QPS|{name}|threads={threads}|qps={:.0}", count.load(Ordering::Relaxed) as f64 / secs);
}

#[test]
#[ignore = "measurement: run with --release --ignored --nocapture"]
fn throughput_table_by_thread_count() {
    for threads in [1usize, 4, 16] {
        let dir = tempfile::tempdir().unwrap();
        let e = engine_on(dir.path(), 100_000);
        let v = vec![b'v'; 256];
        for t in 0..threads {
            for k in 0..64 {
                run(&e, Command::StringSet { key: format!("s{t}-{k}"), value: v.clone() });
                run(&e, Command::HashSet { key: format!("h{t}"), field: format!("f{k}"), value: v.clone() });
            }
            run(&e, Command::FeatureAppend { key: format!("fa{t}"), points: (0..200u64).map(pt).collect() });
        }
        let v2 = v.clone();
        throughput("string_set_256B", threads, move |e, t, i| run(e, Command::StringSet { key: format!("s{t}-{}", i % 64), value: v2.clone() }), e.clone());
        throughput("string_get_256B", threads, |e, t, i| run(e, Command::StringGet { key: format!("s{t}-{}", i % 64) }), e.clone());
        let v3 = v.clone();
        throughput("hash_set_256B", threads, move |e, t, i| run(e, Command::HashSet { key: format!("h{t}"), field: format!("f{}", i % 64), value: v3.clone() }), e.clone());
        throughput("hash_get", threads, |e, t, i| run(e, Command::HashGet { key: format!("h{t}"), field: format!("f{}", i % 64) }), e.clone());
        throughput("feature_append", threads, |e, t, i| run(e, Command::FeatureAppend { key: format!("fa{t}"), points: vec![pt(1000 + i)] }), e.clone());
        throughput("feature_query10", threads, |e, t, i| run(e, Command::FeatureQuery { key: format!("fa{t}"), start_ms: BASE + (i % 100) * 1000, end_ms: BASE + (i % 100 + 9) * 1000, count: None }), e.clone());
    }
}
