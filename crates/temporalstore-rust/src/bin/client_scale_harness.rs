// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use serde::Serialize;
use temporalstore_rust::client::{ClientOptions, TableOptions};
use temporalstore_rust::engine::TemporalEngine;
use temporalstore_rust::http::{json_response, parse_json, serve};
use temporalstore_rust::meta::{GetShardResponse, ShardLocation};
use temporalstore_rust::types::{
    BatchExecuteRequest, Command, CommandResponse, ExecuteRequest, ShardId, Status,
};
use temporalstore_rust::{ClientStats, TemporalStoreClient};

#[derive(Debug, Clone)]
struct HarnessOptions {
    clients: usize,
    threads_per_client: usize,
    ops_per_thread: usize,
    shards: u64,
    servers: usize,
    base_port: u16,
    read_every: usize,
    batch_every: usize,
    route_cache_ttl_ms: u64,
    connect_timeout_ms: u64,
    io_timeout_ms: u64,
    max_retries: usize,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            clients: 4,
            threads_per_client: 4,
            ops_per_thread: 500,
            shards: 8,
            servers: 4,
            base_port: 19_100,
            read_every: 2,
            batch_every: 25,
            route_cache_ttl_ms: 60_000,
            connect_timeout_ms: 200,
            io_timeout_ms: 500,
            max_retries: 1,
        }
    }
}

#[derive(Debug, Serialize)]
struct ClientScaleSummary {
    clients: usize,
    threads: usize,
    shards: u64,
    servers: usize,
    attempted_ops: usize,
    successful_ops: usize,
    failed_ops: usize,
    elapsed_ms: u128,
    ops_per_sec: f64,
    write_latency: LatencySummary,
    read_latency: LatencySummary,
    batch_latency: LatencySummary,
    aggregate_client_stats: ClientStatsSummary,
    route_cache_size_total: usize,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct ClientStatsSummary {
    execute_requests: u64,
    batch_execute_requests: u64,
    route_cache_hits: u64,
    route_cache_misses: u64,
    route_refreshes: u64,
    backend_errors: u64,
    continuous_backend_failures: u64,
    meta_sync_total: u64,
    meta_sync_errors: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
struct LatencySummary {
    samples: usize,
    p50_us: u128,
    p95_us: u128,
    p99_us: u128,
    max_us: u128,
}

#[derive(Debug, Default)]
struct WorkerReport {
    successful_ops: usize,
    failed_ops: usize,
    write_latencies: Vec<Duration>,
    read_latencies: Vec<Duration>,
    batch_latencies: Vec<Duration>,
}

fn main() {
    let options = parse_options();
    if options.clients == 0 || options.threads_per_client == 0 || options.shards == 0 {
        eprintln!("clients, threads-per-client, and shards must be > 0");
        std::process::exit(2);
    }

    let meta_addr = test_addr(options.base_port);
    let routes = Arc::new(RwLock::new(HashMap::<ShardId, String>::new()));
    let server_addrs = start_servers(&options);
    for shard_id in 1..=options.shards {
        let server = &server_addrs[((shard_id - 1) as usize) % server_addrs.len()];
        routes
            .write()
            .expect("route map lock poisoned")
            .insert(shard_id, server.clone());
    }
    start_meta(meta_addr.clone(), Arc::clone(&routes));
    wait_for_http(&meta_addr);
    for server_addr in &server_addrs {
        wait_for_http(server_addr);
    }

    let clients = (0..options.clients)
        .map(|_| {
            let client = TemporalStoreClient::with_options(ClientOptions {
                proxy_addr: meta_addr.clone(),
                meta_addr: Some(meta_addr.clone()),
                connect_timeout_ms: options.connect_timeout_ms,
                io_timeout_ms: options.io_timeout_ms,
                max_retries: options.max_retries,
                route_cache_ttl_ms: options.route_cache_ttl_ms,
                ..ClientOptions::default()
            });
            client.open_table(
                "scale",
                "client",
                TableOptions {
                    first_shard_id: 1,
                    shard_count: options.shards,
                    pin_primary: true,
                    connect_timeout_ms: options.connect_timeout_ms,
                    io_timeout_ms: options.io_timeout_ms,
                    ..TableOptions::default()
                },
            )
        })
        .collect::<Vec<_>>();

    let start = Instant::now();
    let mut handles = Vec::new();
    for client_id in 0..options.clients {
        for thread_id in 0..options.threads_per_client {
            let table = clients[client_id].clone();
            let options = options.clone();
            handles.push(thread::spawn(move || {
                run_worker(client_id, thread_id, table, options)
            }));
        }
    }

    let mut report = WorkerReport::default();
    for handle in handles {
        let worker = handle.join().expect("client scale worker panicked");
        report.successful_ops += worker.successful_ops;
        report.failed_ops += worker.failed_ops;
        report.write_latencies.extend(worker.write_latencies);
        report.read_latencies.extend(worker.read_latencies);
        report.batch_latencies.extend(worker.batch_latencies);
    }
    let elapsed_ms = start.elapsed().as_millis();
    let successful_ops = report.successful_ops;
    let attempted_ops = successful_ops + report.failed_ops;
    let aggregate_stats = clients.iter().map(|table| table.client_stats()).fold(
        ClientStatsSummary::default(),
        |mut out, stats| {
            out.add(stats);
            out
        },
    );
    let route_cache_size_total = clients
        .iter()
        .map(|table| table.client_route_cache_size())
        .sum::<usize>();
    let summary = ClientScaleSummary {
        clients: options.clients,
        threads: options.clients * options.threads_per_client,
        shards: options.shards,
        servers: options.servers,
        attempted_ops,
        successful_ops,
        failed_ops: report.failed_ops,
        elapsed_ms,
        ops_per_sec: if elapsed_ms == 0 {
            successful_ops as f64
        } else {
            successful_ops as f64 / (elapsed_ms as f64 / 1_000.0)
        },
        write_latency: summarize_latencies(&report.write_latencies),
        read_latency: summarize_latencies(&report.read_latencies),
        batch_latency: summarize_latencies(&report.batch_latencies),
        aggregate_client_stats: aggregate_stats,
        route_cache_size_total,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&summary).expect("summary should serialize")
    );
    if summary.failed_ops > 0 {
        std::process::exit(1);
    }
}

fn run_worker(
    client_id: usize,
    thread_id: usize,
    table: temporalstore_rust::TemporalStoreTable,
    options: HarnessOptions,
) -> WorkerReport {
    let mut report = WorkerReport::default();
    for op in 0..options.ops_per_thread {
        let key = format!("client:{client_id}:thread:{thread_id}:op:{op}");
        let value = format!("value:{client_id}:{thread_id}:{op}").into_bytes();
        let write_start = Instant::now();
        match table.set(key.clone(), value.clone()) {
            Ok(()) => {
                report.successful_ops += 1;
                report.write_latencies.push(write_start.elapsed());
            }
            Err(_) => {
                report.failed_ops += 1;
                continue;
            }
        }

        if options.read_every > 0 && op % options.read_every == 0 {
            let read_start = Instant::now();
            match table.get(key.clone()) {
                Ok(Some(read_value)) if read_value == value => {
                    report.successful_ops += 1;
                    report.read_latencies.push(read_start.elapsed());
                }
                _ => report.failed_ops += 1,
            }
        }

        if options.batch_every > 0 && op > 0 && op % options.batch_every == 0 {
            let batch_key = format!("client:{client_id}:thread:{thread_id}:batch:{op}");
            let batch_start = Instant::now();
            match table.batch_execute(vec![
                Command::StringSet {
                    key: batch_key.clone(),
                    value: b"batch-value".to_vec(),
                },
                Command::StringGet { key: batch_key },
            ]) {
                Ok(response)
                    if response.status.ok
                        && matches!(
                            response.responses.last(),
                            Some(execute_response)
                                if matches!(
                                    &execute_response.response,
                                    CommandResponse::Bytes { value: Some(bytes) }
                                        if bytes == b"batch-value"
                                )
                        ) =>
                {
                    report.successful_ops += 1;
                    report.batch_latencies.push(batch_start.elapsed());
                }
                _ => report.failed_ops += 1,
            }
        }
    }
    report
}

fn start_servers(options: &HarnessOptions) -> Vec<String> {
    (0..options.servers.max(1))
        .map(|server_index| {
            let addr = test_addr(options.base_port + 1 + server_index as u16);
            let engine = TemporalEngine::default();
            for shard_id in 1..=options.shards {
                engine.load_shard(shard_id);
            }
            thread::spawn({
                let addr = addr.clone();
                move || {
                    serve(&addr, move |request| {
                        match (request.method.as_str(), request.path.as_str()) {
                            ("GET", "/health") => json_response(200, &Status::ok()),
                            ("POST", "/execute") => {
                                let req = parse_json::<ExecuteRequest>(&request.body).unwrap();
                                json_response(200, &engine.execute(req))
                            }
                            ("POST", "/batch_execute") => {
                                let req = parse_json::<BatchExecuteRequest>(&request.body).unwrap();
                                json_response(200, &engine.batch_execute(req))
                            }
                            _ => json_response(404, &Status::error("not_found", "not found")),
                        }
                    })
                    .unwrap();
                }
            });
            addr
        })
        .collect()
}

fn start_meta(meta_addr: String, routes: Arc<RwLock<HashMap<ShardId, String>>>) {
    thread::spawn(move || {
        serve(&meta_addr, move |request| {
            match (request.method.as_str(), request.path.as_str()) {
                ("GET", "/health") => json_response(200, &Status::ok()),
                ("GET", path) if path.starts_with("/shards/") => {
                    let shard_id = path
                        .trim_start_matches("/shards/")
                        .parse::<ShardId>()
                        .unwrap_or_default();
                    let location = routes
                        .read()
                        .expect("route map lock poisoned")
                        .get(&shard_id)
                        .map(|server_addr| ShardLocation {
                            preferred_location: String::new(),
                            state: temporalstore_rust::meta::MetaEntityState::Normal,
                            shard_id,
                            server_addr: server_addr.clone(),
                            latest_snapshot: None,
                        });
                    json_response(
                        200,
                        &GetShardResponse {
                            status: if location.is_some() {
                                Status::ok()
                            } else {
                                Status::error("shard_not_found", "shard is not registered")
                            },
                            location,
                        },
                    )
                }
                _ => json_response(404, &Status::error("not_found", "not found")),
            }
        })
        .unwrap();
    });
}

impl ClientStatsSummary {
    fn add(&mut self, stats: ClientStats) {
        self.execute_requests += stats.execute_requests;
        self.batch_execute_requests += stats.batch_execute_requests;
        self.route_cache_hits += stats.route_cache_hits;
        self.route_cache_misses += stats.route_cache_misses;
        self.route_refreshes += stats.route_refreshes;
        self.backend_errors += stats.backend_errors;
        self.continuous_backend_failures += stats.continuous_backend_failures;
        self.meta_sync_total += stats.meta_sync_total;
        self.meta_sync_errors += stats.meta_sync_errors;
    }
}

fn summarize_latencies(latencies: &[Duration]) -> LatencySummary {
    if latencies.is_empty() {
        return LatencySummary::default();
    }
    let mut values = latencies
        .iter()
        .map(Duration::as_micros)
        .collect::<Vec<_>>();
    values.sort_unstable();
    LatencySummary {
        samples: values.len(),
        p50_us: percentile(&values, 50),
        p95_us: percentile(&values, 95),
        p99_us: percentile(&values, 99),
        max_us: *values.last().unwrap_or(&0),
    }
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let index = ((values.len() - 1) * percentile) / 100;
    values[index]
}

fn wait_for_http(addr: &str) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if std::net::TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("{addr} did not start");
}

fn test_addr(port: u16) -> String {
    format!("127.0.0.1:{port}")
}

fn parse_options() -> HarnessOptions {
    let mut options = HarnessOptions::default();
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--clients" => options.clients = parse(&value, &key),
            "--threads-per-client" => options.threads_per_client = parse(&value, &key),
            "--ops-per-thread" => options.ops_per_thread = parse(&value, &key),
            "--shards" => options.shards = parse(&value, &key),
            "--servers" => options.servers = parse(&value, &key),
            "--base-port" => options.base_port = parse(&value, &key),
            "--read-every" => options.read_every = parse(&value, &key),
            "--batch-every" => options.batch_every = parse(&value, &key),
            "--route-cache-ttl-ms" => options.route_cache_ttl_ms = parse(&value, &key),
            "--connect-timeout-ms" => options.connect_timeout_ms = parse(&value, &key),
            "--io-timeout-ms" => options.io_timeout_ms = parse(&value, &key),
            "--max-retries" => options.max_retries = parse(&value, &key),
            _ => usage_and_exit(),
        }
    }
    options
}

fn parse<T: std::str::FromStr>(value: &str, key: &str) -> T {
    value.parse().unwrap_or_else(|_| {
        eprintln!("invalid value for {key}: {value}");
        std::process::exit(2);
    })
}

fn usage_and_exit() -> ! {
    eprintln!("usage: client_scale_harness [options]");
    eprintln!("  --clients <n>              default 4");
    eprintln!("  --threads-per-client <n>   default 4");
    eprintln!("  --ops-per-thread <n>       default 500");
    eprintln!("  --shards <n>               default 8");
    eprintln!("  --servers <n>              default 4");
    eprintln!("  --base-port <port>         default 19100");
    eprintln!("  --read-every <n>           default 2");
    eprintln!("  --batch-every <n>          default 25");
    eprintln!("  --route-cache-ttl-ms <n>   default 60000");
    eprintln!("  --connect-timeout-ms <n>   default 200");
    eprintln!("  --io-timeout-ms <n>        default 500");
    eprintln!("  --max-retries <n>          default 1");
    std::process::exit(2);
}
