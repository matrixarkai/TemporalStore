use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde_json::json;
use temporalstore::Client;

#[path = "../matrixark_rust_proxy_cache.rs"]
mod matrixark_rust_proxy_cache;
#[path = "../matrixark_rust_proxy_candidate_node_path.rs"]
mod matrixark_rust_proxy_candidate_node_path;
#[path = "../matrixark_rust_proxy_candidates.rs"]
mod matrixark_rust_proxy_candidates;
#[path = "../matrixark_rust_proxy_command_entries.rs"]
mod matrixark_rust_proxy_command_entries;
#[path = "../matrixark_rust_proxy_command_stats.rs"]
mod matrixark_rust_proxy_command_stats;
#[path = "../matrixark_rust_proxy_cross_session.rs"]
mod matrixark_rust_proxy_cross_session;
#[path = "../matrixark_rust_proxy_metrics.rs"]
mod matrixark_rust_proxy_metrics;
#[path = "../matrixark_rust_proxy_metrics_backend_render.rs"]
mod matrixark_rust_proxy_metrics_backend_render;
#[path = "../matrixark_rust_proxy_metrics_core_render.rs"]
mod matrixark_rust_proxy_metrics_core_render;
#[path = "../matrixark_rust_proxy_metrics_format.rs"]
mod matrixark_rust_proxy_metrics_format;
#[path = "../matrixark_rust_proxy_metrics_io_render.rs"]
mod matrixark_rust_proxy_metrics_io_render;
#[path = "../matrixark_rust_proxy_metrics_render.rs"]
mod matrixark_rust_proxy_metrics_render;
#[path = "../matrixark_rust_proxy_metrics_retrieve_render.rs"]
mod matrixark_rust_proxy_metrics_retrieve_render;
#[path = "../matrixark_rust_proxy_native_pack.rs"]
mod matrixark_rust_proxy_native_pack;
#[path = "../matrixark_rust_proxy_pack.rs"]
mod matrixark_rust_proxy_pack;
#[path = "../matrixark_rust_proxy_protocol.rs"]
mod matrixark_rust_proxy_protocol;
#[path = "../matrixark_rust_proxy_records.rs"]
mod matrixark_rust_proxy_records;
#[path = "../matrixark_rust_proxy_record_time_index.rs"]
mod matrixark_rust_proxy_record_time_index;
#[path = "../matrixark_rust_proxy_dispatch.rs"]
mod matrixark_rust_proxy_dispatch;
#[path = "../matrixark_rust_proxy_dispatch_hash.rs"]
mod matrixark_rust_proxy_dispatch_hash;
#[path = "../matrixark_rust_proxy_dispatch_matrixark.rs"]
mod matrixark_rust_proxy_dispatch_matrixark;
#[path = "../matrixark_rust_proxy_io.rs"]
mod matrixark_rust_proxy_io;
#[path = "../matrixark_rust_proxy_retrieve.rs"]
mod matrixark_rust_proxy_retrieve;
#[path = "../matrixark_rust_proxy_retrieve_policy.rs"]
mod matrixark_rust_proxy_retrieve_policy;
#[path = "../matrixark_rust_proxy_retrieve_result.rs"]
mod matrixark_rust_proxy_retrieve_result;
#[path = "../matrixark_rust_proxy_retrieve_request.rs"]
mod matrixark_rust_proxy_retrieve_request;
#[path = "../matrixark_rust_proxy_retrieve_response.rs"]
mod matrixark_rust_proxy_retrieve_response;
#[path = "../matrixark_rust_proxy_retrieve_scoring.rs"]
mod matrixark_rust_proxy_retrieve_scoring;
#[path = "../matrixark_rust_proxy_retrieve_select.rs"]
mod matrixark_rust_proxy_retrieve_select;
#[path = "../matrixark_rust_proxy_retrieve_telemetry.rs"]
mod matrixark_rust_proxy_retrieve_telemetry;
#[path = "../matrixark_rust_proxy_runtime.rs"]
mod matrixark_rust_proxy_runtime;
#[path = "../matrixark_rust_proxy_scan.rs"]
mod matrixark_rust_proxy_scan;
#[path = "../matrixark_rust_proxy_scan_records.rs"]
mod matrixark_rust_proxy_scan_records;
#[path = "../matrixark_rust_proxy_scan_response.rs"]
mod matrixark_rust_proxy_scan_response;
#[path = "../matrixark_rust_proxy_scan_secondary.rs"]
mod matrixark_rust_proxy_scan_secondary;
#[path = "../matrixark_rust_proxy_scope.rs"]
mod matrixark_rust_proxy_scope;
#[path = "../matrixark_rust_proxy_scope_boost.rs"]
mod matrixark_rust_proxy_scope_boost;
use matrixark_rust_proxy_command_stats::command_stats;
use matrixark_rust_proxy_io::{export_metrics_if_configured, print_result};
use matrixark_rust_proxy_metrics::{matrixark_rust_service_mode, CommandStats, MetricsSnapshot};
use matrixark_rust_proxy_protocol::Command;
use matrixark_rust_proxy_dispatch::{run, run_with_client};
use matrixark_rust_proxy_runtime::{config_key, connect};
#[cfg(test)]
#[path = "../matrixark_rust_proxy_impl_tests.rs"]
mod matrixark_rust_proxy_impl_tests;

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut clients: HashMap<String, Client> = HashMap::new();
    let mut metrics = MetricsSnapshot::default();
    export_metrics_if_configured(&metrics);
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(err) => {
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let command: Command = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(err) => {
                metrics.parse_errors += 1;
                export_metrics_if_configured(&metrics);
                println!("{}", json!({"ok": false, "error": err.to_string()}));
                let _ = stdout.flush();
                continue;
            }
        };
        if command.op == "metrics_prometheus" {
            println!(
                "{}",
                json!({"ok": true, "prometheus": metrics.render_prometheus()})
            );
            let _ = stdout.flush();
            continue;
        }
        if command.op == "health" {
            println!(
                "{}",
                json!({"ok": true, "status": "ok", "mode": matrixark_rust_service_mode()})
            );
            let _ = stdout.flush();
            continue;
        }
        if command.op == "shutdown" {
            println!("{}", json!({"ok": true, "status": "shutting_down"}));
            let _ = stdout.flush();
            return 0;
        }
        let key = config_key(&command);
        if !clients.contains_key(&key) {
            match connect(&command) {
                Ok(client) => {
                    clients.insert(key.clone(), client);
                    metrics.clients_created += 1;
                    export_metrics_if_configured(&metrics);
                }
                Err(err) => {
                    metrics.client_connect_errors += 1;
                    export_metrics_if_configured(&metrics);
                    println!("{}", json!({"ok": false, "error": err}));
                    let _ = stdout.flush();
                    continue;
                }
            }
        }
        if command.op == "readiness" {
            println!(
                "{}",
                json!({
                    "ok": true,
                    "status": "ready",
                    "mode": matrixark_rust_service_mode(),
                    "cached_clients": clients.len()
                })
            );
            let _ = stdout.flush();
            continue;
        }
        let op = command.op.clone();
        let started = Instant::now();
        let result = clients
            .get(&key)
            .ok_or_else(|| "missing cached TemporalStore client".to_string())
            .and_then(|client| run_with_client(client, command.clone()));
        let elapsed_ms = started.elapsed().as_millis();
        let (ok, stats) = match &result {
            Ok(value) => (true, command_stats(&command, value)),
            Err(_) => (false, CommandStats::default()),
        };
        let serialization_started = Instant::now();
        match &result {
            Ok(value) => {
                let _ = serde_json::to_string(value);
            }
            Err(err) => {
                let _ = serde_json::to_string(&json!({"ok": false, "error": err}));
            }
        }
        let serialization_ms = serialization_started.elapsed().as_millis();
        metrics.observe(
            &op,
            ok,
            elapsed_ms,
            serialization_ms,
            result.as_ref().ok(),
            stats,
        );
        export_metrics_if_configured(&metrics);
        let _ = print_result(result, elapsed_ms);
        let _ = stdout.flush();
    }
    0
}

fn single_shot() -> i32 {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        println!("{}", json!({"ok": false, "error": err.to_string()}));
        return 1;
    }
    let command: Command = match serde_json::from_str(&input) {
        Ok(value) => value,
        Err(err) => {
            println!("{}", json!({"ok": false, "error": err.to_string()}));
            return 1;
        }
    };
    let started = Instant::now();
    if print_result(run(command), started.elapsed().as_millis()).0 {
        0
    } else {
        1
    }
}

fn main() {
    let code = if std::env::args().any(|arg| arg == "--serve") {
        serve()
    } else {
        single_shot()
    };
    std::process::exit(code);
}
