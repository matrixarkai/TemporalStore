// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde_json::json;
use temporalstore::Client;

use crate::matrixark_rust_proxy_command_stats::command_stats;
use crate::matrixark_rust_proxy_dispatch::{run, run_with_client};
use crate::matrixark_rust_proxy_io::{export_metrics_if_configured, print_result};
use crate::matrixark_rust_proxy_metrics::{matrixark_rust_service_mode, CommandStats, MetricsSnapshot};
use crate::matrixark_rust_proxy_protocol::Command;
use crate::matrixark_rust_proxy_runtime::{config_key, connect};

pub(crate) fn serve() -> i32 {
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

pub(crate) fn single_shot() -> i32 {
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
