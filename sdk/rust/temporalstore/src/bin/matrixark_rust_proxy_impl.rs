use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde_json::json;
#[cfg(test)]
use serde_json::Value;
use temporalstore::Client;

#[path = "../matrixark_rust_proxy_cache.rs"]
mod matrixark_rust_proxy_cache;
#[path = "../matrixark_rust_proxy_candidates.rs"]
mod matrixark_rust_proxy_candidates;
#[path = "../matrixark_rust_proxy_command_stats.rs"]
mod matrixark_rust_proxy_command_stats;
#[path = "../matrixark_rust_proxy_metrics.rs"]
mod matrixark_rust_proxy_metrics;
#[path = "../matrixark_rust_proxy_metrics_render.rs"]
mod matrixark_rust_proxy_metrics_render;
#[path = "../matrixark_rust_proxy_native_pack.rs"]
mod matrixark_rust_proxy_native_pack;
#[path = "../matrixark_rust_proxy_pack.rs"]
mod matrixark_rust_proxy_pack;
#[path = "../matrixark_rust_proxy_protocol.rs"]
mod matrixark_rust_proxy_protocol;
#[path = "../matrixark_rust_proxy_records.rs"]
mod matrixark_rust_proxy_records;
#[path = "../matrixark_rust_proxy_dispatch.rs"]
mod matrixark_rust_proxy_dispatch;
#[path = "../matrixark_rust_proxy_io.rs"]
mod matrixark_rust_proxy_io;
#[path = "../matrixark_rust_proxy_retrieve.rs"]
mod matrixark_rust_proxy_retrieve;
#[path = "../matrixark_rust_proxy_retrieve_request.rs"]
mod matrixark_rust_proxy_retrieve_request;
#[path = "../matrixark_rust_proxy_retrieve_scoring.rs"]
mod matrixark_rust_proxy_retrieve_scoring;
#[path = "../matrixark_rust_proxy_runtime.rs"]
mod matrixark_rust_proxy_runtime;
#[path = "../matrixark_rust_proxy_scan.rs"]
mod matrixark_rust_proxy_scan;
#[path = "../matrixark_rust_proxy_scope.rs"]
mod matrixark_rust_proxy_scope;
use matrixark_rust_proxy_command_stats::command_stats;
use matrixark_rust_proxy_io::{export_metrics_if_configured, print_result};
use matrixark_rust_proxy_metrics::{matrixark_rust_service_mode, CommandStats, MetricsSnapshot};
use matrixark_rust_proxy_protocol::Command;
use matrixark_rust_proxy_dispatch::{run, run_with_client};
use matrixark_rust_proxy_runtime::{config_key, connect};
#[cfg(test)]
use matrixark_rust_proxy_records::{
    matrixark_context_event_time_field, matrixark_context_event_time_key,
    matrixark_context_event_time_payload, matrixark_record_id, matrixark_record_type,
    matrixark_storage_field, matrixark_storage_key, matrixark_tenant_hash,
};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrixark_record_derives_storage_key_from_common_ids() {
        let record = json!({
            "record_type": "resource_manifest",
            "tenant_hash": 77,
            "resource_hash": 7001,
            "raw_uri": "file:///runbooks/gpu.md"
        });
        assert_eq!(
            matrixark_record_type(&record, None).unwrap(),
            "resource_manifest"
        );
        assert_eq!(matrixark_tenant_hash(&record, None).unwrap(), 77);
        assert_eq!(matrixark_record_id(&record, None).unwrap(), "7001");
        assert_eq!(
            matrixark_storage_key("resource_manifest", 77),
            "matrixark:record:resource_manifest:77"
        );
        assert_eq!(matrixark_storage_field("7001"), "7001");
    }

    #[test]
    fn matrixark_record_allows_explicit_fallbacks() {
        let record = json!({"payload": "minimal"});
        assert_eq!(
            matrixark_record_type(&record, Some(&"skill_section".to_string())).unwrap(),
            "skill_section"
        );
        assert_eq!(matrixark_tenant_hash(&record, Some(9)).unwrap(), 9);
        assert_eq!(
            matrixark_record_id(&record, Some(&"section-a".to_string())).unwrap(),
            "section-a"
        );
    }

    #[test]
    fn matrixark_record_rejects_missing_identity() {
        let record = json!({"record_type": "context_event", "tenant_hash": 1});
        assert!(matrixark_record_id(&record, None).is_err());
        assert!(matrixark_record_type(&json!({}), None).is_err());
        assert!(matrixark_tenant_hash(&json!({}), None).is_err());
    }

    #[test]
    fn matrixark_record_storage_key_is_shared_for_batch_read_write() {
        assert_eq!(
            matrixark_storage_key("context_pack_audit", 77),
            "matrixark:record:context_pack_audit:77"
        );
        assert_eq!(matrixark_storage_field("query-1"), "query-1");
    }

    #[test]
    fn context_event_uses_timestamp_ordered_storage_field() {
        let record = json!({
            "record_type": "context_event",
            "tenant_hash": 77,
            "event_id_hash": 42,
            "updated_at_ms": 1782500000123_u64,
            "text": "timestamp keyed"
        });
        assert_eq!(
            matrixark_context_event_time_key(matrixark_tenant_hash(&record, None).unwrap()),
            "matrixark:record:context_event_by_ingestion_time:77"
        );
        assert_eq!(
            matrixark_context_event_time_field(
                &record,
                &matrixark_record_id(&record, None).unwrap()
            ),
            "00000001782500000123:42"
        );
        let indexed_payload: Value = serde_json::from_str(
            &matrixark_context_event_time_payload(&json!({
                "record_type": "context_event",
                "tenant_hash": 77,
                "event_id_hash": 42,
                "ingestion_time_ms": 1782500000123_u64,
                "event_time_key": "00000001782500000123:42",
                "text": "timestamp keyed"
            }))
            .unwrap(),
        )
        .unwrap();
        assert_eq!(indexed_payload["record_type"], "context_event");
        assert_eq!(indexed_payload["text"], "timestamp keyed");
        assert!(indexed_payload.get("ingestion_time_ms").is_none());
        assert!(indexed_payload.get("event_time_key").is_none());
    }

    #[test]
    fn metrics_render_prometheus_records_op_status_and_latency() {
        let mut metrics = MetricsSnapshot::default();
        metrics.observe(
            "write_matrixark_record",
            true,
            12,
            1,
            None,
            CommandStats {
                records_written: 1,
                bytes_written: 128,
                ..CommandStats::default()
            },
        );
        metrics.observe(
            "write_matrixark_record",
            false,
            30,
            2,
            None,
            CommandStats::default(),
        );
        let text = metrics.render_prometheus();
        assert!(text.contains(
            "matrixark_rust_proxy_commands_total{op=\"write_matrixark_record\",status=\"ok\"} 1"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_commands_total{op=\"write_matrixark_record\",status=\"error\"} 1"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_command_latency_ms_sum{op=\"write_matrixark_record\"} 42"
        ));
        assert!(text.contains(
            "matrixark_rust_proxy_command_latency_ms_max{op=\"write_matrixark_record\"} 30"
        ));
        assert!(text.contains("matrixark_rust_proxy_records_written_total 1"));
        assert!(text.contains("matrixark_rust_proxy_bytes_written_total 128"));
        assert!(text.contains("matrixark_rust_proxy_commands_failed_total 1"));
    }

    #[test]
    fn command_stats_counts_scan_hash_records() {
        let command: Command = serde_json::from_value(json!({
            "op": "scan_hash",
            "key": "matrixark:mcp:records:000000"
        }))
        .expect("command");
        let stats = command_stats(&command, &json!({"ok": true, "count": 3, "records": []}));
        assert_eq!(stats.records_read, 3);
    }

    #[test]
    fn command_stats_counts_matrixark_batch_records() {
        let command: Command = serde_json::from_value(json!({
            "op": "write_matrixark_records",
            "records": [
                {"record_type": "context_event", "tenant_hash": 1, "event_id_hash": 10, "text": "a"},
                {"record_type": "context_event", "tenant_hash": 1, "event_id_hash": 11, "text": "bb"}
            ]
        }))
        .unwrap();
        let stats = command_stats(&command, &json!({"ok": true, "written": 2}));
        assert_eq!(stats.records_written, 2);
        assert!(stats.bytes_written > 0);
    }
}
