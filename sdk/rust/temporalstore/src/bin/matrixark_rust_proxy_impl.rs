use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde_json::{json, Value};
use temporalstore::Client;

#[path = "../matrixark_rust_proxy_cache.rs"]
mod matrixark_rust_proxy_cache;
#[path = "../matrixark_rust_proxy_candidates.rs"]
mod matrixark_rust_proxy_candidates;
#[path = "../matrixark_rust_proxy_command_stats.rs"]
mod matrixark_rust_proxy_command_stats;
#[path = "../matrixark_rust_proxy_metrics.rs"]
mod matrixark_rust_proxy_metrics;
#[path = "../matrixark_rust_proxy_pack.rs"]
mod matrixark_rust_proxy_pack;
#[path = "../matrixark_rust_proxy_protocol.rs"]
mod matrixark_rust_proxy_protocol;
#[path = "../matrixark_rust_proxy_records.rs"]
mod matrixark_rust_proxy_records;
#[path = "../matrixark_rust_proxy_retrieve.rs"]
mod matrixark_rust_proxy_retrieve;
#[path = "../matrixark_rust_proxy_runtime.rs"]
mod matrixark_rust_proxy_runtime;
#[path = "../matrixark_rust_proxy_scan.rs"]
mod matrixark_rust_proxy_scan;
#[path = "../matrixark_rust_proxy_scope.rs"]
mod matrixark_rust_proxy_scope;
use matrixark_rust_proxy_command_stats::{command_entries, command_stats};
use matrixark_rust_proxy_metrics::{matrixark_rust_service_mode, CommandStats, MetricsSnapshot};
use matrixark_rust_proxy_protocol::Command;
use matrixark_rust_proxy_runtime::{config_key, connect, required};
#[cfg(test)]
use matrixark_rust_proxy_records::{
    matrixark_context_event_time_field, matrixark_context_event_time_key,
    matrixark_context_event_time_payload, matrixark_record_id, matrixark_record_type,
    matrixark_storage_field, matrixark_storage_key, matrixark_tenant_hash,
};
use matrixark_rust_proxy_records::{read_matrixark_record, write_matrixark_record};
use matrixark_rust_proxy_retrieve::retrieve_context_pack_native;
use matrixark_rust_proxy_scan::scan_matrixark_candidates;

fn run_with_client(client: &Client, command: Command) -> Result<Value, String> {
    match command.op.as_str() {
        "put_string" => {
            client
                .put_string(
                    &required(command.key, "key")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "get_string" => {
            let value = client
                .get_string(&required(command.key, "key")?)
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        "hset" => {
            client
                .hset(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                    &required(command.value, "value")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true}))
        }
        "batch_hset" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &entries {
                client
                    .hset(entry.key, entry.field, entry.value)
                    .map_err(|err| err.to_string())?;
            }
            Ok(json!({"ok": true, "written": entries.len(), "batch_lowering": "raw_hset"}))
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let entries = command_entries(&command)?;
            if entries.is_empty()
                && command
                    .key
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err("missing entries".to_string());
            }
            let count_key = command.key.as_deref().filter(|value| !value.is_empty());
            let count_value = command.value.as_deref().filter(|value| !value.is_empty());
            let batch: Vec<(&str, &str, &str)> = entries
                .iter()
                .map(|entry| (entry.key, entry.field, entry.value))
                .collect();
            client
                .matrixark_batch_append_records(&batch, count_key, count_value)
                .map_err(|err| err.to_string())?;
            let mut written = entries.len();
            if count_key.is_some() && count_value.is_some() {
                written += 1;
            }
            let append_options = command.append_options.as_ref();
            let raw_backend = append_options
                .and_then(|options| options.get("raw_storage_backend"))
                .and_then(Value::as_str)
                .unwrap_or("temporalstore");
            let append_path = append_options
                .and_then(|options| options.get("append_path"))
                .and_then(Value::as_str)
                .unwrap_or("native_batch_append_records");
            Ok(json!({
                "ok": true,
                "written": written,
                "append_api": command.op,
                "native_append": true,
                "append_path": append_path,
                "raw_storage_backend": raw_backend,
                "batch_lowering": "none"
            }))
        }
        "batch_hget" => {
            let entries = command_entries(&command)?;
            if entries.is_empty() {
                return Err("missing entries".to_string());
            }
            let mut reads = Vec::with_capacity(entries.len());
            for entry in &entries {
                let value = client
                    .hget(entry.key, entry.field)
                    .map_err(|err| err.to_string())?;
                reads.push(json!({"key": entry.key, "field": entry.field, "value": value}));
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hgetall" | "scan_hash" => {
            let key = required(command.key, "key")?;
            let rows = client.scan_hash(&key).map_err(|err| err.to_string())?;
            let records: Vec<Value> = rows
                .iter()
                .map(|(field, value)| json!({"key": key, "field": field, "value": value}))
                .collect();
            Ok(
                json!({"ok": true, "count": records.len(), "read": records.len(), "records": records}),
            )
        }
        "matrixark_scan_candidates" => scan_matrixark_candidates(client, &command),
        "matrixark_retrieve_context_pack" => retrieve_context_pack_native(client, &command),
        "write_matrixark_record" => {
            let record = command
                .record
                .as_ref()
                .ok_or_else(|| "missing record".to_string())?;
            let write = write_matrixark_record(
                client,
                record,
                command.record_type.as_ref(),
                command.tenant_hash,
                command.record_id.as_ref(),
            )?;
            Ok(json!({"ok": true, "write": write}))
        }
        "write_matrixark_records" => {
            let records = command
                .records
                .as_ref()
                .ok_or_else(|| "missing records".to_string())?;
            let mut writes = Vec::with_capacity(records.len());
            for record in records {
                writes.push(write_matrixark_record(
                    client,
                    record,
                    command.record_type.as_ref(),
                    command.tenant_hash,
                    None,
                )?);
            }
            Ok(json!({"ok": true, "written": writes.len(), "writes": writes}))
        }
        "read_matrixark_record" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_id = required(command.record_id, "record_id")?;
            let read = read_matrixark_record(client, &record_type, tenant_hash, &record_id)?;
            Ok(json!({"ok": true, "read": read}))
        }
        "read_matrixark_records" => {
            let record_type = required(command.record_type, "record_type")?;
            let tenant_hash = command
                .tenant_hash
                .ok_or_else(|| "missing tenant_hash".to_string())?;
            let record_ids = command
                .record_ids
                .as_ref()
                .ok_or_else(|| "missing record_ids".to_string())?;
            let mut reads = Vec::with_capacity(record_ids.len());
            for record_id in record_ids {
                reads.push(read_matrixark_record(
                    client,
                    &record_type,
                    tenant_hash,
                    record_id,
                )?);
            }
            Ok(json!({"ok": true, "read": reads.len(), "records": reads}))
        }
        "hget" => {
            let value = client
                .hget(
                    &required(command.key, "key")?,
                    &required(command.field, "field")?,
                )
                .map_err(|err| err.to_string())?;
            Ok(json!({"ok": true, "value": value}))
        }
        other => Err(format!("unsupported op {other}")),
    }
}

fn run(command: Command) -> Result<Value, String> {
    let client = connect(&command)?;
    run_with_client(&client, command)
}

fn print_result(result: Result<Value, String>, engine_ms: u128) -> (bool, u128) {
    match result {
        Ok(mut value) => {
            if let Some(object) = value.as_object_mut() {
                object.insert("rust_engine_time_ms".to_string(), json!(engine_ms));
            }
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (true, total_ms)
        }
        Err(err) => {
            let mut value = json!({
                "ok": false,
                "error": err,
                "rust_engine_time_ms": engine_ms
            });
            let serialize_started = Instant::now();
            let _ = serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string());
            let serialization_ms = serialize_started.elapsed().as_millis();
            let total_ms = engine_ms + serialization_ms;
            if let Some(object) = value.as_object_mut() {
                object.insert("serialization_time_ms".to_string(), json!(serialization_ms));
                object.insert("elapsed_ms".to_string(), json!(total_ms));
            }
            println!("{}", value);
            (false, total_ms)
        }
    }
}

fn export_metrics_if_configured(metrics: &MetricsSnapshot) {
    let Ok(path) = std::env::var("MATRIXARK_RUST_METRICS_PATH") else {
        return;
    };
    if path.trim().is_empty() {
        return;
    }
    let _ = std::fs::write(path, metrics.render_prometheus());
}

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
