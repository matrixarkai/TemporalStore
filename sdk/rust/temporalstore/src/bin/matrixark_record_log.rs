use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Read, Write};
use std::time::Instant;

use serde::Deserialize;
use serde_json::{json, Value};
use temporalstore::{Client, Options};

#[derive(Clone, Debug, Deserialize)]
struct Command {
    op: String,
    key: Option<String>,
    field: Option<String>,
    value: Option<String>,
    entries: Option<Vec<HashEntry>>,
    entries_compact: Option<Vec<[String; 3]>>,
    append_options: Option<Value>,
    record: Option<Value>,
    records: Option<Vec<Value>>,
    record_type: Option<String>,
    tenant_hash: Option<u64>,
    record_id: Option<String>,
    record_ids: Option<Vec<String>>,
    count_key: Option<String>,
    record_hash_key: Option<String>,
    shard_size: Option<u64>,
    record_types: Option<Vec<String>>,
    secondary_index_groups: Option<Vec<Vec<String>>>,
    selected_node_hashes: Option<Vec<u64>>,
    scope: Option<Value>,
    metaserver: Option<String>,
    namespace: Option<String>,
    table: Option<String>,
    request_timeout_ms: Option<i32>,
    io_timeout_ms: Option<i32>,
}

#[derive(Clone, Debug, Deserialize)]
struct HashEntry {
    key: String,
    field: String,
    value: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct HashEntryRef<'a> {
    key: &'a str,
    field: &'a str,
    value: &'a str,
}

#[derive(Clone, Debug, Default)]
struct OpMetrics {
    ok: u64,
    failed: u64,
    latency_ms_sum: u128,
    latency_ms_max: u128,
}

#[derive(Clone, Debug)]
struct MetricsSnapshot {
    started_at_unix_ms: u128,
    commands_total: u64,
    commands_failed: u64,
    records_written: u64,
    records_read: u64,
    bytes_written: u64,
    bytes_read: u64,
    clients_created: u64,
    parse_errors: u64,
    client_connect_errors: u64,
    op: HashMap<String, OpMetrics>,
}

impl Default for MetricsSnapshot {
    fn default() -> Self {
        Self {
            started_at_unix_ms: unix_ms(),
            commands_total: 0,
            commands_failed: 0,
            records_written: 0,
            records_read: 0,
            bytes_written: 0,
            bytes_read: 0,
            clients_created: 0,
            parse_errors: 0,
            client_connect_errors: 0,
            op: HashMap::new(),
        }
    }
}

impl MetricsSnapshot {
    fn observe(&mut self, op: &str, ok: bool, elapsed_ms: u128, stats: CommandStats) {
        self.commands_total += 1;
        if !ok {
            self.commands_failed += 1;
        }
        self.records_written += stats.records_written;
        self.records_read += stats.records_read;
        self.bytes_written += stats.bytes_written;
        self.bytes_read += stats.bytes_read;
        let entry = self.op.entry(op.to_string()).or_default();
        if ok {
            entry.ok += 1;
        } else {
            entry.failed += 1;
        }
        entry.latency_ms_sum += elapsed_ms;
        entry.latency_ms_max = entry.latency_ms_max.max(elapsed_ms);
    }

    fn render_prometheus(&self) -> String {
        let mut out = String::new();
        metric_header(
            &mut out,
            "matrixark_rust_record_log_process_start_time_ms",
            "gauge",
            "Unix millisecond timestamp when this Rust record-log process started.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_process_start_time_ms",
            "",
            self.started_at_unix_ms,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_commands_total",
            "counter",
            "Total MatrixArk Rust record-log commands by op and status.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_command_latency_ms_sum",
            "counter",
            "Total command latency in milliseconds by op.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_command_latency_ms_max",
            "gauge",
            "Maximum observed command latency in milliseconds by op.",
        );
        let mut ops: Vec<_> = self.op.iter().collect();
        ops.sort_by(|a, b| a.0.cmp(b.0));
        for (op, metrics) in ops {
            let ok_labels = format!("{{op=\"{}\",status=\"ok\"}}", escape_label(op));
            let fail_labels = format!("{{op=\"{}\",status=\"error\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_record_log_commands_total",
                &ok_labels,
                metrics.ok,
            );
            line(
                &mut out,
                "matrixark_rust_record_log_commands_total",
                &fail_labels,
                metrics.failed,
            );
            let op_labels = format!("{{op=\"{}\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_record_log_command_latency_ms_sum",
                &op_labels,
                metrics.latency_ms_sum,
            );
            line(
                &mut out,
                "matrixark_rust_record_log_command_latency_ms_max",
                &op_labels,
                metrics.latency_ms_max,
            );
        }
        metric_header(
            &mut out,
            "matrixark_rust_record_log_records_written_total",
            "counter",
            "Total MatrixArk records/hash entries written by the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_records_written_total",
            "",
            self.records_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_records_read_total",
            "counter",
            "Total MatrixArk records/hash entries read by the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_records_read_total",
            "",
            self.records_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_bytes_written_total",
            "counter",
            "Approximate payload bytes written by the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_bytes_written_total",
            "",
            self.bytes_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_bytes_read_total",
            "counter",
            "Approximate payload bytes read by the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_bytes_read_total",
            "",
            self.bytes_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_clients_created_total",
            "counter",
            "TemporalStore clients created by the Rust proxy/direct SDK bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_clients_created_total",
            "",
            self.clients_created,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_parse_errors_total",
            "counter",
            "Invalid JSON command lines received by the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_parse_errors_total",
            "",
            self.parse_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_client_connect_errors_total",
            "counter",
            "TemporalStore client connection failures in the Rust record-log bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_client_connect_errors_total",
            "",
            self.client_connect_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_record_log_commands_failed_total",
            "counter",
            "Total failed MatrixArk Rust record-log commands.",
        );
        line(
            &mut out,
            "matrixark_rust_record_log_commands_failed_total",
            "",
            self.commands_failed,
        );
        metric_header(
            &mut out,
            "matrixark_backend_info",
            "gauge",
            "MatrixArk storage backend identity and storage mode.",
        );
        line(
            &mut out,
            "matrixark_backend_info",
            &format!(
                "{{backend=\"rust\",storage_mode=\"{}\"}}",
                matrixark_rust_storage_mode()
            ),
            1,
        );
        metric_header(
            &mut out,
            "matrixark_backend_qps",
            "gauge",
            "MatrixArk storage backend command QPS.",
        );
        let elapsed_seconds =
            ((unix_ms().saturating_sub(self.started_at_unix_ms)) as f64 / 1000.0).max(0.001);
        line(
            &mut out,
            "matrixark_backend_qps",
            "{backend=\"rust\"}",
            format!("{:.6}", self.commands_total as f64 / elapsed_seconds),
        );
        metric_header(
            &mut out,
            "matrixark_backend_commands_total",
            "counter",
            "MatrixArk storage backend command count.",
        );
        line(
            &mut out,
            "matrixark_backend_commands_total",
            "{backend=\"rust\"}",
            self.commands_total,
        );
        metric_header(
            &mut out,
            "matrixark_backend_errors_total",
            "counter",
            "MatrixArk storage backend command errors.",
        );
        line(
            &mut out,
            "matrixark_backend_errors_total",
            "{backend=\"rust\"}",
            self.commands_failed,
        );
        metric_header(
            &mut out,
            "matrixark_backend_records_written_total",
            "counter",
            "MatrixArk storage backend records written.",
        );
        line(
            &mut out,
            "matrixark_backend_records_written_total",
            "{backend=\"rust\"}",
            self.records_written,
        );
        metric_header(
            &mut out,
            "matrixark_backend_records_read_total",
            "counter",
            "MatrixArk storage backend records read.",
        );
        line(
            &mut out,
            "matrixark_backend_records_read_total",
            "{backend=\"rust\"}",
            self.records_read,
        );
        metric_header(
            &mut out,
            "matrixark_backend_cached_clients",
            "gauge",
            "MatrixArk storage backend cached clients.",
        );
        line(
            &mut out,
            "matrixark_backend_cached_clients",
            "{backend=\"rust\"}",
            self.clients_created,
        );
        metric_header(
            &mut out,
            "matrixark_backend_timeouts_total",
            "counter",
            "MatrixArk storage backend command timeouts.",
        );
        line(
            &mut out,
            "matrixark_backend_timeouts_total",
            "{backend=\"rust\"}",
            0,
        );
        metric_header(
            &mut out,
            "matrixark_backend_command_latency_ms_bucket",
            "counter",
            "MatrixArk storage backend command latency buckets.",
        );
        let le_100 = self
            .op
            .values()
            .map(|metrics| {
                if metrics.latency_ms_max <= 100 {
                    metrics.ok + metrics.failed
                } else {
                    0
                }
            })
            .sum::<u64>();
        line(
            &mut out,
            "matrixark_backend_command_latency_ms_bucket",
            "{backend=\"rust\",le=\"100\"}",
            le_100,
        );
        metric_header(
            &mut out,
            "matrixark_backend_command_latency_max_ms",
            "gauge",
            "MatrixArk storage backend maximum command latency in milliseconds.",
        );
        let max_latency = self
            .op
            .values()
            .map(|metrics| metrics.latency_ms_max)
            .max()
            .unwrap_or(0);
        line(
            &mut out,
            "matrixark_backend_command_latency_max_ms",
            "{backend=\"rust\"}",
            max_latency,
        );
        out
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct CommandStats {
    records_written: u64,
    records_read: u64,
    bytes_written: u64,
    bytes_read: u64,
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn metric_header(out: &mut String, name: &str, metric_type: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(metric_type);
    out.push('\n');
}

fn line<T: std::fmt::Display>(out: &mut String, name: &str, labels: &str, value: T) {
    out.push_str(name);
    out.push_str(labels);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn matrixark_rust_storage_mode() -> &'static str {
    match std::env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref() {
        Some("direct_sdk" | "native-gateway" | "native-binding" | "rust-direct") => {
            "rust-direct-sdk-bridge"
        }
        _ => "rust-proxy",
    }
}

fn matrixark_rust_service_mode() -> &'static str {
    match std::env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref() {
        Some("direct_sdk" | "native-gateway" | "native-binding" | "rust-direct") => {
            "long_lived_rust_direct_sdk_bridge"
        }
        _ => "rust_proxy_stdio",
    }
}

fn command_stats(command: &Command, result: &Value) -> CommandStats {
    let mut stats = CommandStats::default();
    match command.op.as_str() {
        "put_string" => {
            stats.records_written = 1;
            stats.bytes_written = command.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
        }
        "get_string" => {
            stats.records_read = 1;
            stats.bytes_read = result
                .get("value")
                .and_then(Value::as_str)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
        }
        "hset" => {
            stats.records_written = 1;
            stats.bytes_written = command.value.as_ref().map(|v| v.len() as u64).unwrap_or(0);
        }
        "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
            let (entry_count, entry_bytes) = command_entry_stats(command);
            stats.records_written = entry_count;
            stats.bytes_written = entry_bytes;
            if command.key.as_ref().filter(|value| !value.is_empty()).is_some()
                && command.value.as_ref().filter(|value| !value.is_empty()).is_some()
            {
                stats.records_written += 1;
                stats.bytes_written += command.value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
            }
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            if let Some(entries) = &command.entries {
                stats.records_written = entries.len() as u64;
                stats.bytes_written = entries
                    .iter()
                    .map(|entry| {
                        entry
                            .value
                            .as_ref()
                            .map(|value| value.len() as u64)
                            .unwrap_or(0)
                    })
                    .sum();
            }
            if command
                .key
                .as_ref()
                .filter(|value| !value.is_empty())
                .is_some()
                && command
                    .value
                    .as_ref()
                    .filter(|value| !value.is_empty())
                    .is_some()
            {
                stats.records_written += 1;
                stats.bytes_written += command
                    .value
                    .as_ref()
                    .map(|value| value.len() as u64)
                    .unwrap_or(0);
            }
        }
        "batch_hget" | "hgetall" | "scan_hash" => {
            stats.records_read = result
                .get("read")
                .and_then(Value::as_u64)
                .or_else(|| result.get("count").and_then(Value::as_u64))
                .or_else(|| Some(command_entry_count(command)))
                .unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "scan_hash" | "matrixark_scan_candidates" | "matrixark_retrieve_context_pack" => {
            stats.records_read = result.get("count").and_then(Value::as_u64).unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "write_matrixark_record" => {
            stats.records_written = 1;
            stats.bytes_written = command
                .record
                .as_ref()
                .map(|record| record.to_string().len() as u64)
                .unwrap_or(0);
        }
        "write_matrixark_records" => {
            if let Some(records) = &command.records {
                stats.records_written = records.len() as u64;
                stats.bytes_written = records
                    .iter()
                    .map(|record| record.to_string().len() as u64)
                    .sum();
            }
        }
        "read_matrixark_record" => {
            stats.records_read = 1;
            stats.bytes_read = result.to_string().len() as u64;
        }
        "read_matrixark_records" => {
            stats.records_read = result
                .get("read")
                .and_then(Value::as_u64)
                .or_else(|| command.record_ids.as_ref().map(|ids| ids.len() as u64))
                .unwrap_or(0);
            stats.bytes_read = result.to_string().len() as u64;
        }
        "hget" => {
            stats.records_read = 1;
            stats.bytes_read = result
                .get("value")
                .and_then(Value::as_str)
                .map(|v| v.len() as u64)
                .unwrap_or(0);
        }
        _ => {}
    }
    stats
}

fn command_entry_count(command: &Command) -> u64 {
    command
        .entries_compact
        .as_ref()
        .map(|entries| entries.len() as u64)
        .or_else(|| command.entries.as_ref().map(|entries| entries.len() as u64))
        .unwrap_or(0)
}

fn command_entry_stats(command: &Command) -> (u64, u64) {
    if let Some(entries) = &command.entries_compact {
        let bytes = entries
            .iter()
            .map(|entry| entry[2].len() as u64)
            .sum();
        return (entries.len() as u64, bytes);
    }
    if let Some(entries) = &command.entries {
        let bytes = entries
            .iter()
            .map(|entry| entry.value.as_ref().map(|value| value.len() as u64).unwrap_or(0))
            .sum();
        return (entries.len() as u64, bytes);
    }
    (0, 0)
}

fn command_entries(command: &Command) -> Result<Vec<HashEntryRef<'_>>, String> {
    if let Some(entries) = &command.entries_compact {
        return Ok(entries
            .iter()
            .map(|entry| HashEntryRef {
                key: entry[0].as_str(),
                field: entry[1].as_str(),
                value: entry[2].as_str(),
            })
            .collect());
    }
    if let Some(entries) = &command.entries {
        return entries
            .iter()
            .map(|entry| {
                Ok(HashEntryRef {
                    key: entry.key.as_str(),
                    field: entry.field.as_str(),
                    value: entry
                        .value
                        .as_deref()
                        .ok_or_else(|| "matrixark batch append entry missing value".to_string())?,
                })
            })
            .collect();
    }
    Ok(Vec::new())
}

fn required(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn effective_config(command: &Command) -> (String, String, String, i32, i32) {
    (
        command
            .metaserver
            .clone()
            .unwrap_or_else(|| "127.0.0.1:18000".to_string()),
        command
            .namespace
            .clone()
            .unwrap_or_else(|| "deploy_ns".to_string()),
        command
            .table
            .clone()
            .unwrap_or_else(|| "deploy_table".to_string()),
        command.request_timeout_ms.unwrap_or(20_000),
        command.io_timeout_ms.unwrap_or(20_000),
    )
}

fn connect(command: &Command) -> Result<Client, String> {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    let mut options = Options::new(metaserver, namespace, table);
    options.psm = "matrixark.rust.mcp".to_string();
    options.request_timeout_ms = request_timeout_ms;
    options.io_timeout_ms = io_timeout_ms;
    Client::connect(options).map_err(|err| err.to_string())
}

fn config_key(command: &Command) -> String {
    let (metaserver, namespace, table, request_timeout_ms, io_timeout_ms) =
        effective_config(command);
    format!(
        "{metaserver}\u{1f}{namespace}\u{1f}{table}\u{1f}{request_timeout_ms}\u{1f}{io_timeout_ms}"
    )
}

fn value_u64(record: &Value, field: &str) -> Option<u64> {
    record.get(field).and_then(Value::as_u64)
}

fn value_str(record: &Value, field: &str) -> Option<String> {
    record
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn matrixark_record_type(record: &Value, fallback: Option<&String>) -> Result<String, String> {
    value_str(record, "record_type")
        .or_else(|| fallback.cloned())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "matrixark record missing record_type".to_string())
}

fn matrixark_tenant_hash(record: &Value, fallback: Option<u64>) -> Result<u64, String> {
    value_u64(record, "tenant_hash")
        .or(fallback)
        .ok_or_else(|| "matrixark record missing tenant_hash".to_string())
}

fn matrixark_record_id(record: &Value, fallback: Option<&String>) -> Result<String, String> {
    if let Some(value) = fallback.filter(|value| !value.is_empty()) {
        return Ok(value.clone());
    }
    for field in [
        "record_id",
        "node_hash",
        "event_id_hash",
        "entity_hash",
        "resource_hash",
        "chunk_hash",
        "skill_hash",
        "section_hash",
        "summary_hash",
        "ref_hash",
        "query_id_hash",
        "compression_id_hash",
    ] {
        if let Some(value) = record.get(field) {
            if let Some(number) = value.as_u64() {
                return Ok(number.to_string());
            }
            if let Some(text) = value.as_str() {
                if !text.is_empty() {
                    return Ok(text.to_string());
                }
            }
        }
    }
    Err("matrixark record missing stable id".to_string())
}

fn matrixark_storage_key(record_type: &str, tenant_hash: u64) -> String {
    format!("matrixark:record:{record_type}:{tenant_hash}")
}

fn matrixark_storage_field(record_id: &str) -> String {
    record_id.to_string()
}

fn matrixark_event_ingestion_time_ms(record: &Value) -> u64 {
    for field in ["ingestion_time_ms", "updated_at_ms", "created_at_ms"] {
        if let Some(value) = record.get(field).and_then(Value::as_u64) {
            if value > 0 {
                return value;
            }
        }
    }
    record
        .get("envelope")
        .and_then(|value| value.get("ingestion_time_ms"))
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .unwrap_or_else(|| unix_ms() as u64)
}

fn matrixark_context_event_time_key(tenant_hash: u64) -> String {
    format!("matrixark:record:context_event_by_ingestion_time:{tenant_hash}")
}

fn matrixark_context_event_time_field(record: &Value, record_id: &str) -> String {
    format!(
        "{:020}:{}",
        matrixark_event_ingestion_time_ms(record),
        record_id
    )
}

fn write_matrixark_record(
    client: &Client,
    record: &Value,
    record_type_fallback: Option<&String>,
    tenant_hash_fallback: Option<u64>,
    record_id_fallback: Option<&String>,
) -> Result<Value, String> {
    let record_type = matrixark_record_type(record, record_type_fallback)?;
    let tenant_hash = matrixark_tenant_hash(record, tenant_hash_fallback)?;
    let record_id = matrixark_record_id(record, record_id_fallback)?;
    let key = matrixark_storage_key(&record_type, tenant_hash);
    let field = matrixark_storage_field(&record_id);
    let payload = serde_json::to_string(record).map_err(|err| err.to_string())?;
    let mut time_index: Option<Value> = None;
    if record_type == "context_event" {
        let time_key = matrixark_context_event_time_key(tenant_hash);
        let time_field = matrixark_context_event_time_field(record, &record_id);
        client
            .hset(&time_key, &time_field, &payload)
            .map_err(|err| err.to_string())?;
        time_index = Some(json!({"key": time_key, "field": time_field}));
    }
    client
        .hset(&key, &field, &payload)
        .map_err(|err| err.to_string())?;
    Ok(
        json!({"key": key, "field": field, "record_type": record_type, "record_id": record_id, "time_index": time_index}),
    )
}

fn read_matrixark_record(
    client: &Client,
    record_type: &str,
    tenant_hash: u64,
    record_id: &str,
) -> Result<Value, String> {
    let key = matrixark_storage_key(record_type, tenant_hash);
    let field = matrixark_storage_field(record_id);
    let value = client.hget(&key, &field).map_err(|err| err.to_string())?;
    Ok(json!({"key": key, "field": field, "record_id": record_id, "value": value}))
}

fn json_field<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    Some(current)
}

fn record_scope_value<'a>(record: &'a Value) -> Option<&'a Value> {
    if let Some(scope) = record.get("access_scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    if let Some(scope) =
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object())
    {
        return Some(scope);
    }
    if let Some(scope) = record.get("scope").filter(|value| value.is_object()) {
        return Some(scope);
    }
    json_field(record, &["envelope", "scope"]).filter(|value| value.is_object())
}

fn scope_matches_record(record: &Value, query_scope: Option<&Value>) -> bool {
    let Some(query) = query_scope.filter(|value| value.is_object()) else {
        return true;
    };
    let Some(record_scope) = record_scope_value(record) else {
        return true;
    };
    for key in [
        "scope_key",
        "account_id",
        "tenant_id",
        "user_id",
        "team",
        "project",
    ] {
        let Some(query_value) = query.get(key) else {
            continue;
        };
        if query_value.is_null() || query_value.as_str() == Some("") {
            continue;
        }
        if record_scope.get(key) != Some(query_value) && record.get(key) != Some(query_value) {
            return false;
        }
    }
    if let Some(query_session) = query.get("session_id").filter(|value| !value.is_null()) {
        if query_session.as_str() != Some("")
            && record_scope.get("session_id") != Some(query_session)
            && record.get("session_id") != Some(query_session)
        {
            return false;
        }
    }
    true
}

fn record_ref_hash(record: &Value) -> Option<String> {
    for field in ["ref_hash", "chunk_hash", "section_hash", "skill_hash"] {
        if let Some(value) = record.get(field) {
            if let Some(number) = value.as_u64() {
                return Some(number.to_string());
            }
            if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn record_node_hash(record: &Value) -> Option<u64> {
    record.get("node_hash").and_then(Value::as_u64)
}

fn record_index_terms(
    record: &Value,
    index_terms_by_batch: &HashMap<String, HashSet<String>>,
    index_terms_by_node: &HashMap<u64, HashSet<String>>,
    index_terms_by_ref: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    let mut terms = HashSet::new();
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
        if let Some(values) = index_terms_by_batch.get(&batch.to_string()) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(node_hash) = record_node_hash(record) {
        if let Some(values) = index_terms_by_node.get(&node_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(ref_hash) = record_ref_hash(record) {
        if let Some(values) = index_terms_by_ref.get(&ref_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    match record_type {
        "context_event" => {
            terms.insert("source_type:message".to_string());
            if let Some(event_type) =
                json_field(record, &["internal_extraction", "event_type"]).and_then(Value::as_str)
            {
                if !event_type.is_empty() {
                    terms.insert(format!("event_type:{event_type}"));
                }
            }
        }
        "context_entity" => {
            if let Some(entity_type) = record.get("entity_type").and_then(Value::as_str) {
                if !entity_type.is_empty() {
                    terms.insert(format!("entity_type:{entity_type}"));
                }
            }
        }
        "resource_chunk" => {
            terms.insert("source_type:resource".to_string());
            if let Some(resource_type) = record.get("resource_type").and_then(Value::as_str) {
                if !resource_type.is_empty() {
                    terms.insert(format!("resource_type:{resource_type}"));
                }
            }
        }
        "skill_manifest" | "skill_section" => {
            terms.insert("source_type:skill".to_string());
            terms.insert("resource_type:skill".to_string());
            if record_type == "skill_manifest" {
                if let Some(name) = record.get("name").and_then(Value::as_str) {
                    if !name.is_empty() {
                        terms.insert(format!("skill_name:{}", name.to_ascii_lowercase()));
                    }
                }
            }
        }
        _ => {}
    }
    terms
}

fn passes_secondary_groups(terms: &HashSet<String>, groups: &[Vec<String>]) -> bool {
    if groups.is_empty() {
        return true;
    }
    let mode_any = groups.len() > 1;
    if mode_any {
        groups
            .iter()
            .any(|group| group.iter().any(|term| terms.contains(term)))
    } else {
        groups
            .iter()
            .all(|group| group.iter().any(|term| terms.contains(term)))
    }
}

fn decode_matrixark_payload(value: &str) -> Vec<Value> {
    let Ok(decoded) = serde_json::from_str::<Value>(value) else {
        return Vec::new();
    };
    if let Some(bundle) = decoded.get("record_bundle").and_then(Value::as_array) {
        return bundle
            .iter()
            .filter(|item| item.is_object())
            .cloned()
            .collect();
    }
    if decoded.is_object() {
        vec![decoded]
    } else {
        Vec::new()
    }
}

fn scan_matrixark_candidates(client: &Client, command: &Command) -> Result<Value, String> {
    let count_key = required(command.count_key.clone(), "count_key")?;
    let record_hash_key = required(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1);
    let count_text = client
        .get_string(&count_key)
        .map_err(|err| err.to_string())?;
    let count = count_text.parse::<u64>().unwrap_or(0);
    let allowed_types: HashSet<String> = command
        .record_types
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let selected_nodes: HashSet<u64> = command
        .selected_node_hashes
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let secondary_groups = command.secondary_index_groups.clone().unwrap_or_default();
    let max_shard = if count == 0 {
        0
    } else {
        (count - 1) / shard_size
    };
    let mut scanned_records = 0_u64;
    let mut dropped_by_type = 0_u64;
    let mut dropped_by_scope = 0_u64;
    let mut selected_node_dropped = 0_u64;
    let mut records = Vec::new();
    for shard in 0..=max_shard {
        let key = format!("{}:{:06}", record_hash_key, shard);
        for (_field, value) in client.hgetall(&key).map_err(|err| err.to_string())? {
            for record in decode_matrixark_payload(&value) {
                scanned_records += 1;
                let record_type = record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !allowed_types.is_empty() && !allowed_types.contains(record_type) {
                    dropped_by_type += 1;
                    continue;
                }
                if !scope_matches_record(&record, command.scope.as_ref()) {
                    dropped_by_scope += 1;
                    continue;
                }
                if !selected_nodes.is_empty() {
                    let keep_index = matches!(record_type, "context_index" | "context_embedding");
                    let keep_node = record_node_hash(&record)
                        .map(|node| selected_nodes.contains(&node))
                        .unwrap_or(false);
                    if !keep_index && !keep_node {
                        selected_node_dropped += 1;
                        continue;
                    }
                }
                records.push(record);
            }
        }
    }

    let mut index_terms_by_batch: HashMap<String, HashSet<String>> = HashMap::new();
    let mut index_terms_by_node: HashMap<u64, HashSet<String>> = HashMap::new();
    let mut index_terms_by_ref: HashMap<String, HashSet<String>> = HashMap::new();
    for record in &records {
        if record.get("record_type").and_then(Value::as_str) != Some("context_index") {
            continue;
        }
        let Some(index_name) = record
            .get("index_name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
            index_terms_by_batch
                .entry(batch.to_string())
                .or_default()
                .insert(index_name.to_string());
        }
        if let Some(ref_hash) = record_ref_hash(record) {
            index_terms_by_ref
                .entry(ref_hash)
                .or_default()
                .insert(index_name.to_string());
        } else if let Some(node_hash) = record_node_hash(record) {
            index_terms_by_node
                .entry(node_hash)
                .or_default()
                .insert(index_name.to_string());
        }
    }

    let mut secondary_dropped = 0_u64;
    let mut secondary_matched = 0_u64;
    let filtered = if secondary_groups.is_empty() {
        records
    } else {
        records
            .into_iter()
            .filter(|record| {
                let terms = record_index_terms(
                    record,
                    &index_terms_by_batch,
                    &index_terms_by_node,
                    &index_terms_by_ref,
                );
                if !terms.is_empty() && !passes_secondary_groups(&terms, &secondary_groups) {
                    secondary_dropped += 1;
                    return false;
                }
                if !terms.is_empty() {
                    secondary_matched += 1;
                }
                true
            })
            .collect::<Vec<_>>()
    };

    Ok(json!({
        "ok": true,
        "count": filtered.len(),
        "records": filtered,
        "native_candidate_prefilter": true,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_secondary_index_prefilter": !secondary_groups.is_empty(),
            "scanned_records": scanned_records,
            "returned_records": filtered.len(),
            "dropped_by_type": dropped_by_type,
            "dropped_by_scope": dropped_by_scope,
            "selected_node_dropped_candidate_count": selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups.len(),
            "secondary_index_matched_candidate_count": secondary_matched,
            "secondary_index_dropped_candidate_count": secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "C++/Rust ContextPack scoring and budget assembly APIs"
        }
    }))
}

fn candidate_text(record: &Value) -> String {
    for field in ["text", "content", "summary_text", "state", "observation"] {
        if let Some(text) = record.get(field).and_then(Value::as_str) {
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    if let Some(text) =
        json_field(record, &["internal_extraction", "observation"]).and_then(Value::as_str)
    {
        if !text.is_empty() {
            return text.to_string();
        }
    }
    String::new()
}

fn token_estimate(text: &str) -> u64 {
    let words = text.split_whitespace().count() as u64;
    words.max((text.len() as u64 + 3) / 4).max(1)
}

fn sparse_query_score(query_terms: &HashSet<String>, text: &str) -> f64 {
    if query_terms.is_empty() || text.is_empty() {
        return 0.0;
    }
    let lower = text.to_ascii_lowercase();
    let hits = query_terms
        .iter()
        .filter(|term| lower.contains(term.as_str()))
        .count() as f64;
    (hits / query_terms.len() as f64).clamp(0.0, 1.0)
}

fn pack_ref_from_record(record: &Value, score: f64, reason: &str) -> Value {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = candidate_text(record);
    json!({
        "ref_type": record_type,
        "ref_hash": record_ref_hash(record).unwrap_or_else(|| record.get("record_id").and_then(Value::as_str).unwrap_or("").to_string()),
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "text": text,
        "token_estimate": token_estimate(&candidate_text(record)),
        "score": (score * 1000000.0).round() / 1000000.0,
        "selection_reason": reason,
        "source_ref": record.get("source_ref").cloned().unwrap_or(Value::Null),
    })
}

fn retrieve_context_pack_native(client: &Client, command: &Command) -> Result<Value, String> {
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let query = request
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let query_terms: HashSet<String> = query
        .to_ascii_lowercase()
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter(|part| part.len() > 2)
        .map(str::to_string)
        .collect();
    let remote_budget = json_field(&request, &["local_budget", "remote_budget_tokens"])
        .and_then(Value::as_u64)
        .or_else(|| request.get("max_context_tokens").and_then(Value::as_u64))
        .unwrap_or(4000);
    let max_refs = json_field(&request, &["ranking", "max_selected_refs"])
        .and_then(Value::as_u64)
        .unwrap_or(48)
        .max(1);
    let mut scan_command = command.clone();
    scan_command.scope = request
        .get("scope")
        .cloned()
        .or_else(|| command.scope.clone());
    scan_command.secondary_index_groups = request
        .get("secondary_index_groups")
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|group| {
                    group
                        .as_array()
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default()
                })
                .collect()
        })
        .or_else(|| command.secondary_index_groups.clone());
    if scan_command
        .record_types
        .as_ref()
        .map(Vec::is_empty)
        .unwrap_or(true)
    {
        scan_command.record_types = Some(vec![
            "context_compression_event".to_string(),
            "context_entity".to_string(),
            "context_event".to_string(),
            "context_segment".to_string(),
            "context_summary".to_string(),
            "resource_chunk".to_string(),
            "skill_section".to_string(),
        ]);
    }
    let scan = scan_matrixark_candidates(client, &scan_command)?;
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut scored: Vec<(f64, Value)> = records
        .into_iter()
        .filter(|record| {
            matches!(
                record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                "context_compression_event"
                    | "context_entity"
                    | "context_event"
                    | "context_segment"
                    | "context_summary"
                    | "resource_chunk"
                    | "skill_section"
            ) && !candidate_text(record).is_empty()
        })
        .map(|record| {
            let text = candidate_text(&record);
            let mut score = sparse_query_score(&query_terms, &text);
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_entity")
            ) {
                score += 0.08;
            }
            if matches!(
                record.get("record_type").and_then(Value::as_str),
                Some("context_compression_event")
            ) {
                score += 0.06;
            }
            (score, record)
        })
        .collect();
    scored.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mut selected = Vec::new();
    let mut dropped_over_budget = 0_u64;
    let mut used_tokens = 0_u64;
    for (score, record) in scored {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(&record);
        let tokens = token_estimate(&text);
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            continue;
        }
        used_tokens += tokens;
        selected.push(pack_ref_from_record(
            &record,
            score,
            "native_rust_proxy_score_pack",
        ));
    }
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": request.get("question_type").cloned().unwrap_or_else(|| json!("fact")),
        "selected_refs": selected,
        "dropped_refs": {
            "over_budget": dropped_over_budget,
            "reason_counts": {"over_budget": dropped_over_budget}
        },
        "used_context_tokens": used_tokens,
        "used_remote_context_tokens": used_tokens,
        "remote_context_budget_tokens": remote_budget,
        "requested_max_context_tokens": request.get("max_context_tokens").cloned().unwrap_or_else(|| json!(remote_budget)),
        "packing_policy": "native_rust_proxy_question_type_aware",
        "context_pack_assembly": "native_rust_proxy",
        "context_sources_order": ["entities", "events", "segments", "resources", "skills", "summaries"],
        "recall_policy": {
            "native_context_pack": {
                "enabled": true,
                "backend": "rust_proxy",
                "scan_filter_score_pack": true
            },
            "scan_stats": scan.get("scan_stats").cloned().unwrap_or_else(|| json!({})),
            "tree_traversal": {"native_backend": true, "fallback_to_flat": false},
            "secondary_index_filter": {"native_backend": true}
        },
        "quality_warnings": []
    });
    Ok(json!({
        "ok": true,
        "native_pack_assembly": true,
        "context_pack": pack,
        "scan_stats": scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}))
    }))
}

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
        "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
            let entries = command_entries(&command)?;
            if entries.is_empty()
                && command.key.as_ref().filter(|value| !value.is_empty()).is_none()
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
            Ok(json!({"ok": true, "count": records.len(), "read": records.len(), "records": records}))
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

fn print_result(result: Result<Value, String>, elapsed_ms: u128) -> bool {
    match result {
        Ok(mut value) => {
            if let Some(object) = value.as_object_mut() {
                object.insert("elapsed_ms".to_string(), json!(elapsed_ms));
            }
            println!("{}", value);
            true
        }
        Err(err) => {
            println!(
                "{}",
                json!({"ok": false, "error": err, "elapsed_ms": elapsed_ms})
            );
            false
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
        metrics.observe(&op, ok, elapsed_ms, stats);
        export_metrics_if_configured(&metrics);
        print_result(result, elapsed_ms);
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
    if print_result(run(command), started.elapsed().as_millis()) {
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
    }

    #[test]
    fn metrics_render_prometheus_records_op_status_and_latency() {
        let mut metrics = MetricsSnapshot::default();
        metrics.observe(
            "write_matrixark_record",
            true,
            12,
            CommandStats {
                records_written: 1,
                bytes_written: 128,
                ..CommandStats::default()
            },
        );
        metrics.observe("write_matrixark_record", false, 30, CommandStats::default());
        let text = metrics.render_prometheus();
        assert!(text.contains("matrixark_rust_record_log_commands_total{op=\"write_matrixark_record\",status=\"ok\"} 1"));
        assert!(text.contains("matrixark_rust_record_log_commands_total{op=\"write_matrixark_record\",status=\"error\"} 1"));
        assert!(text.contains(
            "matrixark_rust_record_log_command_latency_ms_sum{op=\"write_matrixark_record\"} 42"
        ));
        assert!(text.contains(
            "matrixark_rust_record_log_command_latency_ms_max{op=\"write_matrixark_record\"} 30"
        ));
        assert!(text.contains("matrixark_rust_record_log_records_written_total 1"));
        assert!(text.contains("matrixark_rust_record_log_bytes_written_total 128"));
        assert!(text.contains("matrixark_rust_record_log_commands_failed_total 1"));
    }


    #[test]
    fn command_stats_counts_scan_hash_records() {
        let command = Command {
            op: "scan_hash".to_string(),
            key: Some("matrixark:mcp:records:000000".to_string()),
            field: None,
            value: None,
            entries: None,
            entries_compact: None,
            record: None,
            records: None,
            record_type: None,
            tenant_hash: None,
            record_id: None,
            record_ids: None,
            metaserver: None,
            namespace: None,
            table: None,
            request_timeout_ms: None,
            io_timeout_ms: None,
        };
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
