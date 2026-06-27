use std::collections::HashMap;
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
            "TemporalStore clients created by the long-lived Rust record-log bridge.",
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
            if command.key.as_ref().filter(|value| !value.is_empty()).is_some()
                && command.value.as_ref().filter(|value| !value.is_empty()).is_some()
            {
                stats.records_written += 1;
                stats.bytes_written += command.value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
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
    format!("{:020}:{}", matrixark_event_ingestion_time_ms(record), record_id)
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
    Ok(json!({"key": key, "field": field, "record_type": record_type, "record_id": record_id, "time_index": time_index}))
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
            println!("{}", json!({"ok": true, "status": "ok", "mode": "long_lived_stdio_gateway"}));
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
                    "mode": "long_lived_stdio_gateway",
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
            matrixark_context_event_time_field(&record, &matrixark_record_id(&record, None).unwrap()),
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
