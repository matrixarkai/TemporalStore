// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Read, Write};
use std::sync::{Arc, Mutex, OnceLock};
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
    route_json: Option<String>,
    storage_route: Option<Value>,
}

#[derive(Clone, Debug)]
struct HashEntryRef<'a> {
    key: &'a str,
    field: &'a str,
    value: &'a str,
    route_json: Cow<'a, str>,
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
    rust_engine_time_ms_sum: u128,
    rust_engine_time_ms_max: u128,
    serialization_time_ms_sum: u128,
    serialization_time_ms_max: u128,
    scan_count_total: u64,
    cache_hit_total: u64,
    selected_refs_total: u64,
    dropped_refs_total: u64,
    matrixark_append_blob_parity_total: u64,
    matrixark_append_hset_count_lowering_total: u64,
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
            rust_engine_time_ms_sum: 0,
            rust_engine_time_ms_max: 0,
            serialization_time_ms_sum: 0,
            serialization_time_ms_max: 0,
            scan_count_total: 0,
            cache_hit_total: 0,
            selected_refs_total: 0,
            dropped_refs_total: 0,
            matrixark_append_blob_parity_total: 0,
            matrixark_append_hset_count_lowering_total: 0,
            op: HashMap::new(),
        }
    }
}

impl MetricsSnapshot {
    fn observe(
        &mut self,
        op: &str,
        ok: bool,
        elapsed_ms: u128,
        serialization_ms: u128,
        result: Option<&Value>,
        stats: CommandStats,
    ) {
        self.commands_total += 1;
        if !ok {
            self.commands_failed += 1;
        }
        self.rust_engine_time_ms_sum += elapsed_ms;
        self.rust_engine_time_ms_max = self.rust_engine_time_ms_max.max(elapsed_ms);
        self.serialization_time_ms_sum += serialization_ms;
        self.serialization_time_ms_max = self.serialization_time_ms_max.max(serialization_ms);
        if let Some(result) = result {
            self.scan_count_total += result
                .get("scan_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if result
                .get("cache_hit")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                self.cache_hit_total += 1;
            }
            self.selected_refs_total += result
                .get("selected_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            self.dropped_refs_total += result
                .get("dropped_ref_count")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if matches!(
                op,
                "matrixark_append_records" | "matrixark_batch_append_records"
            ) {
                if result
                    .get("append_blob_parity")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    self.matrixark_append_blob_parity_total += 1;
                }
                if result
                    .get("batch_lowering")
                    .and_then(Value::as_str)
                    .map(|value| value == "rust_proxy_hset_count_lowering")
                    .unwrap_or(false)
                {
                    self.matrixark_append_hset_count_lowering_total += 1;
                }
            }
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
            "matrixark_rust_proxy_process_start_time_ms",
            "gauge",
            "Unix millisecond timestamp when this Rust proxy process started.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_process_start_time_ms",
            "",
            self.started_at_unix_ms,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_commands_total",
            "counter",
            "Total MatrixArk Rust proxy commands by op and status.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_command_latency_ms_sum",
            "counter",
            "Total command latency in milliseconds by op.",
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_command_latency_ms_max",
            "gauge",
            "Maximum observed command latency in milliseconds by op.",
        );
        metric_header(
            &mut out,
            "matrixark_backend_rust_engine_time_ms_total",
            "counter",
            "Total Rust engine execution time in milliseconds.",
        );
        line(
            &mut out,
            "matrixark_backend_rust_engine_time_ms_total",
            "{backend=\"rust\"}",
            self.rust_engine_time_ms_sum,
        );
        metric_header(
            &mut out,
            "matrixark_backend_serialization_time_ms_total",
            "counter",
            "Total Rust proxy response serialization time in milliseconds.",
        );
        line(
            &mut out,
            "matrixark_backend_serialization_time_ms_total",
            "{backend=\"rust\"}",
            self.serialization_time_ms_sum,
        );
        metric_header(
            &mut out,
            "matrixark_retrieve_scan_count_total",
            "counter",
            "Total records scanned by native MatrixArk retrieval calls.",
        );
        line(
            &mut out,
            "matrixark_retrieve_scan_count_total",
            "{backend=\"rust\"}",
            self.scan_count_total,
        );
        metric_header(
            &mut out,
            "matrixark_retrieve_cache_hits_total",
            "counter",
            "Total native MatrixArk retrieval cache hits.",
        );
        line(
            &mut out,
            "matrixark_retrieve_cache_hits_total",
            "{backend=\"rust\"}",
            self.cache_hit_total,
        );
        metric_header(
            &mut out,
            "matrixark_context_pack_selected_refs_total",
            "counter",
            "Total refs selected by native MatrixArk ContextPack assembly.",
        );
        line(
            &mut out,
            "matrixark_context_pack_selected_refs_total",
            "{backend=\"rust\"}",
            self.selected_refs_total,
        );
        metric_header(
            &mut out,
            "matrixark_context_pack_dropped_refs_total",
            "counter",
            "Total refs dropped by native MatrixArk ContextPack assembly.",
        );
        line(
            &mut out,
            "matrixark_context_pack_dropped_refs_total",
            "{backend=\"rust\"}",
            self.dropped_refs_total,
        );
        let mut ops: Vec<_> = self.op.iter().collect();
        ops.sort_by(|a, b| a.0.cmp(b.0));
        for (op, metrics) in ops {
            let ok_labels = format!("{{op=\"{}\",status=\"ok\"}}", escape_label(op));
            let fail_labels = format!("{{op=\"{}\",status=\"error\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_proxy_commands_total",
                &ok_labels,
                metrics.ok,
            );
            line(
                &mut out,
                "matrixark_rust_proxy_commands_total",
                &fail_labels,
                metrics.failed,
            );
            let op_labels = format!("{{op=\"{}\"}}", escape_label(op));
            line(
                &mut out,
                "matrixark_rust_proxy_command_latency_ms_sum",
                &op_labels,
                metrics.latency_ms_sum,
            );
            line(
                &mut out,
                "matrixark_rust_proxy_command_latency_ms_max",
                &op_labels,
                metrics.latency_ms_max,
            );
        }
        metric_header(
            &mut out,
            "matrixark_rust_proxy_records_written_total",
            "counter",
            "Total MatrixArk records/hash entries written by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_records_written_total",
            "",
            self.records_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_records_read_total",
            "counter",
            "Total MatrixArk records/hash entries read by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_records_read_total",
            "",
            self.records_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_bytes_written_total",
            "counter",
            "Approximate payload bytes written by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_bytes_written_total",
            "",
            self.bytes_written,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_bytes_read_total",
            "counter",
            "Approximate payload bytes read by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_bytes_read_total",
            "",
            self.bytes_read,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_clients_created_total",
            "counter",
            "TemporalStore clients created by the Rust proxy/direct SDK bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_clients_created_total",
            "",
            self.clients_created,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_parse_errors_total",
            "counter",
            "Invalid JSON command lines received by the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_parse_errors_total",
            "",
            self.parse_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_client_connect_errors_total",
            "counter",
            "TemporalStore client connection failures in the Rust proxy bridge.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_client_connect_errors_total",
            "",
            self.client_connect_errors,
        );
        metric_header(
            &mut out,
            "matrixark_rust_proxy_commands_failed_total",
            "counter",
            "Total failed MatrixArk Rust proxy commands.",
        );
        line(
            &mut out,
            "matrixark_rust_proxy_commands_failed_total",
            "",
            self.commands_failed,
        );
        metric_header(
            &mut out,
            "matrixark_append_blob_parity_total",
            "counter",
            "MatrixArk Rust proxy append commands that used append-blob parity semantics.",
        );
        line(
            &mut out,
            "matrixark_append_blob_parity_total",
            "{backend=\"rust\"}",
            self.matrixark_append_blob_parity_total,
        );
        metric_header(
            &mut out,
            "matrixark_append_hset_count_lowering_total",
            "counter",
            "MatrixArk Rust proxy append commands lowered to hset plus count updates.",
        );
        line(
            &mut out,
            "matrixark_append_hset_count_lowering_total",
            "{backend=\"rust\"}",
            self.matrixark_append_hset_count_lowering_total,
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

#[derive(Clone, Debug)]
struct ScanRecordCacheEntry {
    records: Arc<Vec<Value>>,
    scanned_records: u64,
}

#[derive(Clone, Debug)]
struct FilteredScanCacheEntry {
    records: Vec<Value>,
    scanned_records: u64,
    dropped_by_type: u64,
    dropped_by_scope: u64,
    dropped_by_retention: u64,
    selected_node_dropped: u64,
    secondary_dropped: u64,
    secondary_matched: u64,
    node_path_filter_count: usize,
}

static SCAN_RECORD_CACHE: OnceLock<Mutex<HashMap<String, ScanRecordCacheEntry>>> = OnceLock::new();
static FILTERED_SCAN_CACHE: OnceLock<Mutex<HashMap<String, FilteredScanCacheEntry>>> =
    OnceLock::new();

fn scan_record_cache() -> &'static Mutex<HashMap<String, ScanRecordCacheEntry>> {
    SCAN_RECORD_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn filtered_scan_cache() -> &'static Mutex<HashMap<String, FilteredScanCacheEntry>> {
    FILTERED_SCAN_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn max_scan_record_cache_entries() -> usize {
    std::env::var("MATRIXARK_RUST_SCAN_RECORD_CACHE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(8)
}

fn max_filtered_scan_cache_entries() -> usize {
    std::env::var("MATRIXARK_RUST_FILTERED_SCAN_CACHE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(32)
}

fn scan_record_cache_key(record_hash_key: &str, shard_size: u64, count: u64) -> String {
    format!("{record_hash_key}\u{1f}{shard_size}\u{1f}{count}")
}

fn filtered_scan_cache_key(
    raw_cache_key: &str,
    allowed_types: &HashSet<String>,
    selected_nodes: &HashSet<u64>,
    secondary_groups: &[Vec<String>],
    scope: Option<&Value>,
) -> String {
    let mut types: Vec<&str> = allowed_types.iter().map(String::as_str).collect();
    types.sort_unstable();
    let mut nodes: Vec<u64> = selected_nodes.iter().copied().collect();
    nodes.sort_unstable();
    let scope_text = scope
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_default();
    let secondary_text = serde_json::to_string(secondary_groups).unwrap_or_default();
    format!(
        "{raw_cache_key}\u{1e}types={}\u{1e}nodes={:?}\u{1e}scope={scope_text}\u{1e}secondary={secondary_text}",
        types.join(","),
        nodes
    )
}

fn get_scan_record_cache(key: &str) -> Option<ScanRecordCacheEntry> {
    scan_record_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key).cloned())
}

fn put_scan_record_cache(key: String, entry: ScanRecordCacheEntry) {
    let Ok(mut cache) = scan_record_cache().lock() else {
        return;
    };
    let max_entries = max_scan_record_cache_entries();
    if cache.len() >= max_entries && !cache.contains_key(&key) {
        if let Some(first_key) = cache.keys().next().cloned() {
            cache.remove(&first_key);
        }
    }
    cache.insert(key, entry);
}

fn get_filtered_scan_cache(key: &str) -> Option<FilteredScanCacheEntry> {
    filtered_scan_cache()
        .lock()
        .ok()
        .and_then(|cache| cache.get(key).cloned())
}

fn put_filtered_scan_cache(key: String, entry: FilteredScanCacheEntry) {
    let Ok(mut cache) = filtered_scan_cache().lock() else {
        return;
    };
    let max_entries = max_filtered_scan_cache_entries();
    if cache.len() >= max_entries && !cache.contains_key(&key) {
        if let Some(first_key) = cache.keys().next().cloned() {
            cache.remove(&first_key);
        }
    }
    cache.insert(key, entry);
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

fn matrixark_rust_sdk_mode_is_direct() -> bool {
    matches!(
        std::env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref(),
        Some("direct_sdk" | "direct-sdk" | "native-gateway" | "native-binding" | "rust-direct")
    ) || std::env::args()
        .next()
        .map(|arg| arg.contains("matrixark_rust_direct_sdk"))
        .unwrap_or(false)
}

fn matrixark_rust_storage_mode() -> &'static str {
    if matrixark_rust_sdk_mode_is_direct() {
        "rust-direct-sdk-bridge"
    } else {
        "rust-proxy"
    }
}

fn matrixark_rust_service_mode() -> &'static str {
    if matrixark_rust_sdk_mode_is_direct() {
        "long_lived_rust_direct_sdk_bridge"
    } else {
        "rust_proxy_stdio"
    }
}

fn native_matrixark_c_api_bridge_enabled() -> bool {
    std::env::var("TEMPORALSTORE_RUST_ALLOW_NATIVE_MATRIXARK_C_API")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
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
        "batch_hset" => {
            let (entry_count, entry_bytes) = command_entry_stats(command);
            stats.records_written = entry_count;
            stats.bytes_written = entry_bytes;
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let (records, bytes) = hash_entry_stats(command);
            stats.records_written = records;
            stats.bytes_written = bytes;
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
        let bytes = entries.iter().map(|entry| entry[2].len() as u64).sum();
        return (entries.len() as u64, bytes);
    }
    if let Some(entries) = &command.entries {
        let bytes = entries
            .iter()
            .map(|entry| {
                entry
                    .value
                    .as_ref()
                    .map(|value| value.len() as u64)
                    .unwrap_or(0)
            })
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
                route_json: Cow::Borrowed("{}"),
            })
            .collect());
    }
    if let Some(entries) = &command.entries {
        return entries
            .iter()
            .map(|entry| {
                let route_json = entry
                    .route_json
                    .as_deref()
                    .filter(|value| !value.is_empty())
                    .map(Cow::Borrowed)
                    .or_else(|| {
                        entry
                            .storage_route
                            .as_ref()
                            .map(|value| Cow::Owned(value.to_string()))
                    })
                    .unwrap_or_else(|| Cow::Borrowed("{}"));
                Ok(HashEntryRef {
                    key: entry.key.as_str(),
                    field: entry.field.as_str(),
                    value: entry
                        .value
                        .as_deref()
                        .ok_or_else(|| "matrixark batch append entry missing value".to_string())?,
                    route_json,
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

fn hash_entry_stats(command: &Command) -> (u64, u64) {
    let mut records = 0_u64;
    let mut bytes = 0_u64;
    if let Some(entries) = &command.entries {
        for entry in entries {
            if let Some(value) = entry.value.as_ref() {
                records += 1;
                bytes += value.len() as u64;
            }
        }
    }
    if let Some(entries) = &command.entries_compact {
        for entry in entries {
            records += 1;
            bytes += entry[2].len() as u64;
        }
    }
    (records, bytes)
}

fn expanded_hash_entries(command: &Command) -> Vec<(String, String, String)> {
    let mut expanded = Vec::new();
    if let Some(entries) = &command.entries {
        expanded.extend(entries.iter().filter_map(|entry| {
            entry
                .value
                .as_ref()
                .map(|value| (entry.key.clone(), entry.field.clone(), value.clone()))
        }));
    }
    if let Some(entries) = &command.entries_compact {
        expanded.extend(
            entries
                .iter()
                .map(|entry| (entry[0].clone(), entry[1].clone(), entry[2].clone())),
        );
    }
    expanded
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

fn matrixark_context_event_time_payload(record: &Value) -> Result<String, String> {
    let mut payload = record.clone();
    if let Some(object) = payload.as_object_mut() {
        object.remove("event_time_key");
        object.remove("ingestion_time_ms");
    }
    serde_json::to_string(&payload).map_err(|err| err.to_string())
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
        let time_payload = matrixark_context_event_time_payload(record)?;
        client
            .hset(&time_key, &time_field, &time_payload)
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

fn session_scope_mode(query: &Value) -> &str {
    match query
        .get("_session_scope")
        .or_else(|| query.get("session_scope"))
        .and_then(Value::as_str)
        .unwrap_or("only")
    {
        "prefer" | "preferred" | "soft" | "continuity" => "prefer",
        _ => "only",
    }
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
    if session_scope_mode(query) == "only" {
        if let Some(query_session) = query.get("session_id").filter(|value| !value.is_null()) {
            if query_session.as_str() != Some("")
                && record_scope.get("session_id") != Some(query_session)
                && record.get("session_id") != Some(query_session)
            {
                return false;
            }
        }
    }
    true
}

fn record_scope_string(record: &Value, field: &str) -> Option<String> {
    for source in [record_scope_value(record), record.get("scope")] {
        if let Some(value) = source
            .and_then(|scope| scope.get(field))
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            return Some(value.to_string());
        }
    }
    record
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn session_continuity_status(record: &Value, query_scope: Option<&Value>) -> String {
    let Some(query) = query_scope else {
        return "unscoped".to_string();
    };
    let Some(query_session) = query
        .get("session_id")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    else {
        return "unscoped".to_string();
    };
    if record_scope_string(record, "session_id").as_deref() == Some(query_session) {
        return "same_session".to_string();
    }
    let has_sessionish_scope = record_scope_string(record, "scope_key").is_some()
        || record_scope_string(record, "session_id").is_some();
    if has_sessionish_scope {
        "cross_session".to_string()
    } else {
        "unscoped".to_string()
    }
}

fn continuity_boost(record: &Value, context_class: &str, status: &str) -> f64 {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    match status {
        "same_session" => match record_type {
            "context_event" | "context_segment" => 0.16,
            "context_summary" => 0.12,
            "context_entity" => 0.10,
            _ => 0.08,
        },
        "cross_session" => {
            if record_type == "context_entity" || context_class == "resource_fact" {
                0.11
            } else if matches!(
                record_type,
                "context_event" | "context_segment" | "context_compression_event"
            ) {
                0.06
            } else {
                0.0
            }
        }
        _ => 0.0,
    }
}

fn cross_session_rerank_boost(
    record: &Value,
    context_class: &str,
    status: &str,
    question_type: &str,
) -> f64 {
    if status != "cross_session" {
        return 0.0;
    }
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    let has_citation = record.get("source_ref").is_some()
        || record.get("citation").is_some()
        || record.get("source_chunk_hash").is_some();
    match record_type {
        "context_entity" => {
            if matches!(question_type, "current_state" | "latest" | "multi_hop") {
                0.10
            } else {
                0.06
            }
        }
        "resource_chunk" if has_citation => 0.04,
        "context_event" | "context_segment"
            if matches!(
                question_type,
                "multi_hop" | "why_emotion" | "fact" | "evidence"
            ) =>
        {
            0.01
        }
        "context_compression_event" => 0.05,
        "context_summary" => {
            if question_type == "broad_exploration" {
                0.05
            } else {
                0.02
            }
        }
        _ if matches!(context_class, "resource_fact" | "resource_entity_fact") => {
            if has_citation {
                0.06
            } else {
                0.04
            }
        }
        _ => 0.0,
    }
}

fn cross_session_key(record: &Value) -> String {
    record_scope_string(record, "session_id")
        .or_else(|| record_scope_string(record, "scope_key"))
        .or_else(|| record_node_hash(record).map(|node| format!("node:{node}")))
        .unwrap_or_else(|| "unknown_cross_session".to_string())
}

#[derive(Clone, Debug)]
struct CrossSessionPolicy {
    enabled: bool,
    budget_ratio: f64,
    budget_tokens: u64,
    max_budget_ratio: f64,
    max_budget_tokens: u64,
    max_sessions: u64,
    max_candidates: u64,
    min_score: f64,
    raw_evidence_min_score: f64,
    min_entity_bridge_refs: u64,
    parallelism: u64,
}

fn native_question_contains_any(lower: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| lower.contains(needle))
}

fn infer_native_question_type(query: &str) -> String {
    let lower = query.to_ascii_lowercase();
    if native_question_contains_any(
        &lower,
        &[
            "when",
            "what date",
            "which date",
            "yesterday",
            "tomorrow",
            "last week",
            "next week",
            "before",
            "after",
            "as of",
            "valid as of",
        ],
    ) {
        return "date".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "current",
            "currently",
            "latest",
            "now",
            "still",
            "today",
            "valid",
            "status",
            "preference",
            "prefer",
            "likes",
            "where does",
            "where is",
        ],
    ) {
        return "current_state".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "why", "reason", "because", "feel", "felt", "emotion", "happy", "sad", "angry",
            "worried", "excited",
        ],
    ) {
        return "why_emotion".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "overview",
            "summarize",
            "summary",
            "explore",
            "broad",
            "what is in",
            "what do we know",
            "topics",
            "map",
            "inventory",
        ],
    ) {
        return "broad_exploration".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "evidence",
            "quote",
            "exactly",
            "what did ",
            "conversation",
            "dialogue",
            "message",
        ],
    ) {
        return "evidence".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "procedure",
            "step",
            "steps",
            "how to",
            "troubleshoot",
            "debug",
            "rollback",
            "runbook",
            "playbook",
            "checklist",
            "fix",
            "remediate",
            "mitigate",
        ],
    ) {
        return "procedure".to_string();
    }
    if native_question_contains_any(
        &lower,
        &[
            "both",
            "together",
            "across",
            "between",
            "compare",
            "combine",
            "sessions",
            "multi-hop",
            "multi session",
            "multi-session",
        ],
    ) {
        return "multi_hop".to_string();
    }
    "fact".to_string()
}

fn parse_cross_session_policy(
    request: &Value,
    scope: Option<&Value>,
    remote_budget: u64,
    question_type: &str,
) -> CrossSessionPolicy {
    let default_enabled = scope.map(session_scope_mode) == Some("prefer") && remote_budget > 0;
    let config = request
        .get("cross_session")
        .filter(|value| value.is_object());
    let mut budget_ratio = if matches!(
        question_type,
        "current_state" | "latest" | "multi_hop" | "date"
    ) {
        0.20
    } else if matches!(question_type, "broad_exploration" | "evidence") {
        0.15
    } else {
        0.12
    };
    let max_budget_ratio = config
        .and_then(|cfg| cfg.get("max_budget_ratio"))
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    if budget_ratio > max_budget_ratio {
        budget_ratio = max_budget_ratio;
    }
    if let Some(value) = config
        .and_then(|cfg| cfg.get("budget_ratio"))
        .and_then(Value::as_f64)
    {
        budget_ratio = value.clamp(0.0, max_budget_ratio);
    }
    let max_budget_tokens = config
        .and_then(|cfg| cfg.get("max_budget_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(8192);
    let mut computed = (remote_budget as f64 * budget_ratio) as u64;
    if remote_budget >= 1200 && computed > 0 {
        computed = computed.max(256);
    }
    let enabled = config
        .and_then(|cfg| cfg.get("enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(default_enabled)
        && default_enabled;
    let mut budget_tokens = config
        .and_then(|cfg| cfg.get("budget_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(computed);
    let mut max_sessions = config
        .and_then(|cfg| cfg.get("max_sessions"))
        .and_then(Value::as_u64)
        .unwrap_or(3);
    let mut max_candidates = config
        .and_then(|cfg| cfg.get("max_candidates"))
        .and_then(Value::as_u64)
        .unwrap_or(24);
    let mut min_score = config
        .and_then(|cfg| cfg.get("min_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let mut raw_evidence_min_score = config
        .and_then(|cfg| cfg.get("raw_evidence_min_score"))
        .and_then(Value::as_f64)
        .unwrap_or(0.45)
        .clamp(0.0, 1.0);
    let mut min_entity_bridge_refs = config
        .and_then(|cfg| cfg.get("min_entity_bridge_refs"))
        .and_then(Value::as_u64)
        .unwrap_or(2);
    let mut parallelism = config
        .and_then(|cfg| cfg.get("parallelism"))
        .and_then(Value::as_u64)
        .unwrap_or(4)
        .max(1);
    if !enabled {
        budget_tokens = 0;
        max_sessions = 0;
        max_candidates = 0;
        min_score = 0.0;
        raw_evidence_min_score = 0.0;
        min_entity_bridge_refs = 0;
        parallelism = 0;
    } else {
        let cap = if max_budget_tokens == 0 {
            remote_budget
        } else {
            max_budget_tokens
        };
        let mut ratio_cap = if max_budget_ratio > 0.0 {
            (remote_budget as f64 * max_budget_ratio) as u64
        } else {
            remote_budget
        };
        if ratio_cap == 0 && remote_budget > 0 && max_budget_ratio > 0.0 {
            ratio_cap = 1;
        }
        budget_tokens = budget_tokens.min(remote_budget).min(cap).min(ratio_cap);
    }
    CrossSessionPolicy {
        enabled,
        budget_ratio,
        budget_tokens,
        max_budget_ratio,
        max_budget_tokens,
        max_sessions,
        max_candidates,
        min_score,
        raw_evidence_min_score,
        min_entity_bridge_refs,
        parallelism,
    }
}

fn eligible_entity_bridge_tuple_remains(
    scored: &[(f64, &Value, String, f64, f64)],
    start_index: usize,
    cross_policy: &CrossSessionPolicy,
) -> bool {
    if !cross_policy.enabled {
        return false;
    }
    scored
        .iter()
        .skip(start_index)
        .any(|(score, record, continuity, _, _)| {
            *continuity == "cross_session"
                && context_class_name(record) == "entity"
                && *score >= cross_policy.min_score
        })
}

fn should_reserve_entity_bridge_slot(
    cross_policy: &CrossSessionPolicy,
    is_entity_bridge: bool,
    selected_count: u64,
    max_refs: u64,
    entity_bridge_selected_refs: u64,
    eligible_bridge_remains: bool,
) -> bool {
    let remaining_slots = max_refs.saturating_sub(selected_count);
    let remaining_required_bridge_refs = cross_policy
        .min_entity_bridge_refs
        .saturating_sub(entity_bridge_selected_refs);
    cross_policy.enabled
        && !is_entity_bridge
        && remaining_required_bridge_refs > 0
        && remaining_slots <= remaining_required_bridge_refs
        && eligible_bridge_remains
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

fn node_path_for_record<'a>(
    record: &'a Value,
    node_paths_by_hash: &'a HashMap<u64, Vec<String>>,
) -> Option<Vec<String>> {
    if let Some(path) = record.get("node_path").and_then(Value::as_array) {
        let values: Vec<String> = path
            .iter()
            .filter_map(Value::as_str)
            .filter(|text| !text.is_empty())
            .map(str::to_string)
            .collect();
        if !values.is_empty() {
            return Some(values);
        }
    }
    record_node_hash(record).and_then(|node_hash| node_paths_by_hash.get(&node_hash).cloned())
}

fn query_node_path_filters(query_scope: Option<&Value>) -> Vec<String> {
    let Some(scope) = query_scope.filter(|value| value.is_object()) else {
        return Vec::new();
    };
    ["team", "project"]
        .iter()
        .filter_map(|key| scope.get(*key).and_then(Value::as_str))
        .filter(|text| !text.is_empty())
        .map(str::to_string)
        .collect()
}

fn node_path_matches_filters(
    record: &Value,
    filters: &[String],
    node_paths_by_hash: &HashMap<u64, Vec<String>>,
) -> bool {
    if filters.is_empty() {
        return true;
    }
    let Some(path) = node_path_for_record(record, node_paths_by_hash) else {
        return false;
    };
    filters
        .iter()
        .all(|required| path.iter().any(|part| part == required))
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

fn serving_count_key(count_key: &str) -> String {
    format!("{count_key}:serving")
}

fn matrixark_serving_count(client: &Client, count_key: &str, count: u64) -> u64 {
    let serving_count_text = client
        .get_string(&serving_count_key(count_key))
        .unwrap_or_default();
    let serving_count = serving_count_text.parse::<u64>().unwrap_or(0);
    if serving_count == 0 || serving_count > count {
        count
    } else {
        serving_count
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
    let serving_count = matrixark_serving_count(client, &count_key, count);
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
    let cache_key = scan_record_cache_key(&record_hash_key, shard_size, serving_count);
    let filtered_cache_key = filtered_scan_cache_key(
        &cache_key,
        &allowed_types,
        &selected_nodes,
        &secondary_groups,
        command.scope.as_ref(),
    );
    if let Some(entry) = get_filtered_scan_cache(&filtered_cache_key) {
        let returned_records = entry.records.len();
        let dropped_ref_count = entry.dropped_by_type
            + entry.dropped_by_scope
            + entry.dropped_by_retention
            + entry.selected_node_dropped
            + entry.secondary_dropped;
        return Ok(json!({
            "ok": true,
            "count": returned_records,
            "records": entry.records,
            "native_candidate_prefilter": true,
            "scan_count": entry.scanned_records,
            "cache_hit": true,
            "cache_hit_used": true,
            "selected_ref_count": 0,
            "dropped_ref_count": dropped_ref_count,
            "scan_stats": {
                "execution_mode": "rust_proxy_native_candidate_prefilter",
                "native_prefix_scan": true,
                "native_scan_record_cache_hit": true,
                "native_filtered_scan_cache_hit": true,
                "native_scan_record_cache_keyed_by_count": true,
                "native_scan_record_cache_key_kind": "serving_count",
                "native_secondary_index_prefilter": !secondary_groups.is_empty(),
                "native_node_path_scope_prefilter": entry.node_path_filter_count > 0,
                "native_node_path_scope_filter_count": entry.node_path_filter_count,
                "scanned_records": entry.scanned_records,
                "total_record_count": count,
                "serving_record_watermark": serving_count,
                "returned_records": returned_records,
                "dropped_by_type": entry.dropped_by_type,
                "dropped_by_scope": entry.dropped_by_scope,
                "dropped_by_retention": entry.dropped_by_retention,
                "selected_node_dropped_candidate_count": entry.selected_node_dropped,
                "secondary_index_groups_supplied": secondary_groups.len(),
                "secondary_index_matched_candidate_count": entry.secondary_matched,
                "secondary_index_dropped_candidate_count": entry.secondary_dropped,
                "native_pack_assembly": false,
                "pack_assembly_location": "python_reference_packer",
                "next_native_gap": "conformance ContextPack scoring and budget assembly APIs"
            }
        }));
    }
    let mut cache_hit = false;
    let (records_source, scanned_records): (Arc<Vec<Value>>, u64) =
        if let Some(entry) = get_scan_record_cache(&cache_key) {
            cache_hit = true;
            (entry.records, entry.scanned_records)
        } else {
            let max_shard = if count == 0 {
                0
            } else {
                (count - 1) / shard_size
            };
            let mut scanned_records = 0_u64;
            let mut records = Vec::new();
            for shard in 0..=max_shard {
                let key = format!("{}:{:06}", record_hash_key, shard);
                for (_field, value) in client.hgetall(&key).map_err(|err| err.to_string())? {
                    for record in decode_matrixark_payload(&value) {
                        scanned_records += 1;
                        records.push(record);
                    }
                }
            }
            let records_source = Arc::new(records);
            put_scan_record_cache(
                cache_key,
                ScanRecordCacheEntry {
                    records: Arc::clone(&records_source),
                    scanned_records,
                },
            );
            (records_source, scanned_records)
        };
    let mut dropped_by_type = 0_u64;
    let mut dropped_by_scope = 0_u64;
    let mut dropped_by_retention = 0_u64;
    let mut selected_node_dropped = 0_u64;
    let mut node_paths_by_hash: HashMap<u64, Vec<String>> = HashMap::new();
    for record in records_source.iter() {
        if record.get("record_type").and_then(Value::as_str) != Some("context_node") {
            continue;
        }
        if let (Some(node_hash), Some(path)) = (
            record_node_hash(record),
            record.get("node_path").and_then(Value::as_array),
        ) {
            let values: Vec<String> = path
                .iter()
                .filter_map(Value::as_str)
                .filter(|text| !text.is_empty())
                .map(str::to_string)
                .collect();
            if !values.is_empty() {
                node_paths_by_hash.insert(node_hash, values);
            }
        }
    }
    let node_path_filters = query_node_path_filters(command.scope.as_ref());
    let retention_now_ms = unix_ms();
    let records = records_source
        .iter()
        .filter_map(|record| {
            if matrixark_record_retention_filtered(record, retention_now_ms) {
                dropped_by_retention += 1;
                return None;
            }
            let record_type = record
                .get("record_type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if !allowed_types.is_empty() && !allowed_types.contains(record_type) {
                dropped_by_type += 1;
                return None;
            }
            if !scope_matches_record(record, command.scope.as_ref()) {
                dropped_by_scope += 1;
                return None;
            }
            if !node_path_matches_filters(record, &node_path_filters, &node_paths_by_hash) {
                dropped_by_scope += 1;
                return None;
            }
            if !selected_nodes.is_empty() {
                let keep_index = matches!(record_type, "context_index" | "context_embedding");
                let keep_node = record_node_hash(record)
                    .map(|node| selected_nodes.contains(&node))
                    .unwrap_or(false);
                if !keep_index && !keep_node {
                    selected_node_dropped += 1;
                    return None;
                }
            }
            Some(record.clone())
        })
        .collect::<Vec<_>>();

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

    let dropped_ref_count = dropped_by_type
        + dropped_by_scope
        + dropped_by_retention
        + selected_node_dropped
        + secondary_dropped;
    put_filtered_scan_cache(
        filtered_cache_key,
        FilteredScanCacheEntry {
            records: filtered.clone(),
            scanned_records,
            dropped_by_type,
            dropped_by_scope,
            dropped_by_retention,
            selected_node_dropped,
            secondary_dropped,
            secondary_matched,
            node_path_filter_count: node_path_filters.len(),
        },
    );
    Ok(json!({
        "ok": true,
        "count": filtered.len(),
        "records": filtered,
        "native_candidate_prefilter": true,
        "scan_count": scanned_records,
        "cache_hit": cache_hit,
        "cache_hit_used": cache_hit,
        "selected_ref_count": 0,
        "dropped_ref_count": dropped_ref_count,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_scan_record_cache_hit": cache_hit,
            "native_filtered_scan_cache_hit": false,
            "native_scan_record_cache_keyed_by_count": true,
            "native_scan_record_cache_key_kind": "serving_count",
            "native_secondary_index_prefilter": !secondary_groups.is_empty(),
            "native_node_path_scope_prefilter": !node_path_filters.is_empty(),
            "native_node_path_scope_filter_count": node_path_filters.len(),
            "scanned_records": scanned_records,
            "total_record_count": count,
            "serving_record_watermark": serving_count,
            "returned_records": filtered.len(),
            "dropped_by_type": dropped_by_type,
            "dropped_by_scope": dropped_by_scope,
            "dropped_by_retention": dropped_by_retention,
            "selected_node_dropped_candidate_count": selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups.len(),
            "secondary_index_matched_candidate_count": secondary_matched,
            "secondary_index_dropped_candidate_count": secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "conformance ContextPack scoring and budget assembly APIs"
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

fn matrixark_record_retention_filtered(record: &Value, now_ms: u128) -> bool {
    if record
        .get("synthetic")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }
    if matches!(
        record.get("retention_class").and_then(Value::as_str),
        Some("debug" | "probe")
    ) {
        return true;
    }
    if let Some(expires_at_ms) = record.get("expires_at_ms").and_then(Value::as_u64) {
        if expires_at_ms > 0 && u128::from(expires_at_ms) <= now_ms {
            return true;
        }
    }
    record
        .get("deleted_at_ms")
        .and_then(Value::as_u64)
        .map(|deleted_at_ms| deleted_at_ms > 0)
        .unwrap_or(false)
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

fn context_class_name(record: &Value) -> String {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if record_type == "context_event" {
        let classification = record
            .get("classification")
            .and_then(Value::as_str)
            .unwrap_or("");
        let event_type = record
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if classification == "resource_fact" || event_type.starts_with("resource_") {
            return "resource_fact".to_string();
        }
        return "event".to_string();
    }
    match record_type {
        "context_entity" => "entity".to_string(),
        "context_segment" => "segment".to_string(),
        "context_summary" => "summary".to_string(),
        "context_compression_event" => "compression".to_string(),
        other => other.to_string(),
    }
}

fn is_serving_selected_ref_class(context_class: &str) -> bool {
    matches!(context_class, "event" | "summary")
}

fn pack_ref_from_record(
    record: &Value,
    score: f64,
    reason: &str,
    session_continuity: &str,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
) -> Value {
    let ref_type = context_class_name(record);
    let text = candidate_text(record);
    let continuity_reason = match session_continuity {
        "same_session" => "same-session continuity",
        "cross_session" => "cross-session memory bridge",
        _ => "session-neutral context",
    };
    json!({
        "ref_type": ref_type,
        "ref_hash": record_ref_hash(record).unwrap_or_else(|| record.get("record_id").and_then(Value::as_str).unwrap_or("").to_string()),
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "text": text,
        "token_estimate": token_estimate(&candidate_text(record)),
        "score": (score * 1000000.0).round() / 1000000.0,
        "session_continuity": session_continuity,
        "continuity_boost": (continuity_boost_value * 1000000.0).round() / 1000000.0,
        "cross_session_rerank_boost": (cross_session_rerank_boost_value * 1000000.0).round() / 1000000.0,
        "continuity_reason": continuity_reason,
        "selection_reason": reason,
        "source_ref": record.get("source_ref").cloned().unwrap_or(Value::Null),
    })
}

fn retrieve_context_pack_via_sdk_native(
    client: &Client,
    command: &Command,
) -> Result<Value, String> {
    let count_key = required(command.count_key.clone(), "count_key")?;
    let record_hash_key = required(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1) as usize;
    let request = command.record.clone().unwrap_or_else(|| json!({}));
    let raw = client
        .matrixark_retrieve_context_pack(
            &count_key,
            &record_hash_key,
            shard_size,
            &request.to_string(),
        )
        .map_err(|err| err.to_string())?;
    let mut response: Value = serde_json::from_str(&raw)
        .map_err(|err| format!("native retrieve context pack returned invalid JSON: {err}"))?;
    if response.get("context_pack").is_none() {
        response = json!({
            "context_pack": response,
        });
    }
    if let Some(obj) = response.as_object_mut() {
        obj.insert("ok".to_string(), Value::Bool(true));
        obj.insert("native_pack_assembly".to_string(), Value::Bool(true));
        obj.insert(
            "rust_proxy_native_sdk_path".to_string(),
            Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
        );
        obj.insert("cache_hit".to_string(), Value::Bool(true));
    }
    if let Some(pack) = response
        .get_mut("context_pack")
        .and_then(Value::as_object_mut)
    {
        pack.entry("context_pack_assembly".to_string())
            .or_insert_with(|| Value::String("native_direct_via_rust_proxy".to_string()));
        let selected_count = pack
            .get("selected_ref_count")
            .and_then(Value::as_u64)
            .or_else(|| {
                pack.get("selected_refs")
                    .and_then(Value::as_array)
                    .map(|refs| refs.len() as u64)
            })
            .unwrap_or(0);
        pack.insert("selected_ref_count".to_string(), json!(selected_count));
        let recall_policy = pack
            .entry("recall_policy".to_string())
            .or_insert_with(|| json!({}));
        if let Some(recall_obj) = recall_policy.as_object_mut() {
            recall_obj.insert(
                "rust_proxy_native_sdk_path".to_string(),
                Value::String("temporalstore_matrixark_retrieve_context_pack".to_string()),
            );
            recall_obj.insert("python_hot_path_records".to_string(), json!(0));
        }
    }
    Ok(response)
}

fn retrieve_context_pack_native(client: &Client, command: &Command) -> Result<Value, String> {
    // The native pack is always attempted. What follows is the fallback for when it FAILS,
    // which is a different question from whether to try it -- and the only one anything asks.
    match retrieve_context_pack_via_sdk_native(client, command) {
        Ok(response) => return Ok(response),
        Err(err) => {
            if std::env::var("MATRIXARK_RUST_PROXY_DISABLE_LEGACY_PACK_FALLBACK")
                .map(|value| {
                    matches!(
                        value.trim().to_ascii_lowercase().as_str(),
                        "1" | "true" | "yes"
                    )
                })
                .unwrap_or(false)
            {
                return Err(err);
            }
        }
    }
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
        .unwrap_or(24)
        .max(1);
    let max_global_candidates = json_field(&request, &["ranking", "max_global_candidates"])
        .and_then(Value::as_u64)
        .unwrap_or(512)
        .max(1);
    let min_similarity_score = json_field(&request, &["ranking", "min_similarity_score"])
        .and_then(Value::as_f64)
        .unwrap_or(0.20)
        .clamp(0.0, 1.0);
    let budget_fill_policy = json_field(&request, &["ranking", "budget_fill_policy"])
        .and_then(Value::as_str)
        .filter(|policy| *policy == "quality_first" || *policy == "force_fill")
        .unwrap_or("quality_first")
        .to_string();
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| infer_native_question_type(&query));
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
            "context_index".to_string(),
        ]);
    }
    let scan = scan_matrixark_candidates(client, &scan_command)?;
    let empty_records = Vec::new();
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .unwrap_or(&empty_records);
    let scope_for_continuity = scan_command.scope.clone();
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let retention_now_ms = unix_ms();
    let mut scored: Vec<(f64, &Value, String, f64, f64)> = records
        .iter()
        .filter(|record| {
            if matrixark_record_retention_filtered(record, retention_now_ms) {
                return false;
            }
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
            let text = candidate_text(record);
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
            let context_class = context_class_name(record);
            let session_continuity =
                session_continuity_status(record, scope_for_continuity.as_ref());
            let continuity_boost_value =
                continuity_boost(record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                record,
                &context_class,
                &session_continuity,
                &question_type,
            );
            score += cross_session_rerank_boost_value;
            (
                score,
                record,
                session_continuity,
                continuity_boost_value,
                cross_session_rerank_boost_value,
            )
        })
        .filter(|(score, _, _, _, _)| *score >= min_similarity_score)
        .collect();
    scored.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    if scored.len() > max_global_candidates as usize {
        scored.truncate(max_global_candidates as usize);
    }
    let mut selected = Vec::new();
    let mut selected_signatures: HashSet<String> = HashSet::new();
    let mut dropped_duplicate_ref = 0_u64;
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_entity_bridge_slot_reserved = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut used_tokens = 0_u64;
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    for (
        index,
        (
            score,
            record,
            session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
        ),
    ) in scored.iter().enumerate()
    {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(record);
        let tokens = token_estimate(&text);
        let context_class = context_class_name(record);
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
            continue;
        }
        let is_cross_session = session_continuity == "cross_session";
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_entity_bridge = is_cross_session && context_class == "entity";
        let is_cross_session_raw_evidence =
            is_cross_session && matches!(record_type, "context_event" | "context_segment");
        let cross_key = if is_cross_session {
            cross_session_key(record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            dropped_cross_budget += 1;
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && *score < cross_policy.min_score {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && *score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            continue;
        }
        if should_reserve_entity_bridge_slot(
            &cross_policy,
            is_entity_bridge,
            selected.len() as u64,
            max_refs,
            entity_bridge_selected_refs,
            eligible_entity_bridge_tuple_remains(&scored, index + 1, &cross_policy),
        ) {
            dropped_entity_bridge_slot_reserved += 1;
            continue;
        }
        let ref_signature = format!(
            "{}:{}",
            context_class,
            record_ref_hash(record).unwrap_or_else(|| {
                record
                    .get("record_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
        );
        if !selected_signatures.insert(ref_signature) {
            dropped_duplicate_ref += 1;
            continue;
        }
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            continue;
        }
        used_tokens += tokens;
        if is_cross_session {
            cross_used_tokens += tokens;
            cross_selected_refs += 1;
            selected_cross_sessions.insert(cross_key);
            if is_entity_bridge {
                entity_bridge_selected_refs += 1;
            }
        }
        *selected_counts.entry(context_class).or_default() += 1;
        if let Some(node_hash) = record_node_hash(record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            record,
            *score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            *continuity_boost_value,
            *cross_session_rerank_boost_value,
        ));
    }
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let mut scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert(
            "pack_assembly_location".to_string(),
            json!("rust_proxy_native"),
        );
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": question_type,
        "selected_ref_counts": selected_counts,
        "remote_context_refs": selected,
        "selected_refs": selected,
        "dropped_refs": {
            "over_budget": dropped_over_budget,
            "cross_session_budget": dropped_cross_budget,
            "cross_session_session_cap": dropped_cross_session_cap,
            "cross_session_candidate_cap": dropped_cross_candidate_cap,
            "entity_bridge_slot_reserved": dropped_entity_bridge_slot_reserved,
            "low_score": dropped_low_score,
            "duplicate_ref": dropped_duplicate_ref,
            "policy_ref": dropped_policy_ref,
            "reason_counts": {
                "over_budget": dropped_over_budget,
                "cross_session_budget": dropped_cross_budget,
                "cross_session_session_cap": dropped_cross_session_cap,
                "cross_session_candidate_cap": dropped_cross_candidate_cap,
                "entity_bridge_slot_reserved": dropped_entity_bridge_slot_reserved,
                "low_score": dropped_low_score,
                "duplicate_ref": dropped_duplicate_ref,
                "policy_ref": dropped_policy_ref
            }
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
            "native_response_contract": {
                "raw_records_returned_to_python": false,
                "python_hot_path_records": 0,
                "python_role": "dispatch_request_receive_context_pack",
                "backend_role": "scan_filter_score_pack"
            },
            "scan_stats": scan_stats,
            "rerank": {
                "enabled": true,
                "mode": "native_weighted_recall_plus_cross_session_rerank",
                "cross_session_rerank_enabled": true,
                "cross_session_signals": ["entity_state", "resource_fact_citation", "answer_event", "compression", "summary_demotion"],
                "heavy_rerank_enabled": false
            },
            "ranking": {
                "min_similarity_score": min_similarity_score,
                "max_global_candidates": max_global_candidates,
                "max_selected_refs": max_refs,
                "budget_fill_policy": budget_fill_policy,
                "quality_first_budget_underfill_allowed": budget_fill_policy == "quality_first"
            },
            "session_continuity": {
                "mode": scan_command.scope.as_ref().map(session_scope_mode).unwrap_or("only"),
                "policy": "same-session continuity first; entity state bridges cross-session memory; cross-session evidence remains eligible under account/tenant/user scope",
                "same_session_selected_ref_count": selected.iter().filter(|item| item.get("session_continuity").and_then(Value::as_str) == Some("same_session")).count(),
                "cross_session_selected_ref_count": cross_selected_refs,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs
            },
            "cross_session": {
                "enabled": cross_policy.enabled,
                "mode": if cross_policy.enabled { "prefer" } else { "disabled" },
                "budget_ratio": cross_policy.budget_ratio,
                "max_budget_ratio": cross_policy.max_budget_ratio,
                "budget_tokens": cross_policy.budget_tokens,
                "remote_budget_tokens": remote_budget,
                "max_budget_tokens": cross_policy.max_budget_tokens,
                "max_sessions": cross_policy.max_sessions,
                "max_candidates": cross_policy.max_candidates,
                "min_score": cross_policy.min_score,
                "raw_evidence_min_score": cross_policy.raw_evidence_min_score,
                "parallelism": cross_policy.parallelism,
                "selected_tokens": cross_used_tokens,
                "selected_ref_count": cross_selected_refs,
                "selected_session_count": selected_cross_sessions.len() as u64,
                "entity_bridge_selected_ref_count": entity_bridge_selected_refs,
                "strategy": "same_session_first_entity_bridge_then_bounded_cross_session",
                "budget_guidance": "cross-session budget is a maximum cap, not a quota: 12% normally, 15% for broad/evidence, 20% for current-state/latest/multi-hop/date; spend it only on high-quality refs, prefer entities/summaries/compressions, and require high-confidence raw events"
            },
            "tree_traversal": {
                "enabled": true,
                "native_backend": true,
                "fallback_to_flat": false,
                "selected_node_count": selected_nodes.len() as u64,
                "selected_leaf_count": selected_nodes.len() as u64,
                "summary_embeddings": ["node_l0", "node_l1"]
            },
            "secondary_index_filter": {
                "enabled": true,
                "native_backend": true,
                "applied_before_embedding_scoring": true,
                "matched_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_matched_candidate_count")).cloned().unwrap_or_else(|| json!(0)),
                "dropped_candidate_count": scan.get("scan_stats").and_then(|v| v.get("secondary_index_dropped_candidate_count")).cloned().unwrap_or_else(|| json!(0))
            }
        },
        "quality_warnings": []
    });
    let scan_dropped_count = scan_stats
        .get("dropped_by_type")
        .and_then(Value::as_u64)
        .unwrap_or(0)
        + scan_stats
            .get("dropped_by_scope")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("selected_node_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0)
        + scan_stats
            .get("secondary_index_dropped_candidate_count")
            .and_then(Value::as_u64)
            .unwrap_or(0);
    let dropped_ref_count = dropped_over_budget
        + dropped_cross_budget
        + dropped_cross_session_cap
        + dropped_cross_candidate_cap
        + dropped_entity_bridge_slot_reserved
        + dropped_policy_ref
        + dropped_duplicate_ref
        + scan_dropped_count;
    let scan_cache_hit = scan_stats
        .get("native_filtered_scan_cache_hit")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || scan_stats
            .get("native_scan_record_cache_hit")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    Ok(json!({
        "ok": true,
        "count": selected.len(),
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scan_stats.get("scanned_records").and_then(Value::as_u64).unwrap_or(0),
        "cache_hit": scan_cache_hit,
        "cache_hit_used": scan_cache_hit,
        "selected_ref_count": selected.len(),
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": dropped_duplicate_ref,
        "context_pack": pack,
        "scan_stats": scan_stats
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
            let batch: Vec<(&str, &str, &str, &str)> = entries
                .iter()
                .map(|entry| {
                    (
                        entry.key,
                        entry.field,
                        entry.value,
                        entry.route_json.as_ref(),
                    )
                })
                .collect();
            let append_options_json = command
                .append_options
                .as_ref()
                .map(|value| serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string()))
                .unwrap_or_else(|| "{}".to_string());
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
            if !native_matrixark_c_api_bridge_enabled() {
                for entry in &entries {
                    client
                        .hset(entry.key, entry.field, entry.value)
                        .map_err(|err| err.to_string())?;
                }
                if let (Some(key), Some(value)) = (count_key, count_value) {
                    client
                        .put_string(key, value)
                        .map_err(|err| err.to_string())?;
                }
                return Ok(json!({
                    "ok": true,
                    "written": written,
                    "append_api": command.op,
                    "native_append": true,
                    "append_path": append_path,
                    "raw_storage_backend": raw_backend,
                    "batch_lowering": "rust_proxy_hset_count_lowering",
                    "append_blob_parity": false,
                    "route_metadata_ignored": true,
                    "next_native_gap": "rust_sdk_append_blob_wal_index_metadata_hot_path"
                }));
            }
            client
                .matrixark_batch_append_records_with_routes_and_options(
                    &batch,
                    count_key,
                    count_value,
                    &append_options_json,
                )
                .map_err(|err| err.to_string())?;
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
    fn metrics_render_prometheus_records_matrixark_append_hot_path() {
        let mut metrics = MetricsSnapshot::default();
        metrics.observe(
            "matrixark_batch_append_records",
            true,
            10,
            1,
            Some(&json!({
                "ok": true,
                "batch_lowering": "rust_proxy_hset_count_lowering",
                "append_blob_parity": false
            })),
            CommandStats::default(),
        );
        metrics.observe(
            "matrixark_batch_append_records",
            true,
            8,
            1,
            Some(&json!({
                "ok": true,
                "batch_lowering": "none",
                "append_blob_parity": true
            })),
            CommandStats::default(),
        );
        let text = metrics.render_prometheus();
        assert!(text.contains("matrixark_append_hset_count_lowering_total{backend=\"rust\"} 1"));
        assert!(text.contains("matrixark_append_blob_parity_total{backend=\"rust\"} 1"));
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

    #[test]
    fn native_retrieve_infers_question_type_when_absent() {
        assert_eq!(
            infer_native_question_type("What is the latest assistant decision for bbb222?"),
            "current_state"
        );
        assert_eq!(
            infer_native_question_type("Show evidence from the exact message"),
            "evidence"
        );
        assert_eq!(
            infer_native_question_type("Compare both sessions together"),
            "multi_hop"
        );
    }

    #[test]
    fn retrieve_budget_reserves_required_profile_entity_bridge_slot() {
        let policy = CrossSessionPolicy {
            enabled: true,
            budget_ratio: 1.0,
            budget_tokens: 200,
            max_budget_ratio: 1.0,
            max_budget_tokens: 200,
            max_sessions: 3,
            max_candidates: 8,
            min_score: 0.0,
            raw_evidence_min_score: 0.45,
            min_entity_bridge_refs: 1,
            parallelism: 1,
        };
        let same_session = json!({
            "record_type": "context_event",
            "event_id_hash": 1_u64,
            "text": "Current session says the storage migration is blocked on capacity review."
        });
        let profile_entity = json!({
            "record_type": "context_entity",
            "entity_hash": 2_u64,
            "scope": {"session_id": "prior-session"},
            "state": "User profile says Alice approved the GPU request after finance review."
        });
        let scored = vec![
            (0.99, &same_session, "same_session".to_string(), 0.0, 0.0),
            (0.21, &profile_entity, "cross_session".to_string(), 0.0, 0.0),
        ];

        assert!(should_reserve_entity_bridge_slot(
            &policy,
            false,
            0,
            1,
            0,
            eligible_entity_bridge_tuple_remains(&scored, 1, &policy),
        ));
        assert!(!should_reserve_entity_bridge_slot(
            &policy,
            true,
            0,
            1,
            0,
            eligible_entity_bridge_tuple_remains(&scored, 1, &policy),
        ));
    }
}
