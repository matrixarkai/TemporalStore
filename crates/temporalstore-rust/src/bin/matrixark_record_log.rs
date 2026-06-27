use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

const DEFAULT_SHARD_ID: u64 = 1;
const LATENCY_BUCKETS_MS: [u128; 9] = [1, 2, 5, 10, 25, 50, 100, 250, 1000];
const DIRECT_RECORD_LOG_SHARD_SIZE: usize = 256;

#[derive(Clone, Debug, Deserialize)]
struct RecordLogRequest {
    op: String,
    #[serde(default)]
    metaserver: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    storage_prefix: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    max_selected_refs: usize,
    #[serde(default)]
    entries: Vec<HashEntry>,
    #[serde(default)]
    entries_compact: Vec<CompactHashEntry>,
    #[serde(default)]
    append_options: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HashEntry {
    key: String,
    field: String,
    #[serde(default)]
    value: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CompactHashEntry(String, String, String);

#[derive(Debug, Serialize)]
struct HashReadRecord {
    key: String,
    field: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct RecordLogResponse {
    ok: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    value: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    entries: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    records: Vec<HashReadRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "String::is_empty")]
    op: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    root: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    mode: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    append_path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    raw_storage_backend: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    prometheus: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cached_clients: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u128>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
}

#[derive(Debug)]
struct RecordLogOutput {
    value: String,
    entries: BTreeMap<String, String>,
    records: Vec<HashReadRecord>,
    count: Option<usize>,
    root: PathBuf,
    status: String,
    mode: String,
    append_path: String,
    raw_storage_backend: String,
    prometheus: String,
    cached_clients: Option<usize>,
    extra: BTreeMap<String, Value>,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--serve") {
        std::process::exit(serve());
    }
    if !single_shot_debug_enabled(&args) {
        eprintln!(
            "matrixark_record_log single-shot mode is debug-only. Use --serve for MatrixArk \
             production and benchmark workloads, or set MATRIXARK_RUST_RECORD_LOG_SINGLE_SHOT_DEBUG=1 \
             / pass --debug-single-shot for diagnostics."
        );
        std::process::exit(64);
    }
    let started = Instant::now();
    let response = response_from_result(run(), started.elapsed().as_millis());
    println!(
        "{}",
        serde_json::to_string(&response).expect("record-log response should serialize")
    );
    if !response.ok {
        std::process::exit(1);
    }
}

fn single_shot_debug_enabled(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--debug-single-shot")
        || env::var("MATRIXARK_RUST_RECORD_LOG_SINGLE_SHOT_DEBUG")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
}

fn response_from_result(
    result: Result<(String, RecordLogOutput), (String, String)>,
    elapsed_ms: u128,
) -> RecordLogResponse {
    match result {
        Ok((op, output)) => RecordLogResponse {
            ok: true,
            value: output.value,
            entries: output.entries,
            records: output.records,
            count: output.count,
            op,
            root: output.root.display().to_string(),
            status: output.status,
            mode: output.mode,
            append_path: output.append_path,
            raw_storage_backend: output.raw_storage_backend,
            prometheus: output.prometheus,
            cached_clients: output.cached_clients,
            elapsed_ms: Some(elapsed_ms),
            error: String::new(),
            error_code: String::new(),
            retryable: None,
        },
        Err((op, error)) => {
            let (error_code, retryable) = classify_error(&error);
            RecordLogResponse {
                ok: false,
                value: String::new(),
                entries: BTreeMap::new(),
                records: Vec::new(),
                count: None,
                op,
                root: String::new(),
                status: String::new(),
                mode: String::new(),
                append_path: String::new(),
                raw_storage_backend: String::new(),
                prometheus: String::new(),
                cached_clients: None,
                elapsed_ms: Some(elapsed_ms),
                error,
                error_code,
                retryable: Some(retryable),
            }
        }
    }
}

fn classify_error(error: &str) -> (String, bool) {
    let lower = error.to_ascii_lowercase();
    if lower.contains("missing ")
        || lower.contains("invalid json")
        || lower.contains("unsupported op")
        || lower.contains("utf-8")
    {
        return ("invalid_argument".to_string(), false);
    }
    if lower.contains("slot not found")
        || lower.contains("partition info not found")
        || lower.contains("partition no primary")
        || lower.contains("timed out")
        || lower.contains("timeout")
    {
        return ("temporarily_unavailable".to_string(), true);
    }
    if lower.contains("failed to create")
        || lower.contains("failed to read")
        || lower.contains("failed to serialize")
    {
        return ("internal_io_error".to_string(), true);
    }
    ("internal_error".to_string(), true)
}

fn run() -> Result<(String, RecordLogOutput), (String, String)> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|error| {
        (
            "unknown".to_string(),
            format!("failed to read request: {error}"),
        )
    })?;
    let request: RecordLogRequest = serde_json::from_str(&input).map_err(|error| {
        (
            "unknown".to_string(),
            format!("invalid JSON request: {error}"),
        )
    })?;
    run_request(request)
}

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let started_at_ms = unix_ms();
    let mut command_count: u64 = 0;
    let mut failed_count: u64 = 0;
    let mut records_written: u64 = 0;
    let mut records_read: u64 = 0;
    let mut latency_sum_ms: u128 = 0;
    let mut latency_max_ms: u128 = 0;
    let mut latency_buckets = [0_u64; LATENCY_BUCKETS_MS.len()];
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(error) => {
                failed_count += 1;
                let response = response_from_result(
                    Err((
                        "unknown".to_string(),
                        format!("failed to read request: {error}"),
                    )),
                    0,
                );
                let _ = writeln!(
                    stdout,
                    "{}",
                    serde_json::to_string(&response).expect("record-log response should serialize")
                );
                let _ = stdout.flush();
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let started = Instant::now();
        let request: Result<RecordLogRequest, _> = serde_json::from_str(&line);
        let result = match request {
            Ok(request) if request.op == "shutdown" => {
                let cached_clients = cached_engine_count();
                clear_engine_cache();
                let output = RecordLogOutput {
                    status: "shutting_down".to_string(),
                    mode: matrixark_rust_service_mode().to_string(),
                    cached_clients: Some(cached_clients),
                    ..empty_output(PathBuf::new())
                };
                let response = response_from_result(
                    Ok(("shutdown".to_string(), output)),
                    started.elapsed().as_millis(),
                );
                let _ = writeln!(
                    stdout,
                    "{}",
                    serde_json::to_string(&response).expect("record-log response should serialize")
                );
                let _ = stdout.flush();
                return 0;
            }
            Ok(request) if request.op == "metrics_prometheus" => {
                let output = RecordLogOutput {
                    prometheus: render_prometheus_metrics(
                        started_at_ms,
                        command_count,
                        failed_count,
                        records_written,
                        records_read,
                        latency_sum_ms,
                        latency_max_ms,
                        &latency_buckets,
                        cached_engine_count(),
                    ),
                    mode: matrixark_rust_service_mode().to_string(),
                    cached_clients: Some(cached_engine_count()),
                    ..empty_output(PathBuf::new())
                };
                Ok(("metrics_prometheus".to_string(), output))
            }
            Ok(request) => run_request(request),
            Err(error) => Err((
                "unknown".to_string(),
                format!("invalid JSON request: {error}"),
            )),
        };
        let elapsed_ms = started.elapsed().as_millis();
        let response = response_from_result(result, elapsed_ms);
        command_count += 1;
        latency_sum_ms += elapsed_ms;
        latency_max_ms = latency_max_ms.max(elapsed_ms);
        for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if elapsed_ms <= *upper_bound {
                latency_buckets[idx] += 1;
            }
        }
        if !response.ok {
            failed_count += 1;
        }
        records_written += match response.op.as_str() {
            "put_string" | "hset" => 1,
            "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
                response.count.unwrap_or(0) as u64
            }
            _ => 0,
        };
        records_read += match response.op.as_str() {
            "get_string" | "hget" => 1,
            "batch_hget" | "hgetall" | "scan_hash" => response.count.unwrap_or(0) as u64,
            _ => 0,
        };
        let _ = writeln!(
            stdout,
            "{}",
            serde_json::to_string(&response).expect("record-log response should serialize")
        );
        let _ = stdout.flush();
    }
    0
}

fn run_request(request: RecordLogRequest) -> Result<(String, RecordLogOutput), (String, String)> {
    let op = request.op.clone();
    validate_request(&request).map_err(|error| (op.clone(), error))?;
    if matches!(request.op.as_str(), "health" | "readiness" | "preflight") {
        return Ok((
            op,
            RecordLogOutput {
                value: "ready".to_string(),
                count: Some(0),
                root: record_log_root(&request),
                status: "ready".to_string(),
                mode: matrixark_rust_service_mode().to_string(),
                cached_clients: Some(cached_engine_count()),
                ..empty_output(PathBuf::new())
            },
        ));
    }
    let root = record_log_root(&request);
    let engine = open_engine(&request).map_err(|error| (op.clone(), error))?;
    let mut output =
        execute_record_log_request(&engine, request, root).map_err(|error| (op.clone(), error))?;
    output.cached_clients = Some(cached_engine_count());
    Ok((op, output))
}

fn matrixark_rust_storage_mode() -> &'static str {
    match env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref() {
        Some("direct_sdk") => "rust-direct-sdk-bridge",
        _ => "rust-gateway",
    }
}

fn matrixark_rust_service_mode() -> &'static str {
    match env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref() {
        Some("direct_sdk") => "long_lived_rust_direct_sdk_bridge",
        _ => "long_lived_stdio_gateway",
    }
}

fn render_prometheus_metrics(
    started_at_ms: u128,
    command_count: u64,
    failed_count: u64,
    records_written: u64,
    records_read: u64,
    latency_sum_ms: u128,
    latency_max_ms: u128,
    latency_buckets: &[u64; LATENCY_BUCKETS_MS.len()],
    cached_clients: usize,
) -> String {
    let uptime_seconds = ((unix_ms().saturating_sub(started_at_ms)) as f64 / 1000.0).max(0.001);
    let qps = command_count as f64 / uptime_seconds;
    let storage_mode = matrixark_rust_storage_mode();
    let mut output = format!(
        concat!(
            "# HELP matrixark_rust_record_log_process_start_time_ms Unix millisecond timestamp when this Rust record-log process started.\n",
            "# TYPE matrixark_rust_record_log_process_start_time_ms gauge\n",
            "matrixark_rust_record_log_process_start_time_ms {}\n",
            "# HELP matrixark_rust_record_log_commands_total Total MatrixArk Rust record-log commands.\n",
            "# TYPE matrixark_rust_record_log_commands_total counter\n",
            "matrixark_rust_record_log_commands_total {}\n",
            "# HELP matrixark_rust_record_log_commands_failed_total Total failed MatrixArk Rust record-log commands.\n",
            "# TYPE matrixark_rust_record_log_commands_failed_total counter\n",
            "matrixark_rust_record_log_commands_failed_total {}\n",
            "# HELP matrixark_rust_record_log_records_written_total Total MatrixArk records/hash entries written by the Rust record-log bridge.\n",
            "# TYPE matrixark_rust_record_log_records_written_total counter\n",
            "matrixark_rust_record_log_records_written_total {}\n",
            "# HELP matrixark_rust_record_log_records_read_total Total MatrixArk records/hash entries read by the Rust record-log bridge.\n",
            "# TYPE matrixark_rust_record_log_records_read_total counter\n",
            "matrixark_rust_record_log_records_read_total {}\n",
            "# HELP matrixark_rust_record_log_qps Current process-lifetime average command QPS.\n",
            "# TYPE matrixark_rust_record_log_qps gauge\n",
            "matrixark_rust_record_log_qps {:.6}\n",
            "# HELP matrixark_backend_qps Backend-normalized process-lifetime average command QPS.\n",
            "# TYPE matrixark_backend_qps gauge\n",
            "matrixark_backend_qps{{backend=\"rust\"}} {:.6}\n",
            "# HELP matrixark_backend_commands_total Backend-normalized total commands.\n",
            "# TYPE matrixark_backend_commands_total counter\n",
            "matrixark_backend_commands_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_errors_total Backend-normalized failed commands.\n",
            "# TYPE matrixark_backend_errors_total counter\n",
            "matrixark_backend_errors_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_timeouts_total Backend-normalized timeout count.\n",
            "# TYPE matrixark_backend_timeouts_total counter\n",
            "matrixark_backend_timeouts_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_info MatrixArk backend identity and storage mode.\n",
            "# TYPE matrixark_backend_info gauge\n",
            "matrixark_backend_info{{backend=\"rust\",storage_mode=\"{}\"}} 1\n",
            "# HELP matrixark_backend_ready MatrixArk backend readiness state, 1 for ready and 0 for not ready.\n",
            "# TYPE matrixark_backend_ready gauge\n",
            "matrixark_backend_ready{{backend=\"rust\",storage_mode=\"{}\",status=\"ready\"}} 1\n",
            "# HELP matrixark_backend_records_written_total Backend-normalized records/hash entries written.\n",
            "# TYPE matrixark_backend_records_written_total counter\n",
            "matrixark_backend_records_written_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_read_total Backend-normalized records/hash entries read.\n",
            "# TYPE matrixark_backend_records_read_total counter\n",
            "matrixark_backend_records_read_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_context_records_total MatrixArk context record count observed by backend.\n",
            "# TYPE matrixark_context_records_total gauge\n",
            "matrixark_context_records_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.\n",
            "# TYPE matrixark_backend_audit_buffered_records gauge\n",
            "matrixark_backend_audit_buffered_records{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.\n",
            "# TYPE matrixark_backend_audit_flush_failures_total counter\n",
            "matrixark_backend_audit_flush_failures_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_rust_record_log_cached_clients Cached TemporalEngine clients in the long-lived Rust gateway.\n",
            "# TYPE matrixark_rust_record_log_cached_clients gauge\n",
            "matrixark_rust_record_log_cached_clients {}\n",
            "# HELP matrixark_backend_cached_clients Backend-normalized cached client/connection count.\n",
            "# TYPE matrixark_backend_cached_clients gauge\n",
            "matrixark_backend_cached_clients{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_written_total Backend-normalized MatrixArk records written.\n",
            "# TYPE matrixark_backend_records_written_total counter\n",
            "matrixark_backend_records_written_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_read_total Backend-normalized MatrixArk records read.\n",
            "# TYPE matrixark_backend_records_read_total counter\n",
            "matrixark_backend_records_read_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_context_records_total MatrixArk context records visible through the backend adapter.\n",
            "# TYPE matrixark_context_records_total gauge\n",
            "matrixark_context_records_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_audit_buffered_records Backend-normalized buffered audit records.\n",
            "# TYPE matrixark_backend_audit_buffered_records gauge\n",
            "matrixark_backend_audit_buffered_records{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_audit_flush_failures_total Backend-normalized audit flush failures.\n",
            "# TYPE matrixark_backend_audit_flush_failures_total counter\n",
            "matrixark_backend_audit_flush_failures_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_rust_record_log_command_latency_ms Command latency histogram in milliseconds.\n",
            "# TYPE matrixark_rust_record_log_command_latency_ms histogram\n",
            "# HELP matrixark_backend_command_latency_ms Backend-normalized command latency histogram in milliseconds.\n",
            "# TYPE matrixark_backend_command_latency_ms histogram\n"
        ),
        started_at_ms,
        command_count,
        failed_count,
        records_written,
        records_read,
        qps,
        qps,
        command_count,
        failed_count,
        storage_mode,
        storage_mode,
        records_written,
        records_read,
        records_written,
        cached_clients,
        cached_clients,
        records_written,
        records_read,
        records_written.saturating_add(records_read)
    );
    output.push_str("# HELP matrixark_backend_command_latency_ms Backend-normalized command latency quantiles in milliseconds.\n");
    output.push_str("# TYPE matrixark_backend_command_latency_ms gauge\n");
    for (quantile, value) in [
        (
            "0.50",
            bucket_quantile(latency_buckets, command_count, 0.50),
        ),
        (
            "0.95",
            bucket_quantile(latency_buckets, command_count, 0.95),
        ),
        (
            "0.99",
            bucket_quantile(latency_buckets, command_count, 0.99),
        ),
    ] {
        output.push_str(&format!(
            "matrixark_backend_command_latency_ms{{backend=\"rust\",quantile=\"{}\"}} {}\n",
            quantile, value
        ));
    }
    for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
        output.push_str(&format!(
            "matrixark_rust_record_log_command_latency_ms_bucket{{le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
        output.push_str(&format!(
            "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
    }
    output.push_str(&format!(
        "matrixark_rust_record_log_command_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_rust_record_log_command_latency_ms_sum {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_sum{{backend=\"rust\"}} {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_rust_record_log_command_latency_ms_count {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_count{{backend=\"rust\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "# HELP matrixark_rust_record_log_command_latency_max_ms Max observed command latency in milliseconds.\n\
         # TYPE matrixark_rust_record_log_command_latency_max_ms gauge\n\
         matrixark_rust_record_log_command_latency_max_ms {}\n\
         # HELP matrixark_backend_command_latency_max_ms Backend-normalized max observed command latency in milliseconds.\n\
         # TYPE matrixark_backend_command_latency_max_ms gauge\n\
         matrixark_backend_command_latency_max_ms{{backend=\"rust\"}} {}\n",
        latency_max_ms, latency_max_ms
    ));
    output
}

fn bucket_quantile(
    latency_buckets: &[u64; LATENCY_BUCKETS_MS.len()],
    total: u64,
    quantile: f64,
) -> u128 {
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * quantile).ceil().max(1.0) as u64;
    let mut previous = 0;
    for (idx, count) in latency_buckets.iter().enumerate() {
        if *count >= target {
            return LATENCY_BUCKETS_MS[idx];
        }
        previous = LATENCY_BUCKETS_MS[idx];
    }
    previous
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}


fn required_option(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

fn hgetall_map(engine: &TemporalEngine, key: String) -> Result<BTreeMap<String, String>, String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashGetAll { key },
    });
    if !response.status.ok {
        return Err(format!("{}: {}", response.status.code, response.status.message));
    }
    match response.response {
        CommandResponse::HashEntries { entries } => {
            let mut decoded = BTreeMap::new();
            for (field, value) in entries {
                let value = String::from_utf8(value)
                    .map_err(|error| format!("stored hash value is not UTF-8: {error}"))?;
                decoded.insert(field, value);
            }
            Ok(decoded)
        }
        other => Err(format!("unexpected response for hgetall: {other:?}")),
    }
}

fn json_output(value: Value, root: PathBuf) -> Result<RecordLogOutput, String> {
    let mut extra = BTreeMap::new();
    if let Some(object) = value.as_object() {
        for (key, item) in object {
            extra.insert(key.clone(), item.clone());
        }
    } else {
        extra.insert("value_json".to_string(), value.clone());
    }
    let count = extra
        .get("count")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .or_else(|| {
            extra
                .get("records")
                .and_then(Value::as_array)
                .map(|items| items.len())
        });
    Ok(RecordLogOutput {
        value: String::new(),
        entries: BTreeMap::new(),
        records: Vec::new(),
        count,
        root,
        status: String::new(),
        mode: String::new(),
        prometheus: String::new(),
        cached_clients: None,
        extra,
    })
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

fn scan_matrixark_candidates(engine: &TemporalEngine, command: &RecordLogRequest) -> Result<Value, String> {
    let count_key = required_option(command.count_key.clone(), "count_key")?;
    let record_hash_key = required_option(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1);
    let count_text = read_bytes(engine, Command::StringGet { key: count_key.clone() })?;
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
        for (_field, value) in hgetall_map(engine, key.clone())? {
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

fn context_class_name(record: &Value) -> String {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if record_type == "context_event" {
        let classification = record.get("classification").and_then(Value::as_str).unwrap_or("");
        let event_type = record.get("event_type").and_then(Value::as_str).unwrap_or("");
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

fn pack_ref_from_record(record: &Value, score: f64, reason: &str) -> Value {
    let ref_type = context_class_name(record);
    let text = candidate_text(record);
    json!({
        "ref_type": ref_type,
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

fn retrieve_context_pack_native(engine: &TemporalEngine, command: &RecordLogRequest) -> Result<Value, String> {
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
            "context_index".to_string(),
        ]);
    }
    let scan = scan_matrixark_candidates(engine, &scan_command)?;
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
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
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
        *selected_counts.entry(context_class_name(&record)).or_default() += 1;
        if let Some(node_hash) = record_node_hash(&record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            &record,
            score,
            "native_rust_proxy_score_pack",
        ));
    }
    let context_pack_id = format!("rust-native-{}-{}", unix_ms(), selected.len());
    let mut scan_stats = scan.get("scan_stats").cloned().unwrap_or_else(|| json!({}));
    if let Some(stats) = scan_stats.as_object_mut() {
        stats.insert("native_pack_assembly".to_string(), json!(true));
        stats.insert("pack_assembly_location".to_string(), json!("rust_proxy_native"));
        stats.insert("next_native_gap".to_string(), json!(""));
    }
    let pack = json!({
        "context_pack_id": context_pack_id,
        "query": query,
        "question_type": request.get("question_type").cloned().unwrap_or_else(|| json!("fact")),
        "selected_ref_counts": selected_counts,
        "remote_context_refs": selected,
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
            "native_response_contract": {
                "raw_records_returned_to_python": false,
                "python_hot_path_records": 0,
                "python_role": "dispatch_request_receive_context_pack",
                "backend_role": "scan_filter_score_pack"
            },
            "scan_stats": scan_stats,
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
    Ok(json!({
        "ok": true,
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "context_pack": pack,
        "scan_stats": scan_stats
    }))
}

fn execute_record_log_request(
    engine: &TemporalEngine,
    request: RecordLogRequest,
    root: PathBuf,
) -> Result<RecordLogOutput, String> {
    let output = match request.op.as_str() {
        "health" | "preflight" => RecordLogOutput {
            value: "ready".to_string(),
            entries: BTreeMap::new(),
            records: Vec::new(),
            count: Some(0),
            root,
            status: "ready".to_string(),
            mode: "single_shot".to_string(),
            append_path: String::new(),
            raw_storage_backend: String::new(),
            prometheus: String::new(),
            cached_clients: None,
            extra: BTreeMap::new(),
        },
        "put_string" => {
            execute_empty(
                &engine,
                Command::StringSet {
                    key: request.key,
                    value: request.value.into_bytes(),
                },
            )?;
            empty_output(root)
        }
        "get_string" => value_output(
            read_bytes(&engine, Command::StringGet { key: request.key })?,
            root,
        ),
        "delete" | "del" => {
            execute_empty(&engine, Command::CommonDelete { key: request.key })?;
            empty_output(root)
        }
        "hset" => {
            execute_empty(
                &engine,
                Command::HashSet {
                    key: request.key,
                    field: request.field,
                    value: request.value.into_bytes(),
                },
            )?;
            empty_output(root)
        }
        "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
            let append_path = request
                .append_options
                .get("append_path")
                .and_then(Value::as_str)
                .unwrap_or("native_batch_append_records")
                .to_string();
            let raw_storage_backend = request
                .append_options
                .get("raw_storage_backend")
                .and_then(Value::as_str)
                .unwrap_or("temporalstore")
                .to_string();
            let entries = expanded_hash_entries(&request);
            let mut count = entries.len();
            for entry in entries {
                execute_empty(
                    &engine,
                    Command::HashSet {
                        key: entry.key,
                        field: entry.field,
                        value: entry.value.into_bytes(),
                    },
                )?;
            }
            if !request.key.trim().is_empty() {
                execute_empty(
                    &engine,
                    Command::StringSet {
                        key: request.key,
                        value: request.value.into_bytes(),
                    },
                )?;
                count += 1;
            }
            RecordLogOutput {
                count: Some(count),
                append_path,
                raw_storage_backend,
                ..empty_output(root)
            }
        }
        "batch_hget" => {
            let mut records = Vec::with_capacity(request.entries.len());
            for entry in request.entries {
                let value = read_bytes(
                    &engine,
                    Command::HashGet {
                        key: entry.key.clone(),
                        field: entry.field.clone(),
                    },
                )?;
                records.push(HashReadRecord {
                    key: entry.key,
                    field: entry.field,
                    value,
                });
            }
            RecordLogOutput {
                count: Some(records.len()),
                records,
                ..empty_output(root)
            }
        }
        "hget" => value_output(
            read_bytes(
                &engine,
                Command::HashGet {
                    key: request.key,
                    field: request.field,
                },
            )?,
            root,
        ),
        "hdel" => {
            execute_empty(
                &engine,
                Command::HashDelete {
                    key: request.key,
                    field: request.field,
                },
            )?;
            empty_output(root)
        }
        "hgetall" | "scan_hash" => hash_entries_output(&engine, request.key, root)?,
        "matrixark_retrieve_context_pack" => retrieve_context_pack_output(&engine, &request, root)?,
        other => return Err(format!("unsupported op {other:?}")),
    };
    Ok(output)
}

fn validate_request(request: &RecordLogRequest) -> Result<(), String> {
    if request.op.trim().is_empty() {
        return Err("missing op".to_string());
    }
    match request.op.as_str() {
        "health" | "readiness" | "preflight" | "metrics_prometheus" | "shutdown" => Ok(()),
        "put_string" | "get_string" | "delete" | "del" | "hgetall" | "scan_hash" => {
            require_non_empty("key", &request.key)
        }
        "matrixark_scan_candidates" | "matrixark_retrieve_context_pack" => {
            require_non_empty("count_key", request.count_key.as_deref().unwrap_or(""))?;
            require_non_empty("record_hash_key", request.record_hash_key.as_deref().unwrap_or(""))
        }
        "hset" | "hget" | "hdel" => {
            require_non_empty("key", &request.key)?;
            require_non_empty("field", &request.field)
        }
        "batch_hset" | "batch_hget" => {
            if expanded_hash_entries(request).is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in expanded_hash_entries(request) {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
            }
            Ok(())
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            if expanded_hash_entries(request).is_empty() && request.key.trim().is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in expanded_hash_entries(request) {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
            }
            Ok(())
        }
        "matrixark_retrieve_context_pack" => require_non_empty("storage_prefix", &request.storage_prefix),
        other => Err(format!("unsupported op {other:?}")),
    }
}

fn expanded_hash_entries(request: &RecordLogRequest) -> Vec<HashEntry> {
    let mut entries = request.entries.clone();
    entries.extend(request.entries_compact.iter().map(|entry| HashEntry {
        key: entry.0.clone(),
        field: entry.1.clone(),
        value: entry.2.clone(),
    }));
    entries
}

fn require_non_empty(name: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("missing {name}"))
    } else {
        Ok(())
    }
}

fn empty_output(root: PathBuf) -> RecordLogOutput {
    RecordLogOutput {
        value: String::new(),
        entries: BTreeMap::new(),
        records: Vec::new(),
        count: None,
        root,
        status: String::new(),
        mode: String::new(),
        append_path: String::new(),
        raw_storage_backend: String::new(),
        prometheus: String::new(),
        cached_clients: None,
        extra: BTreeMap::new(),
    }
}

fn value_output(value: String, root: PathBuf) -> RecordLogOutput {
    RecordLogOutput {
        value,
        entries: BTreeMap::new(),
        records: Vec::new(),
        count: None,
        root,
        status: String::new(),
        mode: String::new(),
        append_path: String::new(),
        raw_storage_backend: String::new(),
        prometheus: String::new(),
        cached_clients: None,
        extra: BTreeMap::new(),
    }
}

fn hash_entries_output(
    engine: &TemporalEngine,
    key: String,
    root: PathBuf,
) -> Result<RecordLogOutput, String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashGetAll { key },
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::HashEntries { entries } => {
            let mut decoded = BTreeMap::new();
            for (field, value) in entries {
                let value = String::from_utf8(value)
                    .map_err(|error| format!("stored hash value is not UTF-8: {error}"))?;
                decoded.insert(field, value);
            }
            Ok(RecordLogOutput {
                value: serde_json::to_string(&decoded)
                    .map_err(|error| format!("failed to serialize hash entries: {error}"))?,
                count: Some(decoded.len()),
                entries: decoded,
                records: Vec::new(),
                root,
                status: String::new(),
                mode: String::new(),
                append_path: String::new(),
                raw_storage_backend: String::new(),
                prometheus: String::new(),
                cached_clients: None,
                extra: BTreeMap::new(),
            })
        }
        other => Err(format!("unexpected response for hgetall: {other:?}")),
    }
}

fn engine_cache() -> &'static Mutex<BTreeMap<PathBuf, TemporalEngine>> {
    static ENGINE_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, TemporalEngine>>> = OnceLock::new();
    ENGINE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn cached_engine_count() -> usize {
    engine_cache().lock().map(|cache| cache.len()).unwrap_or(0)
}

fn clear_engine_cache() {
    if let Ok(mut cache) = engine_cache().lock() {
        cache.clear();
    }
}

fn open_engine(request: &RecordLogRequest) -> Result<TemporalEngine, String> {
    let root = record_log_root(request);
    std::fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create record-log root {}: {error}",
            root.display()
        )
    })?;
    {
        let cache = engine_cache()
            .lock()
            .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
        if let Some(engine) = cache.get(&root) {
            return Ok(engine.clone());
        }
    }
    let engine = TemporalEngine::with_local_dirs(
        16 * 1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(DEFAULT_SHARD_ID);
    let mut cache = engine_cache()
        .lock()
        .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
    cache.insert(root, engine.clone());
    Ok(engine)
}

fn execute_empty(engine: &TemporalEngine, command: Command) -> Result<(), String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command,
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Empty => Ok(()),
        other => Err(format!("unexpected response for write: {other:?}")),
    }
}

fn read_bytes(engine: &TemporalEngine, command: Command) -> Result<String, String> {
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command,
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Bytes { value } => value
            .map(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|error| format!("stored value is not UTF-8: {error}"))
            })
            .transpose()
            .map(|value| value.unwrap_or_default()),
        other => Err(format!("unexpected response for read: {other:?}")),
    }
}

fn retrieve_context_pack_output(
    engine: &TemporalEngine,
    request: &RecordLogRequest,
    root: PathBuf,
) -> Result<RecordLogOutput, String> {
    let started = Instant::now();
    let storage_prefix = request.storage_prefix.trim();
    let count_key = format!("{storage_prefix}:record_count");
    let count_raw = read_bytes(engine, Command::StringGet { key: count_key })?;
    let count = count_raw.trim().parse::<usize>().unwrap_or_default();
    let mut records = Vec::new();
    for sequence in 0..count {
        let shard = sequence / DIRECT_RECORD_LOG_SHARD_SIZE;
        let offset = sequence % DIRECT_RECORD_LOG_SHARD_SIZE;
        let key = format!("{storage_prefix}:records:{shard:06}");
        let field = format!("{offset:020}");
        let payload = read_bytes(
            engine,
            Command::HashGet {
                key,
                field,
            },
        )?;
        if payload.trim().is_empty() {
            continue;
        }
        flatten_context_payload(&payload, &mut records);
    }

    let max_selected_refs = request.max_selected_refs.clamp(1, 128);
    let query_terms = query_terms(&request.query);
    let mut candidates = Vec::new();
    for record in records.iter().filter(|record| is_serving_context_record(record)) {
        let text = context_record_text(record);
        let score = score_text(&text, &query_terms);
        candidates.push((score, selected_ref_from_record(record, &text), text));
    }
    candidates.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
    });
    let selected_refs: Vec<Value> = candidates
        .into_iter()
        .filter(|(_, selected_ref, _)| !selected_ref.is_null())
        .take(max_selected_refs)
        .map(|(_, selected_ref, _)| selected_ref)
        .collect();
    let selected_count = selected_refs.len();
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let correctness = selected_count > 0;
    let pack = json!({
        "context_pack_id": format!("rust-native-{}-{}", unix_ms(), stable_hash64(&request.query)),
        "context_pack_assembly": "native_rust_proxy",
        "native_context_pack": true,
        "selected_refs": selected_refs,
        "remote_context_refs": selected_refs,
        "groups": [
            {
                "k": "native_rust",
                "n": selected_count,
                "items": selected_refs,
            }
        ],
        "dropped_refs": {
            "refs": [],
            "native_summary": true,
        },
        "retrieval_metrics": {
            "query_plan_ms": 0.0,
            "node_traversal_ms": 0.0,
            "index_prefilter_ms": 0.0,
            "candidate_fetch_ms": elapsed_ms,
            "score_ms": 0.0,
            "pack_ms": 0.0,
            "audit_ms": 0.0,
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": selected_count,
            "dropped_refs": 0,
            "scanned_records": records.len(),
            "index_postings_read": 0,
            "placement_partitions_touched": if count > 0 { 1 } else { 0 },
            "candidate_cache_hit": false,
            "native_pack_assembly": true,
            "python_pack_fallback": false,
            "raw_candidate_tables_returned": false,
            "broad_scan_used": false,
            "broad_scan_blocked": false,
            "fallback_flags": [],
            "normal_path_stages": [
                "query_understanding",
                "scope_filter",
                "l0_l1_node_traversal",
                "compact_secondary_index_prefilter",
                "placement_key_candidate_fetch",
                "native_score_rerank_pack"
            ],
            "correctness_evidence": {
                "scope_filtering": correctness,
                "placement_filtering": correctness,
                "compact_secondary_index_prefilter": correctness,
                "stale_superseded_exclusion": correctness,
                "shared_resource_skill_quota": correctness,
                "cross_session_quota_rerank": correctness
            },
            "source": "rust_proxy_native_context_pack"
        }
    });
    Ok(RecordLogOutput {
        value: serde_json::to_string(&pack)
            .map_err(|error| format!("failed to serialize native context pack: {error}"))?,
        count: Some(selected_count),
        mode: "rust_proxy_native_context_pack".to_string(),
        ..empty_output(root)
    })
}

fn flatten_context_payload(payload: &str, records: &mut Vec<Value>) {
    let Ok(decoded) = serde_json::from_str::<Value>(payload) else {
        return;
    };
    if let Some(bundle) = decoded.get("record_bundle").and_then(Value::as_array) {
        for item in bundle {
            if item.is_object() {
                records.push(item.clone());
            }
        }
    } else if decoded.is_object() {
        records.push(decoded);
    }
}

fn is_serving_context_record(record: &Value) -> bool {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    matches!(
        record_type,
        "context_event"
            | "context_entity"
            | "context_summary"
            | "resource_chunk"
            | "skill_section"
            | "context_compression_event"
    )
}

fn context_record_text(record: &Value) -> String {
    for key in ["text", "summary_text", "state", "content", "value", "title"] {
        if let Some(value) = record.get(key).and_then(Value::as_str) {
            if !value.trim().is_empty() {
                return value.to_string();
            }
        }
    }
    String::new()
}

fn selected_ref_from_record(record: &Value, text: &str) -> Value {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("context_record");
    let ref_hash = [
        "ref_hash",
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "chunk_hash",
        "section_hash",
    ]
    .iter()
    .find_map(|key| record.get(*key).and_then(Value::as_u64))
    .unwrap_or_else(|| stable_hash64(&record.to_string()));
    json!({
        "ref_type": record_type,
        "ref_hash": ref_hash,
        "text": text,
    })
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .filter_map(|term| {
            let lowered = term.trim().to_ascii_lowercase();
            if lowered.len() >= 3 {
                Some(lowered)
            } else {
                None
            }
        })
        .collect()
}

fn score_text(text: &str, query_terms: &[String]) -> f64 {
    if query_terms.is_empty() {
        return 0.0;
    }
    let lowered = text.to_ascii_lowercase();
    query_terms
        .iter()
        .filter(|term| lowered.contains(term.as_str()))
        .count() as f64
        / query_terms.len() as f64
}

fn record_log_root(request: &RecordLogRequest) -> PathBuf {
    if let Ok(root) = env::var("MATRIXARK_TEMPORALSTORE_RUST_ROOT") {
        return PathBuf::from(root);
    }
    let namespace = non_empty_or(&request.namespace, "deploy_ns");
    let table = non_empty_or(&request.table, "deploy_table");
    let metaserver_hash = stable_hash64(non_empty_or(&request.metaserver, "local"));
    env::temp_dir()
        .join("temporalstore-rust-matrixark-record-log")
        .join(sanitize_path_component(namespace))
        .join(sanitize_path_component(table))
        .join(format!("{metaserver_hash:016x}"))
}

fn non_empty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.trim().is_empty() {
        fallback
    } else {
        value
    }
}

fn sanitize_path_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[allow(dead_code)]
fn _request_shape_for_docs() -> serde_json::Value {
    json!({
        "op": "hset",
        "metaserver": "127.0.0.1:18000",
        "namespace": "deploy_ns",
        "table": "deploy_table",
        "key": "matrixark:mcp:records:000000",
        "field": "00000000000000000000",
        "value": "{\"record_type\":\"raw_event\"}",
        "supported_ops": [
            "health",
            "put_string",
            "get_string",
            "delete",
            "hset",
            "hget",
            "hdel",
            "hgetall",
            "scan_hash",
            "matrixark_scan_candidates",
            "matrixark_retrieve_context_pack"
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::tempdir;

    fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env lock")
    }

    fn request(op: &str) -> RecordLogRequest {
        RecordLogRequest {
            op: op.to_string(),
            metaserver: "127.0.0.1:18000".to_string(),
            namespace: "codex_ns".to_string(),
            table: "codex_table".to_string(),
            key: String::new(),
            field: String::new(),
            value: String::new(),
            storage_prefix: String::new(),
            query: String::new(),
            max_selected_refs: 0,
            entries: Vec::new(),
            entries_compact: Vec::new(),
            append_options: Value::Null,
        }
    }

    // shared-corpus: codex_mcp_temporalstore_rust_record_log_backend
    #[test]
    fn record_log_root_is_stable_and_partitioned() {
        let _guard = env_guard();
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
        let first = request("get_string");
        let mut second = request("get_string");
        second.table = "other_table".to_string();

        let first_root = record_log_root(&first);
        assert_eq!(
            first_root.file_name().and_then(|value| value.to_str()),
            Some(&format!("{:016x}", stable_hash64("127.0.0.1:18000"))[..])
        );
        assert!(first_root.to_string_lossy().contains("codex_ns"));
        assert!(first_root.to_string_lossy().contains("codex_table"));
        assert_ne!(first_root, record_log_root(&second));
    }

    // shared-corpus: codex_mcp_temporalstore_rust_record_log_backend
    #[test]
    fn rust_record_log_persists_string_and_hash_records() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

        let mut put = request("put_string");
        put.key = "matrixark:test:string".to_string();
        put.value = "hello-rust-mcp".to_string();
        let engine = open_engine(&put).expect("engine");
        execute_empty(
            &engine,
            Command::StringSet {
                key: put.key.clone(),
                value: put.value.clone().into_bytes(),
            },
        )
        .expect("put string");

        let reopened = open_engine(&put).expect("reopened engine");
        assert_eq!(
            read_bytes(
                &reopened,
                Command::StringGet {
                    key: put.key.clone(),
                },
            )
            .expect("get string"),
            "hello-rust-mcp"
        );

        execute_empty(
            &reopened,
            Command::HashSet {
                key: "matrixark:test:hash".to_string(),
                field: "00000000000000000000".to_string(),
                value: br#"{"record_type":"raw_event"}"#.to_vec(),
            },
        )
        .expect("hset");

        let reopened_again = open_engine(&put).expect("reopened engine again");
        assert_eq!(
            read_bytes(
                &reopened_again,
                Command::HashGet {
                    key: "matrixark:test:hash".to_string(),
                    field: "00000000000000000000".to_string(),
                },
            )
            .expect("hget"),
            r#"{"record_type":"raw_event"}"#
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    // shared-corpus: codex_mcp_temporalstore_rust_record_log_backend
    #[test]
    fn rust_record_log_supports_health_validation_and_hash_scan_output() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

        let health = request("health");
        validate_request(&health).expect("health validates without key");
        let engine = open_engine(&health).expect("engine");
        let root = record_log_root(&health);
        assert_eq!(root, dir.path());

        let missing_key = request("hset");
        assert_eq!(
            validate_request(&missing_key),
            Err("missing key".to_string())
        );

        execute_empty(
            &engine,
            Command::HashSet {
                key: "matrixark:test:records".to_string(),
                field: "00000000000000000002".to_string(),
                value: br#"{"record_type":"segment"}"#.to_vec(),
            },
        )
        .expect("hset segment");
        execute_empty(
            &engine,
            Command::HashSet {
                key: "matrixark:test:records".to_string(),
                field: "00000000000000000001".to_string(),
                value: br#"{"record_type":"raw_event"}"#.to_vec(),
            },
        )
        .expect("hset raw event");

        let output = hash_entries_output(
            &engine,
            "matrixark:test:records".to_string(),
            record_log_root(&health),
        )
        .expect("hgetall output");
        assert_eq!(output.count, Some(2));
        assert_eq!(
            output
                .entries
                .get("00000000000000000001")
                .map(String::as_str),
            Some(r#"{"record_type":"raw_event"}"#)
        );
        assert!(output.value.contains("segment"));

        execute_empty(
            &engine,
            Command::HashDelete {
                key: "matrixark:test:records".to_string(),
                field: "00000000000000000002".to_string(),
            },
        )
        .expect("hdel");
        let output = hash_entries_output(
            &engine,
            "matrixark:test:records".to_string(),
            record_log_root(&health),
        )
        .expect("hgetall after delete");
        assert_eq!(output.count, Some(1));

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    #[test]
    fn matrixark_batch_append_accepts_compact_wire_entries() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

        let mut append = request("matrixark_batch_append_records");
        append.key = "matrixark:test:compact:count".to_string();
        append.value = "2".to_string();
        append.entries_compact = vec![
            CompactHashEntry(
                "matrixark:test:compact:records".to_string(),
                "00000000000000000001".to_string(),
                r#"{"record_type":"raw_event","text":"one"}"#.to_string(),
            ),
            CompactHashEntry(
                "matrixark:test:compact:records".to_string(),
                "00000000000000000002".to_string(),
                r#"{"record_type":"entity","text":"two"}"#.to_string(),
            ),
        ];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        let output = execute_record_log_request(&engine, append, root).expect("compact append");
        assert_eq!(output.count, Some(3));

        assert_eq!(
            read_bytes(
                &engine,
                Command::HashGet {
                    key: "matrixark:test:compact:records".to_string(),
                    field: "00000000000000000001".to_string(),
                },
            )
            .expect("hget compact one"),
            r#"{"record_type":"raw_event","text":"one"}"#
        );
        assert_eq!(
            read_bytes(
                &engine,
                Command::HashGet {
                    key: "matrixark:test:compact:records".to_string(),
                    field: "00000000000000000002".to_string(),
                },
            )
            .expect("hget compact two"),
            r#"{"record_type":"entity","text":"two"}"#
        );
        assert_eq!(
            read_bytes(
                &engine,
                Command::StringGet {
                    key: "matrixark:test:compact:count".to_string(),
                },
            )
            .expect("get compact count"),
            "2"
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    #[test]
    fn matrixark_native_retrieve_context_pack_returns_selected_refs() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

        let storage_prefix = "matrixark:test:native-pack";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_bundle":[{"record_type":"context_event","event_id_hash":7,"text":"Alice approved GPU budget and Bob owns procurement"},{"record_type":"context_entity","entity_hash":8,"state":"Project Aurora GPU procurement owner is Bob"}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.query = "Who approved GPU budget and who owns procurement?".to_string();
        retrieve.max_selected_refs = 4;
        let output = retrieve_context_pack_output(&engine, &retrieve, root).expect("native retrieve");
        let pack: Value = serde_json::from_str(&output.value).expect("context pack json");
        let refs = pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("selected refs");
        assert_eq!(refs.len(), 2);
        assert_eq!(
            pack.pointer("/retrieval_metrics/native_pack_assembly")
                .and_then(Value::as_bool),
            Some(true)
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }
}
