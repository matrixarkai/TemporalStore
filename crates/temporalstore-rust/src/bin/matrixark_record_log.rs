use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temporalstore_rust::{
    BatchExecuteRequest, BlockStoreOptions, Command, CommandResponse, ExecuteRequest,
    TemporalEngine,
};
use temporalstore_rust::{Config, SetConfigRequest};

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
    #[serde(default)]
    count_key: Option<String>,
    #[serde(default)]
    record_hash_key: Option<String>,
    #[serde(default)]
    shard_size: Option<u64>,
    #[serde(default)]
    record_types: Option<Vec<String>>,
    #[serde(default)]
    selected_node_hashes: Option<Vec<u64>>,
    #[serde(default)]
    secondary_index_groups: Option<Vec<Vec<String>>>,
    #[serde(default)]
    scope: Option<Value>,
    #[serde(default)]
    return_index_records: bool,
    #[serde(default)]
    record: Option<Value>,
    #[serde(default)]
    visibility_keys: Vec<String>,
    #[serde(default)]
    top_level_response: bool,
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

#[derive(Clone, Debug)]
struct CachedRetrieveCandidate {
    selected_ref: Value,
    lower_text: String,
}

#[derive(Clone, Debug)]
struct RetrieveCandidateSnapshot {
    candidates: Vec<CachedRetrieveCandidate>,
    scanned_records: usize,
    placement_partitions_touched: usize,
    index_postings_read: usize,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    rust_engine_time_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    serialization_time_ms: Option<u128>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
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

fn default_true() -> bool {
    true
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
    let mut response = response_from_result(run(), started.elapsed().as_millis());
    println!("{}", serialize_response_with_metrics(&mut response));
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
            rust_engine_time_ms: Some(elapsed_ms),
            serialization_time_ms: None,
            error: String::new(),
            error_code: String::new(),
            retryable: None,
            extra: output.extra,
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
                rust_engine_time_ms: Some(elapsed_ms),
                serialization_time_ms: None,
                error,
                error_code,
                retryable: Some(retryable),
                extra: BTreeMap::new(),
            }
        }
    }
}

fn serialize_response_with_metrics(response: &mut RecordLogResponse) -> String {
    let started = Instant::now();
    let serialized = serde_json::to_string(response)
        .unwrap_or_else(|error| json!({"ok": false, "error": error.to_string()}).to_string());
    response.serialization_time_ms = Some(started.elapsed().as_millis());
    serialized
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
                let mut response = response_from_result(
                    Ok(("shutdown".to_string(), output)),
                    started.elapsed().as_millis(),
                );
                let _ = writeln!(stdout, "{}", serialize_response_with_metrics(&mut response));
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
        let mut response = response_from_result(result, elapsed_ms);
        let response_json = serialize_response_with_metrics(&mut response);
        command_count += 1;
        let observed_elapsed_ms = response.elapsed_ms.unwrap_or(elapsed_ms);
        latency_sum_ms += observed_elapsed_ms;
        latency_max_ms = latency_max_ms.max(observed_elapsed_ms);
        for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if observed_elapsed_ms <= *upper_bound {
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
        let _ = writeln!(stdout, "{}", response_json);
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

fn matrixark_rust_sdk_mode_is_direct() -> bool {
    matches!(
        env::var("MATRIXARK_RUST_SDK_MODE").ok().as_deref(),
        Some("direct_sdk" | "direct-sdk" | "native-binding" | "rust-direct")
    ) || env::args()
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
            "# HELP matrixark_rust_record_log_clients_created_total TemporalEngine clients created by the long-lived Rust proxy.\n",
            "# TYPE matrixark_rust_record_log_clients_created_total counter\n",
            "matrixark_rust_record_log_clients_created_total {}\n",
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

fn hgetall_snapshot_cache() -> &'static Mutex<BTreeMap<String, BTreeMap<String, String>>> {
    static HGETALL_SNAPSHOT_CACHE: OnceLock<Mutex<BTreeMap<String, BTreeMap<String, String>>>> =
        OnceLock::new();
    HGETALL_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn record_count_cache() -> &'static Mutex<BTreeMap<String, String>> {
    static RECORD_COUNT_CACHE: OnceLock<Mutex<BTreeMap<String, String>>> = OnceLock::new();
    RECORD_COUNT_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn retrieve_candidate_cache() -> &'static Mutex<BTreeMap<String, Arc<RetrieveCandidateSnapshot>>> {
    static RETRIEVE_CANDIDATE_CACHE: OnceLock<
        Mutex<BTreeMap<String, Arc<RetrieveCandidateSnapshot>>>,
    > = OnceLock::new();
    RETRIEVE_CANDIDATE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn matrixark_scan_cache() -> &'static Mutex<BTreeMap<String, Value>> {
    static MATRIXARK_SCAN_CACHE: OnceLock<Mutex<BTreeMap<String, Value>>> = OnceLock::new();
    MATRIXARK_SCAN_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

fn clear_matrixark_scan_cache() {
    if let Ok(mut cache) = matrixark_scan_cache().lock() {
        cache.clear();
    }
}

fn is_record_count_key(key: &str) -> bool {
    key.ends_with(":record_count")
}

fn update_record_count_cache(key: &str, value: &[u8]) {
    if !is_record_count_key(key) {
        return;
    }
    if let Ok(text) = std::str::from_utf8(value) {
        if let Ok(mut cache) = record_count_cache().lock() {
            cache.insert(key.to_string(), text.to_string());
        }
    }
}

fn invalidate_record_count_cache(key: &str) {
    if !is_record_count_key(key) {
        return;
    }
    if let Ok(mut cache) = record_count_cache().lock() {
        cache.remove(key);
    }
}

fn retrieve_candidate_cache_key(
    storage_prefix: &str,
    count: usize,
    scope: Option<&Value>,
    secondary_groups: &[Vec<String>],
) -> String {
    let scope_key = scope
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_default();
    let secondary_key = serde_json::to_string(secondary_groups).unwrap_or_default();
    format!("{storage_prefix}:candidate_snapshot:{count}:{scope_key}:{secondary_key}")
}

fn storage_prefix_from_key(key: &str) -> Option<String> {
    if let Some(prefix) = key.strip_suffix(":record_count") {
        return Some(prefix.to_string());
    }
    key.split_once(":records:")
        .map(|(prefix, _)| prefix.to_string())
}

fn storage_prefix_from_request(request: &RecordLogRequest) -> String {
    if !request.storage_prefix.trim().is_empty() {
        return request.storage_prefix.trim().to_string();
    }
    request
        .count_key
        .as_deref()
        .and_then(storage_prefix_from_key)
        .unwrap_or_default()
}

fn matrixark_compact_snapshot_retrieve_enabled() -> bool {
    env::var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN")
        .map(|value| !matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(true)
}

fn invalidate_retrieve_candidate_cache(storage_prefix: &str) {
    if storage_prefix.trim().is_empty() {
        return;
    }
    let prefix = format!("{storage_prefix}:candidate_snapshot:");
    if let Ok(mut cache) = retrieve_candidate_cache().lock() {
        cache.retain(|key, _| !key.starts_with(&prefix));
    }
}

fn invalidate_retrieve_candidate_cache_for_keys<'a>(keys: impl IntoIterator<Item = &'a String>) {
    let prefixes = keys
        .into_iter()
        .filter_map(|key| storage_prefix_from_key(key))
        .collect::<HashSet<_>>();
    for prefix in prefixes {
        invalidate_retrieve_candidate_cache(&prefix);
    }
}

fn matrixark_scan_cache_key(command: &RecordLogRequest, count: u64) -> String {
    serde_json::to_string(&json!({
        "count_key": command.count_key,
        "record_hash_key": command.record_hash_key,
        "shard_size": command.shard_size.unwrap_or(1024).max(1),
        "count": count,
        "record_types": command.record_types,
        "selected_node_hashes": command.selected_node_hashes,
        "secondary_index_groups": command.secondary_index_groups,
        "scope": command.scope,
        "return_index_records": command.return_index_records,
    }))
    .unwrap_or_else(|_| format!("fallback:{count}"))
}

fn mark_scan_cache_hit(mut value: Value) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("cache_hit".to_string(), json!(true));
        if let Some(stats) = object.get_mut("scan_stats").and_then(Value::as_object_mut) {
            stats.insert("candidate_cache_hit".to_string(), json!(true));
            stats.insert("cache_hit".to_string(), json!(true));
        }
    }
    value
}

fn invalidate_hgetall_snapshot(key: &str) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.remove(key);
    }
}

fn hgetall_snapshot_contains(key: &str) -> bool {
    hgetall_snapshot_cache()
        .lock()
        .map(|cache| cache.contains_key(key))
        .unwrap_or(false)
}

fn update_hgetall_snapshot_fields(key: &str, entries: &[(String, Vec<u8>)]) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(snapshot) = cache.get_mut(key) {
            for (field, value) in entries {
                if let Ok(text) = String::from_utf8(value.clone()) {
                    snapshot.insert(field.clone(), text);
                } else {
                    cache.remove(key);
                    break;
                }
            }
        }
    }
}

fn hgetall_map(engine: &TemporalEngine, key: String) -> Result<BTreeMap<String, String>, String> {
    if let Ok(cache) = hgetall_snapshot_cache().lock() {
        if let Some(cached) = cache.get(&key) {
            return Ok(cached.clone());
        }
    }
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashGetAll { key: key.clone() },
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
            if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
                cache.insert(key, decoded.clone());
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
        append_path: String::new(),
        raw_storage_backend: String::new(),
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

fn session_scope_mode(query: &Value) -> &str {
    match query
        .get("_session_scope")
        .or_else(|| query.get("session_scope"))
        .and_then(Value::as_str)
        .unwrap_or("prefer")
    {
        "only" | "strict" => "only",
        _ => "prefer",
    }
}

fn scope_key_explicit(scope: &Value, field: &str) -> bool {
    scope
        .get("_explicit_scope_keys")
        .and_then(Value::as_array)
        .map(|items| items.iter().any(|item| item.as_str() == Some(field)))
        .unwrap_or(false)
}

fn parse_scope_key(scope_key: &str) -> HashMap<String, u64> {
    scope_key
        .split('|')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            if key.is_empty() || value.is_empty() {
                return None;
            }
            value
                .parse::<u64>()
                .ok()
                .map(|parsed| (key.to_string(), parsed))
        })
        .collect()
}

fn scoped_string_value(scope: Option<&Value>, field: &str) -> Option<String> {
    scope
        .filter(|value| value.is_object())
        .and_then(|value| value.get(field))
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn record_scope_sources(record: &Value) -> [Option<&Value>; 5] {
    [
        Some(record),
        record.get("access_scope").filter(|value| value.is_object()),
        json_field(record, &["metadata", "access_scope"]).filter(|value| value.is_object()),
        record.get("scope").filter(|value| value.is_object()),
        json_field(record, &["envelope", "scope"]).filter(|value| value.is_object()),
    ]
}

fn candidate_scope_key(record: &Value) -> String {
    for source in record_scope_sources(record) {
        if let Some(value) = scoped_string_value(source, "scope_key") {
            return value;
        }
    }
    String::new()
}

fn scope_key_matches_query(record_scope_key: &str, query_scope: &Value) -> bool {
    if record_scope_key.is_empty() {
        return true;
    }
    let parts = parse_scope_key(record_scope_key);
    if let Some(tenant_hash) = query_scope.get("tenant_hash").and_then(Value::as_u64) {
        if tenant_hash != 0 && parts.get("t").copied() != Some(tenant_hash) {
            return false;
        }
    }
    if scope_key_explicit(query_scope, "user_id") {
        if let Some(user_hash) = query_scope.get("user_hash").and_then(Value::as_u64) {
            if user_hash != 0 && parts.get("u").copied() != Some(user_hash) {
                return false;
            }
        }
    }
    if scope_key_explicit(query_scope, "session_id") && session_scope_mode(query_scope) == "only" {
        if let Some(session_hash) = query_scope.get("session_hash").and_then(Value::as_u64) {
            if session_hash != 0 && parts.get("s").copied() != Some(session_hash) {
                return false;
            }
        }
    }
    true
}

fn scope_matches_record(record: &Value, query_scope: Option<&Value>) -> bool {
    let Some(query) = query_scope.filter(|value| value.is_object()) else {
        return true;
    };
    if !scope_key_matches_query(&candidate_scope_key(record), query) {
        return false;
    }
    for key in [
        "scope_key",
        "account_id",
        "tenant_id",
        "user_id",
        "session_id",
        "team",
        "project",
        "agent_name",
    ] {
        if key == "scope_key" {
            continue;
        }
        if matches!(key, "account_id" | "tenant_id" | "user_id" | "session_id")
            && !scope_key_explicit(query, key)
        {
            continue;
        }
        if key == "session_id" && session_scope_mode(query) == "prefer" {
            continue;
        }
        if matches!(key, "team" | "project" | "agent_name") && !scope_key_explicit(query, key) {
            continue;
        }
        let Some(query_value) = query.get(key) else {
            continue;
        };
        if query_value.is_null() || query_value.as_str() == Some("") {
            continue;
        }
        let actual = record_scope_sources(record)
            .into_iter()
            .find_map(|source| scoped_string_value(source, key));
        if actual
            .as_deref()
            .is_some_and(|value| Some(value) != query_value.as_str())
        {
            return false;
        }
    }
    true
}

fn record_scope_string(record: &Value, field: &str) -> Option<String> {
    for source in record_scope_sources(record) {
        if let Some(value) = scoped_string_value(source, field) {
            return Some(value);
        }
    }
    None
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
    if let Some(query_scope_key) = query
        .get("scope_key")
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
    {
        if record_scope_string(record, "scope_key").as_deref() == Some(query_scope_key) {
            return "same_session".to_string();
        }
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

fn type_priority_boost(record: &Value, context_class: &str, question_type: &str) -> f64 {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    match record_type {
        "skill_section" => {
            if matches!(question_type, "procedure" | "evidence") {
                0.42
            } else {
                0.34
            }
        }
        "resource_chunk" => {
            if matches!(question_type, "evidence" | "fact") {
                0.20
            } else {
                0.12
            }
        }
        "context_entity" => {
            if question_type == "current_state" {
                0.24
            } else {
                0.12
            }
        }
        "context_event" | "context_segment" => 0.10,
        "context_summary" => {
            if matches!(question_type, "broad" | "exploration") {
                0.12
            } else {
                0.0
            }
        }
        _ => {
            if context_class == "resource_fact" {
                0.18
            } else {
                0.0
            }
        }
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
        .unwrap_or(1536);
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

fn record_ref_hash(record: &Value) -> Option<String> {
    for field in [
        "ref_hash",
        "chunk_hash",
        "section_hash",
        "skill_hash",
        "event_id_hash",
        "entity_hash",
        "summary_hash",
    ] {
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

fn scan_matrixark_candidates(
    engine: &TemporalEngine,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let count_key = required_option(command.count_key.clone(), "count_key")?;
    let record_hash_key = required_option(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1);
    let count_text = read_record_count(engine, &count_key)?;
    let count = count_text.parse::<u64>().unwrap_or(0);
    let scan_cache_key = matrixark_scan_cache_key(command, count);
    if let Ok(cache) = matrixark_scan_cache().lock() {
        if let Some(cached) = cache.get(&scan_cache_key) {
            return Ok(mark_scan_cache_hit(cached.clone()));
        }
    }
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
    let placement_partitions_touched = if count == 0 { 0 } else { max_shard + 1 };
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
    let mut non_serving_dropped = 0_u64;
    let returned_records = if command.return_index_records {
        filtered
    } else {
        filtered
            .into_iter()
            .filter(|record| {
                let record_type = record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let drop = matches!(
                    record_type,
                    "context_index"
                        | "context_embedding"
                        | "resource_manifest"
                        | "skill_registry_update"
                );
                if drop {
                    non_serving_dropped += 1;
                }
                !drop
            })
            .collect::<Vec<_>>()
    };

    let dropped_ref_count = dropped_by_type
        + dropped_by_scope
        + selected_node_dropped
        + secondary_dropped
        + non_serving_dropped;
    let output = json!({
        "ok": true,
        "count": returned_records.len(),
        "records": returned_records,
        "native_candidate_prefilter": true,
        "scan_count": scanned_records,
        "cache_hit": false,
        "selected_ref_count": 0,
        "dropped_ref_count": dropped_ref_count,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_secondary_index_prefilter": !secondary_groups.is_empty(),
            "candidate_cache_hit": false,
            "cache_hit": false,
            "placement_partitions_touched": placement_partitions_touched,
            "index_postings_read": placement_partitions_touched,
            "scanned_records": scanned_records,
            "returned_records": returned_records.len(),
            "non_serving_record_dropped_count": non_serving_dropped,
            "return_index_records": command.return_index_records,
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
    });
    if let Ok(mut cache) = matrixark_scan_cache().lock() {
        cache.insert(scan_cache_key, output.clone());
    }
    Ok(output)
}

fn candidate_text(record: &Value) -> String {
    for field in [
        "text",
        "content",
        "summary_text",
        "state",
        "observation",
        "entity_value",
        "description",
        "value",
    ] {
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
    ((text.len() as u64 + 3) / 4).max(1)
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

fn increment_class_count(counts: &mut HashMap<String, u64>, class_name: &str) {
    *counts.entry(class_name.to_string()).or_default() += 1;
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

fn retrieve_context_pack_native(
    engine: &TemporalEngine,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let started = Instant::now();
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
    let question_type = request
        .get("question_type")
        .and_then(Value::as_str)
        .unwrap_or("fact")
        .to_string();
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
    let scan_started = Instant::now();
    let scan = scan_matrixark_candidates(engine, &scan_command)?;
    let candidate_fetch_ms = scan_started.elapsed().as_secs_f64() * 1000.0;
    let records = scan
        .get("records")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let scope_for_continuity = scan_command.scope.clone();
    let cross_policy = parse_cross_session_policy(
        &request,
        scope_for_continuity.as_ref(),
        remote_budget,
        &question_type,
    );
    let mut raw_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut text_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    for record in &records {
        let context_class = context_class_name(record);
        increment_class_count(&mut raw_candidate_class_counts, &context_class);
        if !candidate_text(record).is_empty() {
            increment_class_count(&mut text_candidate_class_counts, &context_class);
        }
    }
    let score_started = Instant::now();
    let mut scored_candidate_class_counts: HashMap<String, u64> = HashMap::new();
    let mut score_threshold_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut scored: Vec<(f64, Value, String, f64, f64)> = records
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
        .filter_map(|record| {
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
            let context_class = context_class_name(&record);
            let session_continuity =
                session_continuity_status(&record, scope_for_continuity.as_ref());
            let continuity_boost_value =
                continuity_boost(&record, &context_class, &session_continuity);
            score += continuity_boost_value;
            let cross_session_rerank_boost_value = cross_session_rerank_boost(
                &record,
                &context_class,
                &session_continuity,
                &question_type,
            );
            score += cross_session_rerank_boost_value;
            score += type_priority_boost(&record, &context_class, &question_type);
            if score >= min_similarity_score {
                increment_class_count(&mut scored_candidate_class_counts, &context_class);
                Some((
                    score,
                    record,
                    session_continuity,
                    continuity_boost_value,
                    cross_session_rerank_boost_value,
                ))
            } else {
                increment_class_count(&mut score_threshold_dropped_class_counts, &context_class);
                None
            }
        })
        .collect();
    let score_ms = score_started.elapsed().as_secs_f64() * 1000.0;
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
    let mut selected_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_nodes: HashSet<u64> = HashSet::new();
    let mut dropped_over_budget = 0_u64;
    let mut dropped_cross_budget = 0_u64;
    let mut dropped_cross_session_cap = 0_u64;
    let mut dropped_cross_candidate_cap = 0_u64;
    let mut dropped_low_score = 0_u64;
    let mut dropped_duplicate_ref = 0_u64;
    let mut dropped_policy_ref = 0_u64;
    let mut budget_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut duplicate_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_policy_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_low_score_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_cap_dropped_class_counts: HashMap<String, u64> = HashMap::new();
    let mut selected_class_counts: HashMap<String, u64> = HashMap::new();
    let mut cross_used_tokens = 0_u64;
    let mut cross_selected_refs = 0_u64;
    let mut entity_bridge_selected_refs = 0_u64;
    let mut selected_cross_sessions: HashSet<String> = HashSet::new();
    let mut used_tokens = 0_u64;
    for (
        score,
        record,
        session_continuity,
        continuity_boost_value,
        cross_session_rerank_boost_value,
    ) in scored
    {
        if selected.len() as u64 >= max_refs {
            break;
        }
        let text = candidate_text(&record);
        let tokens = token_estimate(&text);
        let context_class = context_class_name(&record);
        if used_tokens + tokens > remote_budget {
            dropped_over_budget += 1;
            increment_class_count(&mut budget_dropped_class_counts, &context_class);
            continue;
        }
        if !is_serving_selected_ref_class(&context_class) {
            dropped_policy_ref += 1;
            increment_class_count(&mut policy_dropped_class_counts, &context_class);
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
            cross_session_key(&record)
        } else {
            String::new()
        };
        if is_cross_session && !cross_policy.enabled {
            dropped_cross_budget += 1;
            increment_class_count(&mut cross_policy_dropped_class_counts, &context_class);
            continue;
        }
        if is_cross_session && cross_policy.min_score > 0.0 && score < cross_policy.min_score {
            dropped_low_score += 1;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            continue;
        }
        if is_cross_session_raw_evidence
            && cross_policy.raw_evidence_min_score > 0.0
            && score < cross_policy.raw_evidence_min_score
        {
            dropped_low_score += 1;
            increment_class_count(&mut cross_low_score_dropped_class_counts, &context_class);
            continue;
        }
        if is_cross_session
            && cross_policy.max_candidates > 0
            && cross_selected_refs >= cross_policy.max_candidates
        {
            dropped_cross_candidate_cap += 1;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            continue;
        }
        if is_cross_session
            && cross_policy.max_sessions > 0
            && !selected_cross_sessions.contains(&cross_key)
            && selected_cross_sessions.len() as u64 >= cross_policy.max_sessions
        {
            dropped_cross_session_cap += 1;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            continue;
        }
        if is_cross_session
            && cross_policy.budget_tokens > 0
            && cross_used_tokens + tokens > cross_policy.budget_tokens
            && !(is_entity_bridge
                && entity_bridge_selected_refs < cross_policy.min_entity_bridge_refs)
        {
            dropped_cross_budget += 1;
            increment_class_count(&mut cross_cap_dropped_class_counts, &context_class);
            continue;
        }
        let ref_signature = format!(
            "{}:{}",
            context_class,
            record_ref_hash(&record).unwrap_or_else(|| {
                record
                    .get("record_id")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
        );
        if !selected_signatures.insert(ref_signature) {
            dropped_duplicate_ref += 1;
            increment_class_count(&mut duplicate_dropped_class_counts, &context_class);
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
        *selected_counts.entry(context_class.clone()).or_default() += 1;
        increment_class_count(&mut selected_class_counts, &context_class);
        if let Some(node_hash) = record_node_hash(&record) {
            selected_nodes.insert(node_hash);
        }
        selected.push(pack_ref_from_record(
            &record,
            score,
            "native_rust_proxy_score_pack",
            &session_continuity,
            continuity_boost_value,
            cross_session_rerank_boost_value,
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
    let candidate_class_counts = json!({
        "raw": raw_candidate_class_counts,
        "with_text": text_candidate_class_counts,
        "scored": scored_candidate_class_counts,
        "selected": selected_class_counts,
        "score_threshold_dropped": score_threshold_dropped_class_counts,
        "budget_dropped": budget_dropped_class_counts,
        "policy_dropped": policy_dropped_class_counts,
        "duplicate_dropped": duplicate_dropped_class_counts,
        "cross_policy_dropped": cross_policy_dropped_class_counts,
        "cross_low_score_dropped": cross_low_score_dropped_class_counts,
        "cross_cap_dropped": cross_cap_dropped_class_counts
    });
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
            "low_score": dropped_low_score,
            "duplicate_ref": dropped_duplicate_ref,
            "policy_ref": dropped_policy_ref,
            "reason_counts": {
                "over_budget": dropped_over_budget,
                "cross_session_budget": dropped_cross_budget,
                "cross_session_session_cap": dropped_cross_session_cap,
                "cross_session_candidate_cap": dropped_cross_candidate_cap,
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
                "mode": scan_command.scope.as_ref().map(session_scope_mode).unwrap_or("prefer"),
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
            },
            "candidate_class_counts": candidate_class_counts.clone()
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
        + dropped_policy_ref
        + dropped_duplicate_ref
        + scan_dropped_count;
    let candidate_cache_hit = scan_stats
        .get("candidate_cache_hit")
        .or_else(|| scan_stats.get("cache_hit"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let scanned_records = scan_stats
        .get("scanned_records")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let placement_partitions_touched = scan_stats
        .get("placement_partitions_touched")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let index_postings_read = scan_stats
        .get("index_postings_read")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let total_ms = started.elapsed().as_secs_f64() * 1000.0;
    let mut output = json!({
        "ok": true,
        "count": selected.len(),
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": scanned_records,
        "cache_hit": candidate_cache_hit,
        "selected_ref_count": selected.len(),
        "dropped_ref_count": dropped_ref_count,
        "dropped_duplicate_ref_count": dropped_duplicate_ref,
        "retrieval_metrics": {
            "query_plan_ms": 0.0,
            "node_traversal_ms": 0.0,
            "index_prefilter_ms": 0.0,
            "candidate_fetch_ms": candidate_fetch_ms,
            "score_ms": score_ms,
            "pack_ms": total_ms,
            "audit_ms": 0.0,
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": selected.len(),
            "dropped_refs": dropped_ref_count,
            "scanned_records": scanned_records,
            "index_postings_read": index_postings_read,
            "index_postings_touched": index_postings_read,
            "placement_partitions_touched": placement_partitions_touched,
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "compact_index_bucket_used": index_postings_read > 0,
            "compact_index_bucket_count": index_postings_read,
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
            ]
        },
        "context_pack": pack,
        "scan_stats": scan_stats
    });
    if let Some(metrics) = output
        .get_mut("retrieval_metrics")
        .and_then(Value::as_object_mut)
    {
        metrics.insert(
            "candidate_class_counts".to_string(),
            candidate_class_counts.clone(),
        );
    }
    Ok(output)
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
        "matrixark_publish_visibility" => {
            let visibility_key_count = request.visibility_keys.len();
            let index_bytes = engine
                .publish_shard_index_snapshot_for_keys(
                    DEFAULT_SHARD_ID,
                    request.visibility_keys.clone(),
                )
                .map_err(|status| format!("{}: {}", status.code, status.message))?;
            clear_matrixark_scan_cache();
            let mut output = empty_output(root);
            output.status = "published".to_string();
            output.count = Some(index_bytes);
            output
                .extra
                .insert("matrixark_visibility_published".to_string(), json!(true));
            output.extra.insert(
                "matrixark_visibility_index_bytes".to_string(),
                json!(index_bytes),
            );
            output.extra.insert(
                "matrixark_visibility_key_count".to_string(),
                json!(visibility_key_count),
            );
            output.extra.insert(
                "matrixark_visibility_full_shard".to_string(),
                json!(visibility_key_count == 0),
            );
            output.extra.insert(
                "matrixark_visibility_scope".to_string(),
                json!("shard_index_snapshot"),
            );
            output
        }
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
        "batch_hset" => {
            let count = request.entries.len() + request.entries_compact.len();
            let mut grouped: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
            for entry in request.entries {
                grouped
                    .entry(entry.key)
                    .or_default()
                    .push((entry.field, entry.value.into_bytes()));
            }
            for CompactHashEntry(key, field, value) in request.entries_compact {
                grouped.entry(key).or_default().push((field, value.into_bytes()));
            }
            let commands = grouped
                .into_iter()
                .map(|(key, entries)| Command::HashMultiSet { key, entries })
                .collect::<Vec<_>>();
            execute_empty_batch_runtime(&engine, commands, false)?;
            let mut output = empty_output(root);
            output.count = Some(count);
            output
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            let mut count = request.entries.len() + request.entries_compact.len();
            let mut grouped: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
            for entry in request.entries {
                grouped
                    .entry(entry.key)
                    .or_default()
                    .push((entry.field, entry.value.into_bytes()));
            }
            for CompactHashEntry(key, field, value) in request.entries_compact {
                grouped
                    .entry(key)
                    .or_default()
                    .push((field, value.into_bytes()));
            }
            let mut commands =
                Vec::with_capacity(grouped.len() + usize::from(!request.key.trim().is_empty()));
            for (key, entries) in grouped {
                commands.push(Command::HashMultiSet { key, entries });
            }
            if !request.key.trim().is_empty() {
                commands.push(Command::StringSet {
                    key: request.key,
                    value: request.value.into_bytes(),
                });
                count += 1;
            }
            execute_empty_batch_runtime(&engine, commands, true)?;
            let mut output = empty_output(root);
            output.count = Some(count);
            output.append_path = request
                .append_options
                .get("append_path")
                .and_then(Value::as_str)
                .unwrap_or("native_batch_append_records")
                .to_string();
            output.raw_storage_backend = request
                .append_options
                .get("raw_storage_backend")
                .and_then(Value::as_str)
                .unwrap_or("temporalstore")
                .to_string();
            output.extra.insert(
                "matrixark_append_write_path".to_string(),
                json!("rust_proxy_matrixark_batch_runtime_default"),
            );
            output.extra.insert(
                "matrixark_batch_uses_forced_sync_durable_writes".to_string(),
                json!(false),
            );
            output.extra.insert(
                "matrixark_batch_storage_visibility".to_string(),
                json!("runtime_multiplexed_proxy"),
            );
            output
        }
        "batch_hget" => {
            let mut grouped: BTreeMap<String, Vec<String>> = BTreeMap::new();
            for entry in request.entries {
                grouped.entry(entry.key).or_default().push(entry.field);
            }
            for CompactHashEntry(key, field, _) in request.entries_compact {
                grouped.entry(key).or_default().push(field);
            }
            let mut records =
                Vec::with_capacity(grouped.values().map(|fields| fields.len()).sum::<usize>());
            let grouped_entries = grouped.into_iter().collect::<Vec<_>>();
            let commands = grouped_entries
                .iter()
                .map(|(key, fields)| Command::HashMultiGet {
                    key: key.clone(),
                    fields: fields.clone(),
                })
                .collect::<Vec<_>>();
            let response = engine.batch_execute(BatchExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                commands,
            });
            if !response.status.ok {
                return Err(format!(
                    "{}: {}",
                    response.status.code, response.status.message
                ));
            }
            if response.responses.len() != grouped_entries.len() {
                return Err(format!(
                    "batch_hget response count mismatch: expected {} got {}",
                    grouped_entries.len(),
                    response.responses.len()
                ));
            }
            for ((key, fields), item) in grouped_entries.into_iter().zip(response.responses) {
                if !item.status.ok {
                    return Err(format!("{}: {}", item.status.code, item.status.message));
                }
                let values = match item.response {
                    CommandResponse::Values { values } => values,
                    other => return Err(format!("unexpected response for batch_hget: {other:?}")),
                };
                for (field, value) in fields.into_iter().zip(values.into_iter()) {
                    let value = value
                        .map(|bytes| {
                            String::from_utf8(bytes)
                                .map_err(|error| format!("stored value is not UTF-8: {error}"))
                        })
                        .transpose()?
                        .unwrap_or_default();
                    records.push(HashReadRecord {
                        key: key.clone(),
                        field,
                        value,
                    });
                }
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
        "matrixark_scan_candidates" => {
            json_output(scan_matrixark_candidates(&engine, &request)?, root)?
        }
        "matrixark_retrieve_context_pack" => {
            if matrixark_compact_snapshot_retrieve_enabled() {
                retrieve_context_pack_output(&engine, &request, root)?
            } else {
                json_output(retrieve_context_pack_native(&engine, &request)?, root)?
            }
        }
        "matrixark_retrieve_context_pack_full_scan" => {
            json_output(retrieve_context_pack_native(&engine, &request)?, root)?
        }
        other => return Err(format!("unsupported op {other:?}")),
    };
    Ok(output)
}

fn validate_request(request: &RecordLogRequest) -> Result<(), String> {
    if request.op.trim().is_empty() {
        return Err("missing op".to_string());
    }
    match request.op.as_str() {
        "health"
        | "readiness"
        | "preflight"
        | "metrics_prometheus"
        | "shutdown"
        | "matrixark_publish_visibility" => Ok(()),
        "put_string" | "get_string" | "delete" | "del" | "hgetall" | "scan_hash" => {
            require_non_empty("key", &request.key)
        }
        "matrixark_scan_candidates"
        | "matrixark_retrieve_context_pack"
        | "matrixark_retrieve_context_pack_full_scan" => {
            require_non_empty("count_key", request.count_key.as_deref().unwrap_or(""))?;
            require_non_empty(
                "record_hash_key",
                request.record_hash_key.as_deref().unwrap_or(""),
            )
        }
        "hset" | "hget" | "hdel" => {
            require_non_empty("key", &request.key)?;
            require_non_empty("field", &request.field)
        }
        "batch_hset" | "batch_hget" => {
            if request.entries.is_empty() && request.entries_compact.is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &request.entries {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
            }
            for CompactHashEntry(key, field, _) in &request.entries_compact {
                require_non_empty("key", key)?;
                require_non_empty("field", field)?;
            }
            Ok(())
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            if request.entries.is_empty()
                && request.entries_compact.is_empty()
                && request.key.trim().is_empty()
            {
                return Err("missing entries".to_string());
            }
            for entry in &request.entries {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
            }
            for CompactHashEntry(key, field, _) in &request.entries_compact {
                require_non_empty("key", key)?;
                require_non_empty("field", field)?;
            }
            Ok(())
        }
        other => Err(format!("unsupported op {other:?}")),
    }
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
        command: Command::HashGetAll { key: key.clone() },
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
            let mut records = Vec::new();
            for (field, value) in entries {
                let value = String::from_utf8(value)
                    .map_err(|error| format!("stored hash value is not UTF-8: {error}"))?;
                records.push(HashReadRecord {
                    key: key.clone(),
                    field: field.clone(),
                    value: value.clone(),
                });
                decoded.insert(field, value);
            }
            let mut extra = BTreeMap::new();
            extra.insert("native_prefix_scan".to_string(), json!(true));
            extra.insert(
                "prefix_scan_path".to_string(),
                json!("rust_proxy_scan_hash"),
            );
            Ok(RecordLogOutput {
                value: serde_json::to_string(&decoded)
                    .map_err(|error| format!("failed to serialize hash entries: {error}"))?,
                count: Some(decoded.len()),
                entries: decoded,
                records,
                root,
                status: String::new(),
                mode: String::new(),
                append_path: String::new(),
                raw_storage_backend: String::new(),
                prometheus: String::new(),
                cached_clients: None,
                extra,
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
    {
        let cache = engine_cache()
            .lock()
            .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
        if let Some(engine) = cache.get(&root) {
            return Ok(engine.clone());
        }
    }
    std::fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create record-log root {}: {error}",
            root.display()
        )
    })?;
    let cache_bytes = env::var("MATRIXARK_RUST_PROXY_CACHE_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(128 * 1024 * 1024);
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        cache_bytes,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
        matrixark_proxy_block_store_options(),
    );
    engine.load_shard(DEFAULT_SHARD_ID);
    let _ = engine.set_config(SetConfigRequest {
        shard_id: DEFAULT_SHARD_ID,
        config: Config {
            version: 2,
            async_storage: env::var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE")
                .ok()
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(true),
            ..Config::default()
        },
    });
    let mut cache = engine_cache()
        .lock()
        .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
    cache.insert(root, engine.clone());
    Ok(engine)
}

fn matrixark_proxy_block_store_options() -> BlockStoreOptions {
    let defaults = BlockStoreOptions::default();
    BlockStoreOptions {
        compression_enabled: env_bool_any(
            &[
                "MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_ENABLED",
                "TS_PAGE_STORE_COMPRESSION_ENABLED",
            ],
            defaults.compression_enabled,
        ),
        compression_min_bytes: env_usize_any(
            &[
                "MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_MIN_BYTES",
                "TS_PAGE_STORE_COMPRESSION_MIN_BYTES",
            ],
            4096,
        ),
        compression_level: env_i32_any(
            &[
                "MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_LEVEL",
                "TS_PAGE_STORE_COMPRESSION_LEVEL",
            ],
            defaults.compression_level,
        ),
    }
}

fn env_bool_any(names: &[&str], default: bool) -> bool {
    names
        .iter()
        .find_map(|name| env::var(name).ok())
        .map(|value| {
            matches!(
                value.trim(),
                "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON"
            )
        })
        .unwrap_or(default)
}

fn env_usize_any(names: &[&str], default: usize) -> usize {
    names
        .iter()
        .find_map(|name| env::var(name).ok())
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_i32_any(names: &[&str], default: i32) -> i32 {
    names
        .iter()
        .find_map(|name| env::var(name).ok())
        .and_then(|value| value.trim().parse::<i32>().ok())
        .unwrap_or(default)
}

fn execute_empty(engine: &TemporalEngine, command: Command) -> Result<(), String> {
    let retrieve_cache_keys = match &command {
        Command::HashSet { key, .. }
        | Command::HashMultiSet { key, .. }
        | Command::HashDelete { key, .. }
        | Command::CommonDelete { key }
        | Command::StringSet { key, .. } => vec![key.clone()],
        _ => Vec::new(),
    };
    let cache_update = match &command {
        Command::HashSet { key, field, value } if hgetall_snapshot_contains(key) => {
            Some((key.clone(), vec![(field.clone(), value.clone())]))
        }
        Command::HashMultiSet { key, entries } if hgetall_snapshot_contains(key) => {
            Some((key.clone(), entries.clone()))
        }
        _ => None,
    };
    let cache_invalidate = match &command {
        Command::HashDelete { key, .. } | Command::CommonDelete { key } => Some(key.clone()),
        _ => None,
    };
    let record_count_update = match &command {
        Command::StringSet { key, value } => Some((key.clone(), value.clone())),
        _ => None,
    };
    let record_count_invalidate = match &command {
        Command::CommonDelete { key } | Command::StringDelete { key } => Some(key.clone()),
        _ => None,
    };
    let response = engine.execute(ExecuteRequest {
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
        CommandResponse::Empty => {
            if let Some((key, value)) = record_count_update {
                update_record_count_cache(&key, &value);
            }
            if let Some(key) = record_count_invalidate {
                invalidate_record_count_cache(&key);
            }
            if let Some((key, entries)) = cache_update {
                update_hgetall_snapshot_fields(&key, &entries);
            }
            if let Some(key) = cache_invalidate {
                invalidate_hgetall_snapshot(&key);
            }
            invalidate_retrieve_candidate_cache_for_keys(retrieve_cache_keys.iter());
            clear_matrixark_scan_cache();
            Ok(())
        }
        other => Err(format!("unexpected response for write: {other:?}")),
    }
}

fn execute_empty_batch_runtime(
    engine: &TemporalEngine,
    commands: Vec<Command>,
    invalidate_matrixark_scan_cache: bool,
) -> Result<(), String> {
    if commands.is_empty() {
        return Ok(());
    }
    let retrieve_cache_keys = commands
        .iter()
        .filter_map(|command| match command {
            Command::HashSet { key, .. }
            | Command::HashMultiSet { key, .. }
            | Command::HashDelete { key, .. }
            | Command::CommonDelete { key }
            | Command::StringSet { key, .. } => Some(key.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let cache_updates = commands
        .iter()
        .filter_map(|command| match command {
            Command::HashSet { key, field, value } if hgetall_snapshot_contains(key) => {
                Some((key.clone(), vec![(field.clone(), value.clone())]))
            }
            Command::HashMultiSet { key, entries } if hgetall_snapshot_contains(key) => {
                Some((key.clone(), entries.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    let cache_invalidates = commands
        .iter()
        .filter_map(|command| match command {
            Command::HashDelete { key, .. } | Command::CommonDelete { key } => Some(key.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let record_count_updates = commands
        .iter()
        .filter_map(|command| match command {
            Command::StringSet { key, value } => Some((key.clone(), value.clone())),
            _ => None,
        })
        .collect::<Vec<_>>();
    let record_count_invalidates = commands
        .iter()
        .filter_map(|command| match command {
            Command::CommonDelete { key } | Command::StringDelete { key } => Some(key.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let response = engine.batch_execute(BatchExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        commands,
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    for item in response.responses {
        if !item.status.ok {
            return Err(format!("{}: {}", item.status.code, item.status.message));
        }
        if !matches!(item.response, CommandResponse::Empty) {
            return Err(format!(
                "unexpected response for batch write: {:?}",
                item.response
            ));
        }
    }
    for (key, entries) in cache_updates {
        update_hgetall_snapshot_fields(&key, &entries);
    }
    for (key, value) in record_count_updates {
        update_record_count_cache(&key, &value);
    }
    for key in record_count_invalidates {
        invalidate_record_count_cache(&key);
    }
    for key in cache_invalidates {
        invalidate_hgetall_snapshot(&key);
    }
    invalidate_retrieve_candidate_cache_for_keys(retrieve_cache_keys.iter());
    if invalidate_matrixark_scan_cache {
        clear_matrixark_scan_cache();
    }
    Ok(())
}

fn read_bytes(engine: &TemporalEngine, command: Command) -> Result<String, String> {
    let response = engine.execute(ExecuteRequest {
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

fn read_record_count(engine: &TemporalEngine, key: &str) -> Result<String, String> {
    if let Ok(cache) = record_count_cache().lock() {
        if let Some(value) = cache.get(key) {
            return Ok(value.clone());
        }
    }
    let value = read_bytes(
        engine,
        Command::StringGet {
            key: key.to_string(),
        },
    )?;
    if !value.trim().is_empty() {
        if let Ok(mut cache) = record_count_cache().lock() {
            cache.insert(key.to_string(), value.clone());
        }
    }
    Ok(value)
}

fn load_retrieve_candidate_snapshot(
    engine: &TemporalEngine,
    storage_prefix: &str,
    record_hash_key: &str,
    count: usize,
    scope: Option<&Value>,
    secondary_groups: &[Vec<String>],
) -> Result<(Arc<RetrieveCandidateSnapshot>, bool), String> {
    let cache_key = retrieve_candidate_cache_key(storage_prefix, count, scope, secondary_groups);
    if let Ok(cache) = retrieve_candidate_cache().lock() {
        if let Some(snapshot) = cache.get(&cache_key) {
            return Ok((Arc::clone(snapshot), true));
        }
    }

    let shard_count = if count == 0 {
        0
    } else {
        (count + DIRECT_RECORD_LOG_SHARD_SIZE - 1) / DIRECT_RECORD_LOG_SHARD_SIZE
    };
    let mut records = Vec::new();
    for shard in 0..shard_count {
        let key = format!("{record_hash_key}:{shard:06}");
        for payload in hgetall_map(engine, key)?.values() {
            if payload.trim().is_empty() {
                continue;
            }
            flatten_context_payload(payload, &mut records);
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

    let candidates = records
        .iter()
        .filter(|record| scope_matches_record(record, scope))
        .filter(|record| {
            if secondary_groups.is_empty() {
                return true;
            }
            let terms = record_index_terms(
                record,
                &index_terms_by_batch,
                &index_terms_by_node,
                &index_terms_by_ref,
            );
            terms.is_empty() || passes_secondary_groups(&terms, secondary_groups)
        })
        .filter(|record| is_serving_context_record(record))
        .filter_map(|record| {
            let text = context_record_text(record);
            let lower_text = text.to_ascii_lowercase();
            let selected_ref = selected_ref_from_record(record, &text);
            if selected_ref.is_null() {
                None
            } else {
                Some(CachedRetrieveCandidate {
                    selected_ref,
                    lower_text,
                })
            }
        })
        .collect::<Vec<_>>();
    let snapshot = Arc::new(RetrieveCandidateSnapshot {
        candidates,
        scanned_records: records.len(),
        placement_partitions_touched: shard_count,
        index_postings_read: shard_count,
    });
    if let Ok(mut cache) = retrieve_candidate_cache().lock() {
        cache.insert(cache_key, Arc::clone(&snapshot));
    }
    Ok((snapshot, false))
}

fn retrieve_context_pack_output(
    engine: &TemporalEngine,
    request: &RecordLogRequest,
    root: PathBuf,
) -> Result<RecordLogOutput, String> {
    let started = Instant::now();
    let storage_prefix = storage_prefix_from_request(request);
    if storage_prefix.is_empty() {
        return Err("missing storage_prefix or count_key-derived storage prefix".to_string());
    }
    let count_key = format!("{storage_prefix}:record_count");
    let count_raw = read_record_count(engine, &count_key)?;
    let count = count_raw.trim().parse::<usize>().unwrap_or_default();
    let record_hash_key = request
        .record_hash_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("{storage_prefix}:records"));
    let request_record = request.record.clone().unwrap_or_else(|| json!({}));
    let scope = request
        .scope
        .as_ref()
        .or_else(|| request_record.get("scope"));
    let secondary_groups = request
        .secondary_index_groups
        .clone()
        .or_else(|| {
            request_record
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
        })
        .unwrap_or_default();
    let (snapshot, candidate_cache_hit) = load_retrieve_candidate_snapshot(
        engine,
        &storage_prefix,
        &record_hash_key,
        count,
        scope,
        &secondary_groups,
    )?;

    let requested_max_selected_refs = request.max_selected_refs.max(
        request_record
            .pointer("/ranking/max_selected_refs")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
    );
    let max_selected_refs = if requested_max_selected_refs == 0 {
        24
    } else {
        requested_max_selected_refs
    }
    .clamp(1, 128);
    let query = if request.query.trim().is_empty() {
        request_record
            .get("query")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string()
    } else {
        request.query.clone()
    };
    let query_terms = query_terms(&query);
    let score_started = Instant::now();
    let mut candidates = Vec::with_capacity(snapshot.candidates.len());
    for (ordinal, candidate) in snapshot.candidates.iter().enumerate() {
        let score = score_lowered_text(&candidate.lower_text, &query_terms);
        candidates.push((score, ordinal));
    }
    let score_ms = score_started.elapsed().as_secs_f64() * 1000.0;
    let keep = max_selected_refs.min(candidates.len());
    if keep > 0 && candidates.len() > keep {
        candidates
            .select_nth_unstable_by(keep, |left, right| compare_scored_candidate(*left, *right));
        candidates.truncate(keep);
    }
    candidates.sort_by(|left, right| compare_scored_candidate(*left, *right));
    let selected_refs: Vec<Value> = candidates
        .into_iter()
        .filter_map(|(_, ordinal)| snapshot.candidates.get(ordinal))
        .map(|candidate| candidate.selected_ref.clone())
        .filter(|selected_ref| !selected_ref.is_null())
        .take(max_selected_refs)
        .collect();
    let selected_count = selected_refs.len();
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let correctness = selected_count > 0;
    let pack = json!({
        "context_pack_id": format!("rust-native-{}-{}", unix_ms(), stable_hash64(&query)),
        "context_pack_assembly": "native_rust_proxy",
        "native_context_pack": true,
        "selected_refs": selected_refs,
        "dropped_refs": {
            "refs": [],
            "native_summary": true,
        },
        "retrieval_metrics": {
            "query_plan_ms": 0.0,
            "node_traversal_ms": 0.0,
            "index_prefilter_ms": 0.0,
            "candidate_fetch_ms": elapsed_ms,
            "score_ms": score_ms,
            "pack_ms": 0.0,
            "audit_ms": 0.0,
            "append_queue_wait_ms": 0.0,
            "append_engine_ms": 0.0,
            "selected_refs": selected_count,
            "dropped_refs": 0,
            "scanned_records": snapshot.scanned_records,
            "index_postings_read": snapshot.index_postings_read,
            "placement_partitions_touched": snapshot.placement_partitions_touched,
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
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
    let response = json!({
        "ok": true,
        "count": selected_count,
        "native_pack_assembly": true,
        "raw_records_returned": false,
        "python_hot_path_records": 0,
        "scan_count": snapshot.scanned_records,
        "cache_hit": candidate_cache_hit,
        "selected_ref_count": selected_count,
        "dropped_ref_count": 0,
        "retrieval_metrics": pack
            .get("retrieval_metrics")
            .cloned()
            .unwrap_or_else(|| json!({})),
        "context_pack": pack,
    });
    let mut output = empty_output(root);
    output.count = Some(selected_count);
    output.mode = "rust_proxy_native_context_pack".to_string();
    if request.top_level_response {
        if let Some(object) = response.as_object() {
            for (key, value) in object {
                if !matches!(key.as_str(), "ok" | "count") {
                    output.extra.insert(key.clone(), value.clone());
                }
            }
        }
    } else {
        output.value = serde_json::to_string(&response)
            .map_err(|error| format!("failed to serialize native context pack: {error}"))?;
    }
    Ok(output)
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
    let public_ref_type = match record_type {
        "context_event" | "context_compression_event" => "event",
        "context_summary" => "summary",
        "context_entity" => "entity",
        "resource_chunk" => "resource",
        "skill_section" => "skill",
        other => other,
    };
    let ref_hash = stable_ref_hash_from_record(record);
    json!({
        "ref_type": public_ref_type,
        "ref_hash": ref_hash,
        "text": text,
    })
}

fn stable_ref_hash_from_record(record: &Value) -> u64 {
    for key in [
        "ref_hash",
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "chunk_hash",
        "section_hash",
    ]
    .iter()
    {
        if let Some(value) = record.get(*key) {
            if let Some(hash) = value.as_u64() {
                return hash;
            }
            if let Some(hash) = value.as_str().and_then(|raw| raw.parse::<u64>().ok()) {
                return hash;
            }
        }
    }
    for key in [
        "record_id",
        "event_id",
        "entity_id",
        "summary_id",
        "chunk_id",
        "section_id",
        "source_ref",
    ] {
        if let Some(value) = record.get(key).and_then(Value::as_str) {
            if !value.is_empty() {
                return stable_hash64(value);
            }
        }
    }
    stable_hash64(&record.to_string())
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

fn score_lowered_text(lowered: &str, query_terms: &[String]) -> f64 {
    if query_terms.is_empty() {
        return 0.0;
    }
    query_terms
        .iter()
        .filter(|term| lowered.contains(term.as_str()))
        .count() as f64
        / query_terms.len() as f64
}

fn compare_scored_candidate(left: (f64, usize), right: (f64, usize)) -> std::cmp::Ordering {
    right
        .0
        .partial_cmp(&left.0)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.1.cmp(&right.1))
}

fn record_log_root(request: &RecordLogRequest) -> PathBuf {
    if let Ok(root) = env::var("MATRIXARK_TEMPORALSTORE_RUST_ROOT") {
        return PathBuf::from(root);
    }
    let namespace = non_empty_or(&request.namespace, "deploy_ns");
    let table = non_empty_or(&request.table, "deploy_table");
    let metaserver_hash = stable_hash64(non_empty_or(&request.metaserver, "local"));
    let mut root = env::temp_dir()
        .join("temporalstore-rust-matrixark")
        .join(sanitize_path_component(namespace))
        .join(sanitize_path_component(table))
        .join(format!("{metaserver_hash:016x}"));
    if let Some(prefix) = matrixark_storage_prefix_partition(request) {
        root = root.join(format!("prefix_{:016x}", stable_hash64(&prefix)));
    }
    root
}

fn matrixark_storage_prefix_partition(request: &RecordLogRequest) -> Option<String> {
    let mut candidates: Vec<&str> = Vec::new();
    candidates.push(&request.key);
    if let Some(count_key) = request.count_key.as_deref() {
        candidates.push(count_key);
    }
    if let Some(record_hash_key) = request.record_hash_key.as_deref() {
        candidates.push(record_hash_key);
    }
    for entry in &request.entries {
        candidates.push(&entry.key);
    }
    for CompactHashEntry(key, _, _) in &request.entries_compact {
        candidates.push(key);
    }
    candidates
        .into_iter()
        .filter_map(matrixark_storage_prefix_from_key)
        .next()
}

fn matrixark_storage_prefix_from_key(key: &str) -> Option<String> {
    let trimmed = key.trim();
    if !trimmed.starts_with("matrixark:mcp:") {
        return None;
    }
    for marker in [
        ":records",
        ":record_count",
        ":record_index",
        ":event_time",
        ":readiness",
        ":direct_write_queue",
    ] {
        if let Some((prefix, _)) = trimmed.split_once(marker) {
            if !prefix.is_empty() {
                return Some(prefix.to_string());
            }
        }
    }
    Some(trimmed.to_string())
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
            "matrixark_retrieve_context_pack",
            "matrixark_publish_visibility"
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
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
            count_key: None,
            record_hash_key: None,
            shard_size: None,
            record_types: None,
            selected_node_hashes: None,
            secondary_index_groups: None,
            scope: None,
            return_index_records: false,
            record: None,
            visibility_keys: Vec::new(),
            top_level_response: false,
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

        let mut prefixed = request("hset");
        prefixed.key = "matrixark:mcp:scale:rust:abc:records:000000".to_string();
        let prefixed_root = record_log_root(&prefixed);
        assert!(prefixed_root.to_string_lossy().contains("prefix_"));
        assert_ne!(first_root, prefixed_root);

        let mut same_prefix_count = request("put_string");
        same_prefix_count.key = "matrixark:mcp:scale:rust:abc:record_count".to_string();
        assert_eq!(prefixed_root, record_log_root(&same_prefix_count));

        let mut compact_only = request("batch_hset");
        compact_only.entries_compact = vec![CompactHashEntry(
            "matrixark:mcp:scale:rust:abc:records:000001".to_string(),
            "00000000000000000001".to_string(),
            "{}".to_string(),
        )];
        assert_eq!(prefixed_root, record_log_root(&compact_only));
    }

    #[test]
    fn matrixark_proxy_block_store_options_default_to_throughput_threshold() {
        let _guard = env_guard();
        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_ENABLED");
        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_MIN_BYTES");
        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_LEVEL");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_ENABLED");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_MIN_BYTES");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_LEVEL");

        let options = matrixark_proxy_block_store_options();
        assert!(options.compression_enabled);
        assert_eq!(options.compression_min_bytes, 4096);
        assert_eq!(
            options.compression_level,
            BlockStoreOptions::default().compression_level
        );

        env::set_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_ENABLED", "false");
        env::set_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_MIN_BYTES", "8192");
        env::set_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_LEVEL", "3");
        let overridden = matrixark_proxy_block_store_options();
        assert!(!overridden.compression_enabled);
        assert_eq!(overridden.compression_min_bytes, 8192);
        assert_eq!(overridden.compression_level, 3);

        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_ENABLED");
        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_MIN_BYTES");
        env::remove_var("MATRIXARK_RUST_PROXY_PAGE_COMPRESSION_LEVEL");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_ENABLED");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_MIN_BYTES");
        env::remove_var("TS_PAGE_STORE_COMPRESSION_LEVEL");
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
    fn matrixark_publish_visibility_makes_async_writes_visible_to_reopened_engine() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::set_var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE", "true");
        clear_engine_cache();

        let storage_prefix = "matrixark:test:publish";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_type":"context_event","text":"published async page"}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");
        let publish = request("matrixark_publish_visibility");
        let output =
            execute_record_log_request(&engine, publish, root).expect("publish visibility");
        assert_eq!(output.status, "published");
        assert_eq!(
            output.extra.get("matrixark_visibility_published"),
            Some(&json!(true))
        );

        clear_engine_cache();
        let reopened_request = request("get_string");
        let reopened = open_engine(&reopened_request).expect("reopened engine");
        assert_eq!(
            read_bytes(
                &reopened,
                Command::StringGet {
                    key: format!("{storage_prefix}:record_count"),
                },
            )
            .expect("get published count"),
            "1"
        );
        assert_eq!(
            read_bytes(
                &reopened,
                Command::HashGet {
                    key: format!("{storage_prefix}:records:000000"),
                    field: "00000000000000000000".to_string(),
                },
            )
            .expect("get published hash field"),
            r#"{"record_type":"context_event","text":"published async page"}"#
        );

        clear_engine_cache();
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
        env::remove_var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE");
    }

    #[test]
    fn matrixark_publish_visibility_can_target_only_written_keys() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::set_var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE", "true");
        clear_engine_cache();

        let storage_prefix = "matrixark:test:targeted-publish";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![
            CompactHashEntry(
                format!("{storage_prefix}:records:000000"),
                "00000000000000000000".to_string(),
                r#"{"record_type":"context_event","text":"target published"}"#.to_string(),
            ),
            CompactHashEntry(
                format!("{storage_prefix}:records:000001"),
                "00000000000000000001".to_string(),
                r#"{"record_type":"context_event","text":"target not published"}"#.to_string(),
            ),
        ];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");
        let mut publish = request("matrixark_publish_visibility");
        publish.visibility_keys = vec![
            format!("{storage_prefix}:record_count"),
            format!("{storage_prefix}:records:000000"),
        ];
        let publish_output =
            execute_record_log_request(&engine, publish.clone(), root.clone())
                .expect("publish selected visibility");
        assert!(
            publish_output.count.unwrap_or_default() > 0,
            "first targeted publish should persist selected hot pages"
        );
        assert_eq!(
            publish_output.extra.get("matrixark_visibility_key_count"),
            Some(&json!(2)),
            "publish diagnostics should report targeted key fanout"
        );
        assert_eq!(
            publish_output.extra.get("matrixark_visibility_full_shard"),
            Some(&json!(false)),
            "targeted publish diagnostics should not look like a full-shard publish"
        );
        let republish_output =
            execute_record_log_request(&engine, publish, root.clone())
                .expect("republish selected visibility");
        assert_eq!(
            republish_output.count,
            Some(0),
            "republishing the same clean keys should not rewrite visibility"
        );

        clear_engine_cache();
        let reopened = open_engine(&request("get_string")).expect("reopened engine");
        assert_eq!(
            read_bytes(
                &reopened,
                Command::StringGet {
                    key: format!("{storage_prefix}:record_count"),
                },
            )
            .expect("get targeted count"),
            "1"
        );
        assert_eq!(
            read_bytes(
                &reopened,
                Command::HashGet {
                    key: format!("{storage_prefix}:records:000000"),
                    field: "00000000000000000000".to_string(),
                },
            )
            .expect("get targeted hash field"),
            r#"{"record_type":"context_event","text":"target published"}"#
        );
        assert_eq!(
            read_bytes(
                &reopened,
                Command::HashGet {
                    key: format!("{storage_prefix}:records:000001"),
                    field: "00000000000000000001".to_string(),
                },
            )
            .expect("get untargeted hash field"),
            ""
        );

        clear_engine_cache();
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
        env::remove_var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE");
    }

    #[test]
    fn matrixark_native_retrieve_context_pack_returns_selected_refs() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::remove_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN");

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
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.query = "Who approved GPU budget and who owns procurement?".to_string();
        retrieve.max_selected_refs = 4;
        let output = execute_record_log_request(&engine, retrieve.clone(), root.clone())
            .expect("native retrieve through proxy op");
        let response: Value = serde_json::from_str(&output.value).expect("context pack json");
        let pack = response
            .get("context_pack")
            .expect("wrapped context pack from proxy op");
        let refs = pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("selected refs");
        assert_eq!(refs.len(), 2);
        let ref_types: BTreeSet<_> = refs
            .iter()
            .filter_map(|value| value.get("ref_type").and_then(Value::as_str))
            .collect();
        assert!(ref_types.contains("event"));
        assert!(ref_types.contains("entity"));
        assert_eq!(
            pack.pointer("/retrieval_metrics/native_pack_assembly")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/candidate_cache_hit")
                .and_then(Value::as_bool),
            Some(false)
        );

        let cached_output = execute_record_log_request(&engine, retrieve.clone(), root.clone())
            .expect("native retrieve cache hit through proxy op");
        let cached_response: Value =
            serde_json::from_str(&cached_output.value).expect("cached context pack json");
        assert_eq!(
            cached_response
                .pointer("/retrieval_metrics/candidate_cache_hit")
                .and_then(Value::as_bool),
            Some(true)
        );

        let mut default_ref_limit = retrieve.clone();
        default_ref_limit.max_selected_refs = 0;
        let default_limit_output = execute_record_log_request(&engine, default_ref_limit, root)
            .expect("native retrieve default ref limit through proxy op");
        let default_limit_response: Value =
            serde_json::from_str(&default_limit_output.value).expect("default context pack json");
        let default_refs = default_limit_response
            .pointer("/context_pack/selected_refs")
            .and_then(Value::as_array)
            .expect("default selected refs");
        assert_eq!(default_refs.len(), 2);

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    #[test]
    fn selected_ref_hash_prefers_string_hashes_and_stable_ids() {
        let numeric_string = json!({
            "record_type": "context_event",
            "event_id_hash": "42",
            "record_id": "slow-fallback-should-not-win",
            "text": "visible text"
        });
        assert_eq!(stable_ref_hash_from_record(&numeric_string), 42);

        let stable_id = json!({
            "record_type": "context_summary",
            "record_id": "summary-record-7",
            "text": "summary text"
        });
        assert_eq!(
            stable_ref_hash_from_record(&stable_id),
            stable_hash64("summary-record-7")
        );
    }
}
