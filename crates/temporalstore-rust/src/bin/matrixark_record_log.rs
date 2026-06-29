use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::json;
use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

const DEFAULT_SHARD_ID: u64 = 1;
const LATENCY_BUCKETS_MS: [u128; 9] = [1, 2, 5, 10, 25, 50, 100, 250, 1000];

#[derive(Debug, Deserialize)]
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
    entries: Vec<HashEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HashEntry {
    key: String,
    field: String,
    #[serde(default)]
    value: String,
}

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
    prometheus: String,
    cached_clients: Option<usize>,
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
                    mode: "long_lived_stdio_gateway".to_string(),
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
                    mode: "long_lived_stdio_gateway".to_string(),
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
                mode: "long_lived_stdio_gateway".to_string(),
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
            "matrixark_backend_info{{backend=\"rust\",storage_mode=\"rust-gateway\"}} 1\n",
            "# HELP matrixark_backend_ready MatrixArk backend readiness state, 1 for ready and 0 for not ready.\n",
            "# TYPE matrixark_backend_ready gauge\n",
            "matrixark_backend_ready{{backend=\"rust\",storage_mode=\"rust-gateway\",status=\"ready\"}} 1\n",
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
            prometheus: String::new(),
            cached_clients: None,
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
            let mut count = request.entries.len();
            for entry in request.entries {
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
        "hset" | "hget" | "hdel" => {
            require_non_empty("key", &request.key)?;
            require_non_empty("field", &request.field)
        }
        "batch_hset" | "batch_hget" => {
            if request.entries.is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &request.entries {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
            }
            Ok(())
        }
        "matrixark_append_records" | "matrixark_batch_append_records" => {
            if request.entries.is_empty() && request.key.trim().is_empty() {
                return Err("missing entries".to_string());
            }
            for entry in &request.entries {
                require_non_empty("key", &entry.key)?;
                require_non_empty("field", &entry.field)?;
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
        prometheus: String::new(),
        cached_clients: None,
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
        prometheus: String::new(),
        cached_clients: None,
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
                prometheus: String::new(),
                cached_clients: None,
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
            "scan_hash"
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
            entries: Vec::new(),
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
}
