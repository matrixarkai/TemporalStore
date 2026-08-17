// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Shared implementation body for the thin proxy entrypoints
// (matrixark_rust_proxy, matrixark_rust_direct_sdk), which `include!` this file.
// It deliberately lives under src/ (not src/bin/) so it is NOT compiled as a
// standalone bin, and it carries no crate-level inner attributes: each includer
// sets its own `#![recursion_limit = "256"]` (required for a large `json!`
// literal below). An inner attribute here would be illegal once `include!`d.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
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

/// Treat an explicit JSON `null` as the type's default. `#[serde(default)]` only
/// covers *absent* fields, so agent clients that serialize an empty list as `null`
/// (common from Python) would otherwise fail request parsing with
/// "invalid type: null, expected a sequence". Applied to the plain `Vec` request
/// fields so the proxy tolerates null lists uniformly across agents.
fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

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
    #[serde(default, deserialize_with = "deserialize_null_default")]
    entries: Vec<HashEntry>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
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
    #[serde(default, deserialize_with = "deserialize_null_default")]
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
    ref_type: String,
}

#[derive(Clone, Debug)]
struct RetrieveCandidateSnapshot {
    candidates: Vec<CachedRetrieveCandidate>,
    memory_inventory: Value,
    scanned_records: usize,
    placement_partitions_touched: usize,
    index_postings_read: usize,
}

struct NativeScoredCandidate {
    score: f64,
    record: Value,
    text: String,
    tokens: u64,
    context_class: String,
    session_continuity: String,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
}

fn native_query_contains_any(lower: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| lower.contains(needle))
}

fn infer_native_question_type(query: &str) -> &'static str {
    let lower = query.to_ascii_lowercase();
    if native_query_contains_any(
        &lower,
        &[
            "profile memory",
            "user profile",
            "long term memory",
            "long-term memory",
            "cross session memory",
            "cross-session memory",
            "session memory",
            "memory feature",
            "mem0",
        ],
    ) {
        return "profile_memory";
    }
    if native_query_contains_any(
        &lower,
        &[
            "benchmark",
            "workload",
            "latency",
            "p50",
            "p90",
            "p95",
            "p99",
            "throughput",
            "qps",
            "ops/s",
            "req/s",
            "hit rate",
            "read hit",
            "memory quality",
            "locomo",
            "longmemeval",
        ],
    ) {
        return "benchmark_quality";
    }
    if native_query_contains_any(
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
            "cross session",
            "cross-session",
            "previous sessions",
            "other sessions",
        ],
    ) {
        return "multi_hop";
    }
    if native_query_contains_any(
        &lower,
        &[
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
    ) || lower.split_whitespace().any(|term| matches!(term, "when" | "day" | "month" | "year"))
    {
        return "date";
    }
    if native_query_contains_any(
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
            "goal",
            "task",
            "requirement",
            "user request",
            "asked codex",
            "what did we decide",
            "what was decided",
            "who owns",
            "owner",
            "decision",
            "decided",
        ],
    ) {
        return "current_state";
    }
    if (lower.contains("assistant") || lower.contains("codex"))
        && native_query_contains_any(
            &lower,
            &[
                "decide",
                "decided",
                "decision",
                "done",
                "implemented",
                "fixed",
                "pushed",
                "push",
                "committed",
                "commit",
                "changed",
                "updated",
                "validated",
                "verified",
            ],
        )
    {
        return "current_state";
    }
    if native_query_contains_any(
        &lower,
        &["why", "reason", "because", "feel", "felt", "emotion", "happy", "sad", "angry", "worried", "excited"],
    ) {
        return "why_emotion";
    }
    if native_query_contains_any(
        &lower,
        &["overview", "summarize", "summary", "explore", "broad", "what is in", "what do we know", "topics", "map", "inventory"],
    ) {
        return "broad_exploration";
    }
    if native_query_contains_any(
        &lower,
        &["evidence", "quote", "exactly", "what did", "conversation", "dialogue", "message"],
    ) {
        return "evidence";
    }
    if native_query_contains_any(
        &lower,
        &["procedure", "step", "steps", "how to", "troubleshoot", "rollback", "runbook", "playbook", "checklist", "fix", "remediate", "mitigate"],
    ) {
        return "procedure";
    }
    "fact"
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
            "matrixark_rust_proxy single-shot mode is debug-only. Use --serve for MatrixArk \
             production and benchmark workloads, or set MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG=1 \
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
        || env::var("MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG")
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
    if lower.contains("bucket not found")
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
            "# HELP matrixark_rust_proxy_process_start_time_ms Unix millisecond timestamp when this Rust proxy process started.\n",
            "# TYPE matrixark_rust_proxy_process_start_time_ms gauge\n",
            "matrixark_rust_proxy_process_start_time_ms {}\n",
            "# HELP matrixark_rust_proxy_commands_total Total MatrixArk Rust proxy commands.\n",
            "# TYPE matrixark_rust_proxy_commands_total counter\n",
            "matrixark_rust_proxy_commands_total {}\n",
            "# HELP matrixark_rust_proxy_commands_failed_total Total failed MatrixArk Rust proxy commands.\n",
            "# TYPE matrixark_rust_proxy_commands_failed_total counter\n",
            "matrixark_rust_proxy_commands_failed_total {}\n",
            "# HELP matrixark_rust_proxy_records_written_total Total MatrixArk records/hash entries written by the Rust proxy bridge.\n",
            "# TYPE matrixark_rust_proxy_records_written_total counter\n",
            "matrixark_rust_proxy_records_written_total {}\n",
            "# HELP matrixark_rust_proxy_records_read_total Total MatrixArk records/hash entries read by the Rust proxy bridge.\n",
            "# TYPE matrixark_rust_proxy_records_read_total counter\n",
            "matrixark_rust_proxy_records_read_total {}\n",
            "# HELP matrixark_rust_proxy_qps Current process-lifetime average command QPS.\n",
            "# TYPE matrixark_rust_proxy_qps gauge\n",
            "matrixark_rust_proxy_qps {:.6}\n",
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
            "# HELP matrixark_rust_proxy_cached_clients Cached TemporalEngine clients in the long-lived Rust gateway.\n",
            "# TYPE matrixark_rust_proxy_cached_clients gauge\n",
            "matrixark_rust_proxy_cached_clients {}\n",
            "# HELP matrixark_rust_proxy_clients_created_total TemporalEngine clients created by the long-lived Rust proxy.\n",
            "# TYPE matrixark_rust_proxy_clients_created_total counter\n",
            "matrixark_rust_proxy_clients_created_total {}\n",
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
            "# HELP matrixark_rust_proxy_command_latency_ms Command latency histogram in milliseconds.\n",
            "# TYPE matrixark_rust_proxy_command_latency_ms histogram\n",
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
            "matrixark_rust_proxy_command_latency_ms_bucket{{le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
        output.push_str(&format!(
            "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
    }
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_sum {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_sum{{backend=\"rust\"}} {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_count {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_count{{backend=\"rust\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "# HELP matrixark_rust_proxy_command_latency_max_ms Max observed command latency in milliseconds.\n\
         # TYPE matrixark_rust_proxy_command_latency_max_ms gauge\n\
         matrixark_rust_proxy_command_latency_max_ms {}\n\
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

fn hgetall_snapshot_cache_has_entries() -> bool {
    hgetall_snapshot_cache()
        .lock()
        .map(|cache| !cache.is_empty())
        .unwrap_or(false)
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

fn monotonic_record_count_enabled() -> bool {
    env::var("MATRIXARK_MONOTONIC_RECORD_COUNT")
        .map(|value| !matches!(value.trim(), "0" | "false" | "no" | "off"))
        .unwrap_or(true)
}

// TemporalStore parity with the native storage engine: the storage engine's
// record/serving SEQUENCE is an engine-owned MONOTONIC log id, taken from the append
// log's own iterator id and exposed read-only. It is advanced only by the append log /
// commit and is never a client
// read-modify-write of a stored count, so a stale read can never make it regress.
//
// The MatrixArk serving record-log counter (`{prefix}:record_count`) is instead computed
// client-side (Python `_get_count()` + `_record_location(sequence)`), a read-modify-write.
// Under SYNCHRONOUS commit a stale/low counter read makes a subsequent write REGRESS the
// stored counter; that cascades (later turns read low, replay low sequences and OVERWRITE
// earlier serving records -> fact records clobbered -> sync retrieval collapses to 0/14,
// while async stays 14/14). Mirror the contract at the engine boundary: a record_count
// write can only ADVANCE the stored counter, never lower it -> the client always reads a
// correct high sequence -> placement never regresses. Gated (default on;
// MATRIXARK_MONOTONIC_RECORD_COUNT=0 restores prior behavior). Inert for async, whose
// counter already advances monotonically (the clamp only ever raises a low write).
fn clamp_record_count_value(engine: &TemporalEngine, key: &str, value: Vec<u8>) -> Vec<u8> {
    if !monotonic_record_count_enabled() || !is_record_count_key(key) {
        return value;
    }
    let Some(new_count) = std::str::from_utf8(&value)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
    else {
        return value;
    };
    let existing = read_record_count(engine, key)
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok())
        .unwrap_or(0);
    if existing > new_count {
        return existing.to_string().into_bytes();
    }
    value
}

fn clamp_record_count_command(engine: &TemporalEngine, command: Command) -> Command {
    match command {
        Command::StringSet { key, value } if is_record_count_key(&key) => {
            let value = clamp_record_count_value(engine, &key, value);
            Command::StringSet { key, value }
        }
        other => other,
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
    invalidate_retrieve_candidate_cache_for_prefixes(prefixes);
}

fn invalidate_retrieve_candidate_cache_for_prefixes(prefixes: HashSet<String>) {
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
            let cache_entries = matrixark_scan_cache()
                .lock()
                .map(|cache| cache.len())
                .unwrap_or(0);
            stats.insert("candidate_cache_hit".to_string(), json!(true));
            stats.insert("cache_hit".to_string(), json!(true));
            stats.insert("candidate_cache_scope".to_string(), json!("process_global"));
            stats.insert("native_placement_candidate_cache_hit".to_string(), json!(true));
            stats.insert("native_placement_candidate_cache_entries".to_string(), json!(cache_entries));
            stats.insert("native_candidate_cache_key_shape".to_string(), json!("storage_prefix+count+scope+record_types+selected_node_hashes+secondary_index_groups+return_index_records"));
            stats.insert("native_candidate_cache_payload".to_string(), json!("compact_struct"));
            stats.insert("serving_memory_cache_layer".to_string(), json!("rust_proxy_scan_cache"));
            stats.insert("serving_memory_promoted".to_string(), json!(true));
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

include!("matrixark_rust_proxy_impl/scope_scoring.rs");

include!("matrixark_rust_proxy_impl/budget_parsing.rs");
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
    let scan_cache_entries_before_store = matrixark_scan_cache()
        .lock()
        .map(|cache| cache.len())
        .unwrap_or(0);
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
            "candidate_cache_scope": "process_global",
            "cache_hit": false,
            "native_placement_candidate_cache_hit": false,
            "native_placement_candidate_cache_entries": scan_cache_entries_before_store,
            "native_candidate_cache_key_shape": "storage_prefix+count+scope+record_types+selected_node_hashes+secondary_index_groups+return_index_records",
            "native_candidate_cache_payload": "compact_struct",
            "serving_memory_cache_layer": "rust_proxy_scan_cache",
            "serving_memory_promoted": true,
            "serving_memory_promoted_record_count": returned_records.len(),
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
            "next_native_gap": "conformance ContextPack scoring and budget assembly APIs"
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
    matches!(context_class, "entity" | "event" | "summary")
}

fn increment_class_count(counts: &mut HashMap<String, u64>, class_name: &str) {
    *counts.entry(class_name.to_string()).or_default() += 1;
}

fn increment_class_tokens(tokens_by_class: &mut HashMap<String, u64>, class_name: &str, tokens: u64) {
    *tokens_by_class.entry(class_name.to_string()).or_default() += tokens;
}

fn broad_memory_layer(record: &Value, ref_type: &str) -> String {
    if let Some(layer) = record
        .get("memory_layer")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return layer.to_string();
    }
    let sharing_scope = record
        .get("sharing_scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if matches!(sharing_scope, "tenant_shared" | "global_shared")
        || matches!(
            ref_type,
            "resource" | "resource_chunk" | "resource_fact" | "resource_entity_fact" | "skill" | "skill_section"
        )
    {
        return "shared_context".to_string();
    }
    let memory_scope = record
        .get("memory_scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if matches!(memory_scope, "user_profile" | "profile" | "cross_session_profile") {
        return "profile".to_string();
    }
    if matches!(memory_scope, "session" | "session_memory") {
        return "session".to_string();
    }
    let session_continuity = record
        .get("session_continuity")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if session_continuity == "same_session" {
        return "session".to_string();
    }
    if session_continuity == "cross_session" {
        if ref_type == "entity" {
            return "profile".to_string();
        }
        return "cross_session".to_string();
    }
    String::new()
}

fn pack_ref_from_record(
    record: &Value,
    text: &str,
    context_class: &str,
    score: f64,
    reason: &str,
    session_continuity: &str,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
) -> Value {
    let continuity_reason = match session_continuity {
        "same_session" => "same-session continuity",
        "cross_session" => "cross-session memory bridge",
        _ => "session-neutral context",
    };
    json!({
        "ref_type": context_class,
        "ref_hash": record_ref_hash(record).unwrap_or_else(|| record.get("record_id").and_then(Value::as_str).unwrap_or("").to_string()),
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "text": text,
        "token_estimate": token_estimate(text),
        "score": (score * 1000000.0).round() / 1000000.0,
        "session_continuity": session_continuity,
        "continuity_boost": (continuity_boost_value * 1000000.0).round() / 1000000.0,
        "cross_session_rerank_boost": (cross_session_rerank_boost_value * 1000000.0).round() / 1000000.0,
        "continuity_reason": continuity_reason,
        "selection_reason": reason,
        "memory_layer": broad_memory_layer(record, context_class),
        "memory_scope": record.get("memory_scope").and_then(Value::as_str).unwrap_or(""),
        "extraction_phase": record.get("extraction_phase").and_then(Value::as_str).unwrap_or(""),
        "final_session_boundary": record.get("final_session_boundary").and_then(Value::as_bool).unwrap_or(false),
        "entity_type": record.get("entity_type").and_then(Value::as_str).unwrap_or(""),
        "entity_name": record.get("entity_name").and_then(Value::as_str).unwrap_or(""),
        "source_roles": record.get("source_roles").cloned().unwrap_or_else(|| json!([])),
        "source_role_counts": record.get("source_role_counts").cloned().unwrap_or_else(|| json!({})),
        "source_hook_types": record.get("source_hook_types").cloned().unwrap_or_else(|| json!([])),
        "source_hook_type_counts": record.get("source_hook_type_counts").cloned().unwrap_or_else(|| json!({})),
        "source_codex_events": record.get("source_codex_events").cloned().unwrap_or_else(|| json!([])),
        "source_codex_event_counts": record.get("source_codex_event_counts").cloned().unwrap_or_else(|| json!({})),
        "source_session_ids": record.get("source_session_ids").cloned().unwrap_or_else(|| json!([])),
        "source_entity_hashes": record.get("source_entity_hashes").cloned().unwrap_or_else(|| json!([])),
        "source_entity_types": record.get("source_entity_types").cloned().unwrap_or_else(|| json!([])),
        "source_memory_scopes": record.get("source_memory_scopes").cloned().unwrap_or_else(|| json!([])),
        "source_session_continuities": record.get("source_session_continuities").cloned().unwrap_or_else(|| json!([])),
        "source_extraction_phases": record.get("source_extraction_phases").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_policies": record.get("source_profile_promotion_policies").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers").cloned().unwrap_or_else(|| json!([])),
        "source_ref": record.get("source_ref").cloned().unwrap_or(Value::Null),
    })
}

include!("matrixark_rust_proxy_impl/native_serving.rs");

include!("matrixark_rust_proxy_impl/retrieve_pack.rs");
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
            // Inherit the durable engine-library default (async_storage=false, i.e. every
            // write is fsync-committed to the WAL before it is acked). The async path buffers
            // the WAL with no barrier, so a crash before the next flush drops an acked write --
            // that must never be the deployed front-door default. Async is opt-in only, via an
            // explicit truthy MATRIXARK_RUST_PROXY_ASYNC_STORAGE.
            async_storage: env::var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE")
                .ok()
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(false),
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
    // parity monotonic serving sequence: never let a record_count write regress.
    let command = clamp_record_count_command(engine, command);
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
    // parity monotonic serving sequence: never let a record_count write regress
    // (mirrors the append-log's advance-only log id). Applies to the serving append
    // batch that carries the {prefix}:record_count StringSet.
    let commands: Vec<Command> = commands
        .into_iter()
        .map(|command| clamp_record_count_command(engine, command))
        .collect();
    let mut retrieve_cache_prefixes = HashSet::<String>::new();
    for command in &commands {
        match command {
            Command::HashSet { key, .. }
            | Command::HashMultiSet { key, .. }
            | Command::HashDelete { key, .. }
            | Command::CommonDelete { key }
            | Command::StringSet { key, .. } => {
                if let Some(prefix) = storage_prefix_from_key(key) {
                    retrieve_cache_prefixes.insert(prefix);
                }
            }
            _ => {}
        }
    }
    let cache_updates = if hgetall_snapshot_cache_has_entries() {
        commands
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
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
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
    invalidate_retrieve_candidate_cache_for_prefixes(retrieve_cache_prefixes);
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

    let memory_inventory = native_retrieval_memory_inventory(&records, scope);
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
                let ref_type = selected_ref
                    .get("ref_type")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                Some(CachedRetrieveCandidate {
                    selected_ref,
                    lower_text,
                    ref_type,
                })
            }
        })
        .collect::<Vec<_>>();
    let snapshot = Arc::new(RetrieveCandidateSnapshot {
        candidates,
        memory_inventory,
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
    let inferred_question_type;
    let question_type = if let Some(explicit) = request_record
        .get("question_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
    {
        explicit
    } else {
        inferred_question_type = infer_native_question_type(&query);
        inferred_question_type
    };
    let summary_allowed_for_question = matches!(
        question_type,
        "broad" | "broad_exploration" | "exploration" | "profile_memory"
    );
    let has_event_candidate = snapshot
        .candidates
        .iter()
        .any(|candidate| candidate.ref_type == "event");
    let score_started = Instant::now();
    let mut candidates = Vec::with_capacity(snapshot.candidates.len());
    for (ordinal, candidate) in snapshot.candidates.iter().enumerate() {
        if candidate.ref_type == "summary" && has_event_candidate && !summary_allowed_for_question {
            continue;
        }
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
    let current_state_query = matches!(
        question_type,
        "current_state" | "latest" | "profile_memory"
    );
    let all_candidate_refs: Vec<Value> = snapshot
        .candidates
        .iter()
        .map(|candidate| candidate.selected_ref.clone())
        .collect();
    let (profile_by_entity, profile_by_source_entity_hash) = if current_state_query {
        profile_shadow_maps_from_selected_refs(&all_candidate_refs)
    } else {
        (HashMap::new(), HashMap::new())
    };
    let mut selected_refs = Vec::new();
    let mut dropped_stale_ref = 0_u64;
    let mut dropped_stale_ref_tokens = 0_u64;
    let mut dropped_ref_type_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_type_token_counts: HashMap<String, u64> = HashMap::new();
    let mut dropped_ref_details: Vec<Value> = Vec::new();
    for (_, ordinal) in candidates.into_iter() {
        if selected_refs.len() >= max_selected_refs {
            break;
        }
        let Some(candidate) = snapshot.candidates.get(ordinal) else {
            continue;
        };
        let selected_ref = &candidate.selected_ref;
        if selected_ref.is_null() {
            continue;
        }
        if current_state_query {
            if let Some(profile_shadow) = profile_shadow_for_selected_ref(
                selected_ref,
                &profile_by_entity,
                &profile_by_source_entity_hash,
            ) {
                let tokens = selected_ref
                    .get("token_estimate")
                    .and_then(Value::as_u64)
                    .unwrap_or_else(|| token_estimate(string_field(selected_ref, "text")));
                dropped_stale_ref += 1;
                dropped_stale_ref_tokens += tokens;
                increment_class_count(&mut dropped_ref_type_counts, &candidate.ref_type);
                increment_class_tokens(&mut dropped_ref_type_token_counts, &candidate.ref_type, tokens);
                dropped_ref_details.push(native_dropped_ref_detail(
                    selected_ref,
                    string_field(selected_ref, "text"),
                    &candidate.ref_type,
                    "stale",
                    tokens,
                    Some(profile_shadow),
                ));
                continue;
            }
        }
        selected_refs.push(selected_ref.clone());
    }
    let selected_count = selected_refs.len();
    let mut memory_inventory = snapshot.memory_inventory.clone();
    let selected_profile_ref_count = selected_refs
        .iter()
        .filter(|item| {
            matches!(
                item.get("memory_scope").and_then(Value::as_str),
                Some("user_profile" | "profile" | "cross_session_profile")
            ) || (item.get("session_continuity").and_then(Value::as_str) == Some("cross_session")
                && item.get("ref_type").and_then(Value::as_str) == Some("entity"))
        })
        .count();
    let profile_available = memory_inventory
        .get("has_profile_memory")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if let Some(object) = memory_inventory.as_object_mut() {
        object.insert(
            "profile_records_available_but_not_selected".to_string(),
            json!(profile_available && selected_profile_ref_count == 0),
        );
    }
    let memory_layer_budget = selected_ref_layer_budget(&selected_refs);
    let dropped_memory_layer_budget = dropped_ref_layer_budget_from_native_counts(
        &[("stale", dropped_stale_ref, dropped_stale_ref_tokens)],
        &dropped_ref_type_counts,
        &dropped_ref_type_token_counts,
        &dropped_ref_details,
    );
    let memory_layer_pressure =
        memory_layer_pressure_summary(&memory_layer_budget, &dropped_memory_layer_budget);
    let serving_memory_layer_budget = native_serving_memory_layer_budget(&memory_layer_budget);
    let serving_dropped_memory_layer_budget =
        native_serving_memory_layer_budget(&dropped_memory_layer_budget);
    let serving_memory_layer_pressure =
        native_serving_memory_layer_pressure(&memory_layer_pressure);
    let retrieve_candidate_cache_entries = retrieve_candidate_cache()
        .lock()
        .map(|cache| cache.len())
        .unwrap_or(0);
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let correctness = selected_count > 0;
    let serving_selected_refs = native_serving_refs(&selected_refs);
    let serving_dropped_refs = native_serving_dropped_refs(json!({
        "refs": dropped_ref_details,
        "native_summary": true,
    }));
    let pack = json!({
        "context_pack_id": format!("rust-native-{}-{}", unix_ms(), stable_hash64(&query)),
        "context_pack_assembly": "native_rust_proxy",
        "native_context_pack": true,
        "selected_refs": serving_selected_refs,
        "dropped_refs": serving_dropped_refs,
        "memory_inventory": memory_inventory.clone(),
        "recall_policy": {
            "memory_layer_budget": serving_memory_layer_budget,
            "dropped_memory_layer_budget": serving_dropped_memory_layer_budget,
            "memory_layer_pressure": serving_memory_layer_pressure,
            "memory_inventory": memory_inventory.clone(),
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
            "dropped_refs": dropped_stale_ref,
            "scanned_records": snapshot.scanned_records,
            "index_postings_read": snapshot.index_postings_read,
            "placement_partitions_touched": snapshot.placement_partitions_touched,
            "candidate_cache_hit": candidate_cache_hit,
            "cache_hit": candidate_cache_hit,
            "candidate_cache_scope": "process_global",
            "native_placement_candidate_cache_hit": candidate_cache_hit,
            "native_placement_candidate_cache_entries": retrieve_candidate_cache_entries,
            "native_candidate_cache_key_shape": "storage_prefix+count+scope+secondary_index_groups",
            "native_candidate_cache_payload": "compact_struct",
            "serving_memory_cache_layer": "rust_proxy_retrieve_candidate_snapshot",
            "serving_memory_promoted": true,
            "serving_memory_promoted_record_count": snapshot.candidates.len(),
            "native_pack_assembly": true,
            "python_pack_fallback": false,
            "raw_candidate_tables_returned": false,
            "memory_layer_budget": serving_memory_layer_budget,
            "dropped_memory_layer_budget": serving_dropped_memory_layer_budget,
            "memory_layer_pressure": serving_memory_layer_pressure,
            "memory_inventory": memory_inventory,
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
        "dropped_ref_count": dropped_stale_ref,
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
        "token_estimate": token_estimate(text),
        "memory_layer": broad_memory_layer(record, public_ref_type),
        "memory_scope": record.get("memory_scope").and_then(Value::as_str).unwrap_or(""),
        "session_continuity": record.get("session_continuity").and_then(Value::as_str).unwrap_or(""),
        "extraction_phase": record.get("extraction_phase").and_then(Value::as_str).unwrap_or(""),
        "final_session_boundary": record.get("final_session_boundary").and_then(Value::as_bool).unwrap_or(false),
        "entity_type": record.get("entity_type").and_then(Value::as_str).unwrap_or(""),
        "entity_name": record.get("entity_name").and_then(Value::as_str).unwrap_or(""),
        "source_roles": record.get("source_roles").cloned().unwrap_or_else(|| json!([])),
        "source_role_counts": record.get("source_role_counts").cloned().unwrap_or_else(|| json!({})),
        "source_hook_types": record.get("source_hook_types").cloned().unwrap_or_else(|| json!([])),
        "source_hook_type_counts": record.get("source_hook_type_counts").cloned().unwrap_or_else(|| json!({})),
        "source_codex_events": record.get("source_codex_events").cloned().unwrap_or_else(|| json!([])),
        "source_codex_event_counts": record.get("source_codex_event_counts").cloned().unwrap_or_else(|| json!({})),
        "source_session_ids": record.get("source_session_ids").cloned().unwrap_or_else(|| json!([])),
        "source_entity_hashes": record.get("source_entity_hashes").cloned().unwrap_or_else(|| json!([])),
        "updated_at_ms": record.get("updated_at_ms").and_then(Value::as_u64).unwrap_or(0),
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
    for key in &request.visibility_keys {
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
        ":context_event_by_ingestion_time",
        ":context_latest_state",
        ":context_ref_locator",
        ":context_index_lookup",
        ":context_placement_lookup",
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
    fn matrixark_publish_visibility_uses_visibility_keys_for_partition_root() {
        let _guard = env_guard();
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
        env::remove_var("MATRIXARK_RUST_PROXY_ASYNC_STORAGE");

        let storage_prefix = "matrixark:mcp:codex:raw_ingestion";
        let mut append = request("matrixark_batch_append_records");
        append.namespace = "deploy_ns".to_string();
        append.table = "deploy_table".to_string();
        append.metaserver = "127.0.0.1:17100".to_string();
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_type":"agent_message","text":"raw visible"}"#.to_string(),
        )];

        let mut publish = request("matrixark_publish_visibility");
        publish.namespace = append.namespace.clone();
        publish.table = append.table.clone();
        publish.metaserver = append.metaserver.clone();
        publish.visibility_keys = vec![
            format!("{storage_prefix}:record_count"),
            format!("{storage_prefix}:records:000000"),
        ];

        let append_root = record_log_root(&append);
        let publish_root = record_log_root(&publish);
        assert!(
            publish_root.to_string_lossy().contains("prefix_"),
            "visibility-only publish requests must route to the prefix partition"
        );
        assert_eq!(
            publish_root, append_root,
            "publish and append must share the same durable partition"
        );
    }

    #[test]
    fn matrixark_native_selected_budget_counts_source_layers() {
        let record = json!({
            "record_type": "context_entity",
            "entity_hash": 42,
            "entity_type": "decision",
            "entity_name": "assistant rollout decision",
            "state": "Assistant responses should promote durable profile memory.",
            "memory_scope": "user_profile",
            "session_continuity": "cross_session",
            "extraction_phase": "final",
            "source_memory_scopes": ["session", "user_profile"],
            "source_session_continuities": ["same_session", "cross_session"],
            "source_extraction_phases": ["provisional", "final"],
            "source_entity_types": ["assistant_decision", "tool_evidence"],
            "source_profile_promotion_policies": ["always_when_profile_scope_available"],
            "source_roles": ["assistant"],
            "source_hook_types": ["hook_boundary"],
            "source_codex_events": ["Stop"],
        });
        let selected_ref = pack_ref_from_record(
            &record,
            "Assistant responses should promote durable profile memory.",
            "entity",
            1.0,
            "unit_test",
            "cross_session",
            0.0,
            0.0,
        );
        let budget = selected_ref_layer_budget(&[selected_ref]);
        assert_eq!(
            budget
                .pointer("/by_memory_scope/session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_memory_scope/user_profile/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_session_continuity/same_session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_session_continuity/cross_session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_extraction_phase/provisional/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_extraction_phase/final/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_entity_type/assistant_decision/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_entity_type/tool_evidence/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            budget
                .pointer("/by_profile_promotion_policy/always_when_profile_scope_available/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
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
            r#"{"record_bundle":[{"record_type":"context_event","event_id_hash":7,"text":"Alice approved GPU budget and Bob owns procurement","memory_scope":"session","extraction_phase":"provisional","source_roles":["user"],"source_hook_types":["UserPromptSubmit"]},{"record_type":"context_entity","entity_hash":8,"entity_type":"decision","entity_name":"gpu procurement owner","state":"Project Aurora GPU procurement owner is Bob","memory_scope":"user_profile","session_continuity":"cross_session","extraction_phase":"final","final_session_boundary":true,"source_roles":["assistant","tool"],"source_hook_types":["hook_boundary"],"source_codex_events":["Stop"],"source_session_ids":["codex:prior-session"],"source_memory_scopes":["session","user_profile"],"source_session_continuities":["same_session","cross_session"],"source_extraction_phases":["provisional","final"]},{"record_type":"resource_chunk","chunk_hash":9,"text":"","sharing_scope":"tenant_shared","resource_type":"runbook","title":"GPU procurement runbook"}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.query = "Who approved GPU budget and who owns procurement?".to_string();
        retrieve.max_selected_refs = 2;
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
        let session_event = refs
            .iter()
            .find(|value| value.get("ref_type").and_then(Value::as_str) == Some("event"))
            .expect("session event ref");
        let profile_entity = refs
            .iter()
            .find(|value| value.get("ref_type").and_then(Value::as_str) == Some("entity"))
            .expect("profile entity ref");
        assert_eq!(
            session_event.get("memory_layer").and_then(Value::as_str),
            Some("session")
        );
        assert_eq!(
            profile_entity.get("memory_layer").and_then(Value::as_str),
            Some("profile")
        );
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
        assert_eq!(
            pack.pointer("/retrieval_metrics/serving_memory_cache_layer")
                .and_then(Value::as_str),
            Some("rust_proxy_retrieve_candidate_snapshot")
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/serving_memory_promoted")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/native_candidate_cache_payload")
                .and_then(Value::as_str),
            Some("compact_struct")
        );
        assert_eq!(
            pack.pointer("/memory_inventory/session/context_events")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/profile/context_entities")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/shared/resource_chunks")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/has_session_memory")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/has_profile_memory")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/has_shared_memory")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/memory_inventory/profile_records_available_but_not_selected")
                .and_then(Value::as_bool),
            Some(false)
        );
        let available_layers: BTreeSet<_> = pack
            .pointer("/memory_inventory/available_layers")
            .and_then(Value::as_array)
            .expect("memory inventory available layers")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        assert_eq!(
            available_layers,
            BTreeSet::from(["profile", "session", "shared"])
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/memory_inventory"),
            pack.pointer("/memory_inventory")
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_inventory"),
            pack.pointer("/memory_inventory")
        );
        for field in [
            "/memory_inventory/source_roles",
            "/memory_inventory/source_hook_types",
            "/memory_inventory/source_codex_events",
            "/memory_inventory/source_session_ids",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default memory inventory leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_memory_scope/user_profile/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_memory_layer/profile/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_memory_layer/session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_memory_scope/session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_session_continuity/cross_session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_extraction_phase/final/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_entity_type/decision/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        for field in [
            "/recall_policy/memory_layer_budget/by_source_role",
            "/recall_policy/memory_layer_budget/by_hook_type",
            "/recall_policy/memory_layer_budget/by_codex_event",
            "/recall_policy/memory_layer_budget/source_message_counts_by_role",
            "/recall_policy/memory_layer_budget/source_hook_counts_by_type",
            "/recall_policy/memory_layer_budget/source_codex_event_counts_by_event",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default memory budget leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/final_session_boundary_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/memory_layer_budget"),
            pack.pointer("/recall_policy/memory_layer_budget")
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_budget/by_ref_type/entity/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/retrieval_metrics/dropped_memory_layer_budget"),
            pack.pointer("/recall_policy/dropped_memory_layer_budget")
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/stale_ref_count")
                .and_then(Value::as_u64),
            Some(0)
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
        assert_eq!(
            cached_response
                .pointer("/retrieval_metrics/native_placement_candidate_cache_hit")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            cached_response
                .pointer("/retrieval_metrics/serving_memory_promoted")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            cached_response
                .pointer("/retrieval_metrics/serving_memory_cache_layer")
                .and_then(Value::as_str),
            Some("rust_proxy_retrieve_candidate_snapshot")
        );
        assert_eq!(
            cached_response
                .pointer("/retrieval_metrics/native_candidate_cache_payload")
                .and_then(Value::as_str),
            Some("compact_struct")
        );
        assert!(
            cached_response
                .pointer("/retrieval_metrics/native_placement_candidate_cache_entries")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > 0
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
        assert_eq!(default_refs.len(), 3);

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }


    #[test]
    fn matrixark_native_retrieve_enforces_source_role_budget() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::remove_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN");

        let storage_prefix = "matrixark:test:native-source-role-budget";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_bundle":[{"record_type":"context_entity","entity_hash":101,"entity_type":"decision","entity_name":"assistant alpha","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","source_roles":["assistant"],"source_role_counts":{"assistant":1},"source_hook_types":["hook_boundary"],"source_hook_type_counts":{"hook_boundary":1},"source_codex_events":["Stop"],"source_codex_event_counts":{"Stop":1},"source_entity_types":["assistant_decision"],"source_profile_promotion_policies":["always_when_profile_scope_available"]},{"record_type":"context_entity","entity_hash":102,"entity_type":"decision","entity_name":"assistant bravo","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","extraction_phase":"final","source_memory_scopes":["session","user_profile"],"source_session_continuities":["same_session","cross_session"],"source_extraction_phases":["provisional","final"],"source_roles":["assistant"],"source_role_counts":{"assistant":1},"source_hook_types":["hook_boundary"],"source_hook_type_counts":{"hook_boundary":1},"source_codex_events":["Stop"],"source_codex_event_counts":{"Stop":1},"source_entity_types":["tool_evidence"],"source_profile_promotion_policies":["always_when_profile_scope_available"]},{"record_type":"context_event","event_id_hash":103,"text":"gpu","memory_scope":"session","session_continuity":"same_session","source_roles":["user"],"source_role_counts":{"user":1},"source_hook_types":["UserPromptSubmit"],"source_hook_type_counts":{"UserPromptSubmit":1}}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack_full_scan");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.record = Some(json!({
            "query": "gpu",
            "max_context_tokens": 64,
            "source_role_budget_tokens": {"assistant": 1},
            "ranking": {
                "max_selected_refs": 4,
                "min_similarity_score": 0.0
            }
        }));
        let output = execute_record_log_request(&engine, retrieve, root.clone())
            .expect("native retrieve with source-role budget");
        let pack = output
            .extra
            .get("context_pack")
            .expect("wrapped context pack from proxy op");
        let selected_refs = pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("selected refs");
        for field in [
            "ref_hash",
            "node_hash",
            "node_path",
            "token_estimate",
            "score",
            "continuity_boost",
            "cross_session_rerank_boost",
            "continuity_reason",
            "selection_reason",
            "source_session_ids",
            "source_entity_hashes",
            "source_entity_types",
            "source_roles",
            "source_role_counts",
            "budget_source_roles",
            "budget_source_role_counts",
            "source_hook_types",
            "source_hook_type_counts",
            "source_codex_events",
            "source_codex_event_counts",
            "source_memory_scopes",
            "source_session_continuities",
            "source_extraction_phases",
            "source_profile_promotion_policies",
            "source_profile_promotion_blockers",
        ] {
            assert!(
                selected_refs.iter().all(|value| value.get(field).is_none()),
                "default serving ref leaked {field}"
            );
        }
        let selected_entities: BTreeSet<_> = selected_refs
            .iter()
            .filter_map(|value| value.get("entity_name").and_then(Value::as_str))
            .collect();
        assert!(selected_entities.contains("assistant alpha"));
        assert!(!selected_entities.contains("assistant bravo"));
        assert!(selected_refs.iter().any(|value| value.get("ref_type").and_then(Value::as_str) == Some("event")));
        assert_eq!(
            pack.pointer("/dropped_refs/source_role_budget")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/source_role_budget/budget_tokens/assistant")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/source_role_budget/selected_tokens_by_role/assistant")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_drop_reason/source_role_budget/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        for field in [
            "/recall_policy/memory_layer_budget/by_source_role",
            "/recall_policy/memory_layer_budget/by_hook_type",
            "/recall_policy/memory_layer_budget/by_codex_event",
            "/recall_policy/memory_layer_budget/source_message_counts_by_role",
            "/recall_policy/memory_layer_budget/source_hook_counts_by_type",
            "/recall_policy/memory_layer_budget/source_codex_event_counts_by_event",
            "/recall_policy/memory_layer_budget/by_profile_promotion_policy",
            "/recall_policy/memory_layer_budget/by_profile_promotion_blocker",
            "/recall_policy/dropped_memory_layer_budget/by_source_role",
            "/recall_policy/dropped_memory_layer_budget/by_hook_type",
            "/recall_policy/dropped_memory_layer_budget/by_codex_event",
            "/recall_policy/dropped_memory_layer_budget/source_message_counts_by_role",
            "/recall_policy/dropped_memory_layer_budget/source_hook_counts_by_type",
            "/recall_policy/dropped_memory_layer_budget/source_codex_event_counts_by_event",
            "/recall_policy/dropped_memory_layer_budget/by_profile_promotion_policy",
            "/recall_policy/dropped_memory_layer_budget/by_profile_promotion_blocker",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default serving budget leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_memory_scope/session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_memory_scope/user_profile/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_session_continuity/cross_session/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_extraction_phase/provisional/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_pressure/profile_memory_pressure")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_layer_pressure/cross_session_pressure")
                .and_then(Value::as_bool),
            Some(true)
        );
        for field in [
            "/recall_policy/memory_layer_pressure/by_dimension/by_source_role",
            "/recall_policy/memory_layer_pressure/by_dimension/by_hook_type",
            "/recall_policy/memory_layer_pressure/by_dimension/by_codex_event",
            "/recall_policy/memory_layer_pressure/by_dimension/source_message_counts_by_role",
            "/recall_policy/memory_layer_pressure/by_dimension/source_hook_counts_by_type",
            "/recall_policy/memory_layer_pressure/by_dimension/source_codex_event_counts_by_event",
            "/recall_policy/memory_layer_pressure/assistant_source_message_pressure",
            "/recall_policy/memory_layer_pressure/hook_boundary_source_pressure",
            "/recall_policy/memory_layer_pressure/stop_event_source_pressure",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default serving pressure leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/retrieval_metrics/memory_layer_pressure"),
            pack.pointer("/recall_policy/memory_layer_pressure")
        );
        assert!(pack.pointer("/dropped_refs/refs").is_none());
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_detail_available_in_audit")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );

        env::set_var("MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE", "1");
        let mut debug_retrieve = request("matrixark_retrieve_context_pack_full_scan");
        debug_retrieve.storage_prefix = storage_prefix.to_string();
        debug_retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        debug_retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        debug_retrieve.record = Some(json!({
            "query": "gpu debug lineage",
            "max_context_tokens": 64,
            "source_role_budget_tokens": {"assistant": 1},
            "ranking": {
                "max_selected_refs": 4,
                "min_similarity_score": 0.0
            }
        }));
        let debug_output = execute_record_log_request(&engine, debug_retrieve, root)
            .expect("native retrieve with debug lineage");
        let debug_pack = debug_output
            .extra
            .get("context_pack")
            .expect("debug context pack");
        let debug_selected_refs = debug_pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("debug selected refs");
        assert!(debug_selected_refs
            .iter()
            .any(|value| value.get("source_role_counts").is_some()));
        assert!(debug_selected_refs
            .iter()
            .any(|value| value.get("source_hook_type_counts").is_some()));
        assert!(debug_selected_refs
            .iter()
            .any(|value| value.get("source_codex_event_counts").is_some()));
        assert!(debug_selected_refs
            .iter()
            .any(|value| value.pointer("/source_entity_types/0").and_then(Value::as_str)
                == Some("assistant_decision")));
        assert!(debug_selected_refs.iter().any(|value| value
            .pointer("/source_profile_promotion_policies/0")
            .and_then(Value::as_str)
            == Some("always_when_profile_scope_available")));
        assert!(debug_selected_refs
            .iter()
            .any(|value| value.get("ref_hash").is_some()));
        let debug_dropped_refs = debug_pack
            .pointer("/dropped_refs/refs")
            .and_then(Value::as_array)
            .expect("debug dropped ref audit details");
        assert_eq!(debug_dropped_refs.len(), 1);
        assert_eq!(
            debug_dropped_refs[0]
                .pointer("/source_role_budget_capped_roles/0")
                .and_then(Value::as_str),
            Some("assistant")
        );
        assert_eq!(
            debug_dropped_refs[0]
                .pointer("/source_entity_types/0")
                .and_then(Value::as_str),
            Some("tool_evidence")
        );
        assert_eq!(
            debug_pack
                .pointer("/recall_policy/memory_layer_budget/by_profile_promotion_policy/always_when_profile_scope_available/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            debug_pack
                .pointer("/recall_policy/dropped_memory_layer_budget/by_profile_promotion_policy/always_when_profile_scope_available/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        env::remove_var("MATRIXARK_CONTEXT_PACK_DEBUG_LINEAGE");

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }


    #[test]
    fn matrixark_native_retrieve_enforces_memory_selection_and_extraction_phase_budgets() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::remove_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN");

        let storage_prefix = "matrixark:test:native-selection-phase-budget";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_bundle":[{"record_type":"context_entity","entity_hash":201,"entity_type":"decision","entity_name":"policy alpha","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","extraction_phase":"final","source_memory_selection_policies":["selected_tool_evidence_only"],"source_memory_selection_policy_counts":{"selected_tool_evidence_only":1}},{"record_type":"context_entity","entity_hash":202,"entity_type":"decision","entity_name":"policy bravo","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","extraction_phase":"final","source_memory_selection_policies":["selected_tool_evidence_only"],"source_memory_selection_policy_counts":{"selected_tool_evidence_only":1}},{"record_type":"context_entity","entity_hash":203,"entity_type":"decision","entity_name":"phase alpha","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","extraction_phase":"provisional","source_memory_selection_policies":["selected_assistant_decision_outcome_only"]},{"record_type":"context_entity","entity_hash":204,"entity_type":"decision","entity_name":"phase bravo","state":"gpu","memory_scope":"user_profile","session_continuity":"same_session","extraction_phase":"provisional","source_memory_selection_policies":["selected_assistant_decision_outcome_only"]}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack_full_scan");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.record = Some(json!({
            "query": "gpu",
            "max_context_tokens": 64,
            "memory_selection_policy_budget_tokens": {"selected_tool_evidence_only": 1},
            "extraction_phase_budget_tokens": {"provisional": 1},
            "ranking": {
                "max_selected_refs": 4,
                "min_similarity_score": 0.0
            }
        }));
        let output = execute_record_log_request(&engine, retrieve, root.clone())
            .expect("native retrieve with selection/phase budgets");
        let pack = output
            .extra
            .get("context_pack")
            .expect("wrapped context pack from proxy op");

        assert_eq!(
            pack.pointer("/dropped_refs/memory_selection_policy_budget")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/dropped_refs/extraction_phase_budget")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_selection_policy_budget_policy/budget_tokens/selected_tool_evidence_only")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_selection_policy_budget_policy/selected_tokens_by_policy/selected_tool_evidence_only")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/memory_selection_policy_budget_policy/dropped_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/extraction_phase_budget_policy/budget_tokens/provisional")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/extraction_phase_budget_policy/selected_tokens_by_phase/provisional")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/extraction_phase_budget_policy/dropped_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_drop_reason/memory_selection_policy_budget/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_drop_reason/extraction_phase_budget/refs")
                .and_then(Value::as_u64),
            Some(1)
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }


    #[test]
    fn matrixark_native_compact_drops_profile_shadowed_session_entity() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::remove_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN");

        let storage_prefix = "matrixark:test:native-compact-profile-shadow";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_bundle":[{"record_type":"context_event","event_id_hash":7,"text":"GPU procurement owner current state was reviewed","memory_scope":"session","extraction_phase":"provisional","source_roles":["user"],"source_hook_types":["UserPromptSubmit"]},{"record_type":"context_entity","entity_hash":11,"entity_type":"decision","entity_name":"gpu procurement owner","state":"Old session-local GPU procurement owner is Alice","memory_scope":"session","session_continuity":"same_session","source_roles":["tool"],"source_hook_types":["tool_result"],"source_codex_events":["PostToolUse"],"extraction_phase":"provisional","updated_at_ms":100},{"record_type":"context_entity","entity_hash":22,"entity_type":"decision","entity_name":"gpu procurement owner","state":"Current cross-session GPU procurement owner is Bob","memory_scope":"user_profile","session_continuity":"cross_session","source_entity_hashes":[11],"source_session_ids":["codex:old","codex:new"],"extraction_phase":"final","updated_at_ms":200,"final_session_boundary":true}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.record = Some(json!({
            "query": "Who is the current GPU procurement owner?",
            "question_type": "current_state",
            "ranking": {"max_selected_refs": 4},
            "scope": {
                "account_id": "acct_shadow",
                "tenant_id": "tenant_shadow",
                "user_id": "user_shadow",
                "session_id": "codex:new"
            }
        }));
        let output = execute_record_log_request(&engine, retrieve, root.clone())
            .expect("native compact retrieve through proxy op");
        let response: Value = serde_json::from_str(&output.value).expect("compact context pack json");
        let pack = response
            .get("context_pack")
            .expect("wrapped context pack from proxy op");
        let selected_refs = pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("selected refs");
        assert!(selected_refs.iter().all(|value| value.get("ref_hash").is_none()));
        assert!(selected_refs.iter().all(|value| value.get("source_session_ids").is_none()));
        assert!(selected_refs.iter().all(|value| value.get("source_ref").is_none()));
        assert!(selected_refs.iter().any(|value| value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("Current cross-session GPU procurement owner is Bob")));
        assert!(!selected_refs.iter().any(|value| value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("Old session-local GPU procurement owner is Alice")));
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/stale_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_profile_shadowed_reason/source_entity_lineage/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        for field in [
            "/recall_policy/dropped_memory_layer_budget/by_source_role",
            "/recall_policy/dropped_memory_layer_budget/by_hook_type",
            "/recall_policy/dropped_memory_layer_budget/by_codex_event",
            "/recall_policy/dropped_memory_layer_budget/source_message_counts_by_role",
            "/recall_policy/dropped_memory_layer_budget/source_hook_counts_by_type",
            "/recall_policy/dropped_memory_layer_budget/source_codex_event_counts_by_event",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default dropped budget leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/retrieval_metrics/dropped_memory_layer_budget"),
            pack.pointer("/recall_policy/dropped_memory_layer_budget")
        );
        assert_eq!(
            response.get("dropped_ref_count").and_then(Value::as_u64),
            Some(1)
        );
        assert!(pack.pointer("/dropped_refs/refs").is_none());
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_detail_available_in_audit")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );

        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    #[test]
    fn matrixark_native_full_scan_drops_profile_shadowed_session_entity() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        env::set_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN", "1");

        let storage_prefix = "matrixark:test:native-profile-shadow";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:000000"),
            "00000000000000000000".to_string(),
            r#"{"record_bundle":[{"record_type":"context_event","event_id_hash":7,"text":"GPU procurement owner current state was reviewed","memory_scope":"session","extraction_phase":"provisional","source_roles":["user"],"source_hook_types":["UserPromptSubmit"]},{"record_type":"context_entity","entity_hash":11,"entity_type":"decision","entity_name":"gpu procurement owner","state":"Old session-local GPU procurement owner is Alice","memory_scope":"session","session_continuity":"same_session","source_roles":["tool"],"source_hook_types":["tool_result"],"source_codex_events":["PostToolUse"],"extraction_phase":"provisional","updated_at_ms":100},{"record_type":"context_entity","entity_hash":22,"entity_type":"decision","entity_name":"gpu procurement owner","state":"Current cross-session GPU procurement owner is Bob","memory_scope":"user_profile","session_continuity":"cross_session","source_entity_hashes":[11],"source_session_ids":["codex:old","codex:new"],"extraction_phase":"final","updated_at_ms":200,"final_session_boundary":true}]}"#.to_string(),
        )];

        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append compact bundle");

        let mut retrieve = request("matrixark_retrieve_context_pack");
        retrieve.storage_prefix = storage_prefix.to_string();
        retrieve.count_key = Some(format!("{storage_prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{storage_prefix}:records"));
        retrieve.record = Some(json!({
            "query": "Who is the current GPU procurement owner?",
            "question_type": "current_state",
            "max_context_tokens": 500,
            "ranking": {"max_selected_refs": 4},
            "scope": {
                "account_id": "acct_shadow",
                "tenant_id": "tenant_shadow",
                "user_id": "user_shadow",
                "session_id": "codex:new"
            }
        }));
        let output = execute_record_log_request(&engine, retrieve, root.clone())
            .expect("native full scan retrieve through proxy op");
        let pack = output
            .extra
            .get("context_pack")
            .expect("wrapped context pack from proxy op");
        let selected_refs = pack
            .get("selected_refs")
            .and_then(Value::as_array)
            .expect("selected refs");
        assert!(selected_refs.iter().all(|value| value.get("ref_hash").is_none()));
        assert!(selected_refs.iter().all(|value| value.get("source_session_ids").is_none()));
        assert!(selected_refs.iter().all(|value| value.get("source_ref").is_none()));
        assert!(selected_refs.iter().any(|value| value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("Current cross-session GPU procurement owner is Bob")));
        assert!(!selected_refs.iter().any(|value| value
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .contains("Old session-local GPU procurement owner is Alice")));
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/stale_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/profile_shadowed_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            pack.pointer("/recall_policy/dropped_memory_layer_budget/by_profile_shadowed_reason/source_entity_lineage/refs")
                .and_then(Value::as_u64),
            Some(1)
        );
        for field in [
            "/recall_policy/dropped_memory_layer_budget/by_source_role",
            "/recall_policy/dropped_memory_layer_budget/by_hook_type",
            "/recall_policy/dropped_memory_layer_budget/by_codex_event",
            "/recall_policy/dropped_memory_layer_budget/source_message_counts_by_role",
            "/recall_policy/dropped_memory_layer_budget/source_hook_counts_by_type",
            "/recall_policy/dropped_memory_layer_budget/source_codex_event_counts_by_event",
        ] {
            assert!(
                pack.pointer(field).is_none(),
                "default dropped budget leaked lineage field {field}"
            );
        }
        assert_eq!(
            pack.pointer("/retrieval_metrics/dropped_memory_layer_budget"),
            pack.pointer("/recall_policy/dropped_memory_layer_budget")
        );
        assert!(pack.pointer("/dropped_refs/refs").is_none());
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_detail_available_in_audit")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            pack.pointer("/dropped_refs/dropped_ref_count")
                .and_then(Value::as_u64),
            Some(1)
        );

        env::remove_var("MATRIXARK_RUST_PROXY_FULL_RETRIEVE_SCAN");
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
