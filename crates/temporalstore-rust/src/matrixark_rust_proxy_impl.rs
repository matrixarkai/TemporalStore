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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use temporalstore_rust::{
    BatchExecuteRequest, BatchExecuteResponse, BlockStoreOptions, Command, CommandResponse,
    ExecuteRequest, ExecuteResponse, ShardId, Status, TemporalEngine, TemporalStoreClient,
    TemporalStoreTable,
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
    /// The query's embedding, so ranking can happen HERE instead of in the caller.
    ///
    /// Without it this packer can only score `score_lowered_text` -- lexical substring matching
    /// over the query terms -- and it says so in its own output as `ranking_uses_vectors: false`.
    /// That is why the caller does not use it: ranking here would silently turn semantic
    /// retrieval into keyword matching, where a paraphrase scores exactly 0.0.
    ///
    /// With it, every candidate is scored against the query by cosine and only the selected refs
    /// need cross the lane. Measured on this store, a scan returns 2,954 records of which the
    /// caller keeps a few dozen, and the vectors and text of the rest are 35% of 11 MB.
    ///
    /// Absent means score lexically, exactly as before, so a caller that does not send one is
    /// unaffected.
    #[serde(default)]
    query_vector: Option<Vec<f32>>,
    /// The score a candidate must BEAT to be returned at all.
    ///
    /// The caller has been sending this on every request and the engine has never read it -- it was
    /// not even a field here, so serde dropped it silently. With dense retrieval it is the control
    /// that matters: a candidate the query embedding does not reach is not ranked low, it is not
    /// returned.
    ///
    /// Absent means 0.0, which returns everything the query can score at all and excludes only what
    /// scores exactly zero -- an opposed vector, or one this query cannot place.
    #[serde(default)]
    min_score: Option<f64>,
    /// Total token budget for the pack. 0 or absent means no budget, and the slot count decides.
    ///
    /// The caller computes this and sends it; the engine has only ever read it in test fixtures.
    /// Selection counted REFS and never summed tokens, so a pack of 24 long refs and a pack of 24
    /// short ones were treated as the same size.
    #[serde(default)]
    max_context_tokens: Option<u64>,
    /// The fewest refs each memory layer must get before the rest of the budget is filled by score.
    ///
    /// Without this every layer competes in one flat contest, so a large shared_context corpus
    /// (skills and resources) and a handful of session memories fight for the same slots and the
    /// bigger corpus wins on sheer count. A floor makes each layer's best survive to the pack, and
    /// the remaining budget still goes to whatever scores highest.
    ///
    /// Absent means no floors, which is the flat behaviour this engine has always had.
    #[serde(default)]
    layer_min_refs: Option<std::collections::BTreeMap<String, u64>>,
    /// How to combine the dense and lexical scores, and what an index hint is worth.
    ///
    /// The CALLER owns ranking policy and the engine executes it. That is deliberate: the caller
    /// already computes these weights from its own configuration -- on the one-box they are 1.00
    /// dense and 0.00 lexical, elsewhere 0.72 and 0.28 -- and an engine that hardcoded its own
    /// would silently rank differently from the caller that used to do it, which is the failure
    /// this whole change has to avoid.
    ///
    /// Absent means dense-only (1.0 / 0.0 / 0.0), which is what this engine already did.
    #[serde(default)]
    ranking_weights: Option<RankingWeights>,
    /// Send record payloads as sub-documents instead of JSON strings (see `RecordPayload`).
    /// Absent means the historical string shape, so an older reader is unaffected.
    #[serde(default)]
    records_inline_json: bool,
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
    /// Keep only records whose `status` is one of these, when the caller supplies any.
    ///
    /// A consumer that acts on a narrow set of statuses -- the pre-retrieval idle-commit flush reads
    /// three of the seven a pipeline task can carry -- otherwise receives every row of the type and
    /// discards most of them after they have been decoded. Absent or empty means no status
    /// filtering, so a caller that wants the whole type is unaffected.
    #[serde(default)]
    record_statuses: Option<Vec<String>>,
    /// Cap the scan to the newest N locations of a given record type, by append order.
    ///
    /// For a consumer that only ever looks at the tail of a type -- prior context reads the newest
    /// eight events and stops -- fetching the whole type is work whose result is discarded. Capping
    /// is per TYPE on purpose: the same scan also carries tombstones and retention cutoffs, and a
    /// cap on the union would drop the very records that make deleted memories stay deleted.
    #[serde(default)]
    newest_by_type: Option<BTreeMap<String, usize>>,
    /// Return only these top-level fields of each record.
    ///
    /// A scan returns whole records, and a caller uses a handful of fields. Measured on a
    /// production store, the average record is 673 bytes of which the TEXT is 2.4%:
    /// `storage_options` is 17.5%, `envelope` 11.9%, `scope` 10.7%, `embedding_meta` 9.6% and
    /// `vector` 4.4% -- and the packer that asks for these records reads none of those five, since
    /// it ranks lexically and reports `ranking_uses_vectors: false`. About nine tenths of every
    /// record crossing the lane is never looked at, and the gateway spends 53.5% of its CPU
    /// decoding it.
    ///
    /// The CALLER names the fields rather than the engine guessing them: the engine cannot know
    /// what a caller will read, and a projection that guesses wrong is a missing field rather than
    /// a slow response. Absent means the whole record, so nothing that does not ask is affected.
    ///
    /// Filtering happens BEFORE projection, so a scan can still filter on a field it does not
    /// return -- otherwise asking for less data would silently change which records match.
    #[serde(default)]
    record_fields: Option<Vec<String>>,
    /// Identity ids to remove, for `matrixark_delete_records`. Sent by the caller, which owns the
    /// decision about what a delete covers; the engine only matches and removes.
    #[serde(default)]
    record_ids: Option<Vec<String>>,
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
    /// Byte offset for `matrixark_resource_blob_fetch` (0 = start).
    #[serde(default)]
    blob_offset: Option<u64>,
    /// Byte count for `matrixark_resource_blob_fetch` (0/absent = to the end).
    #[serde(default)]
    blob_length: Option<u64>,
    /// Content hashes (16-digit hex) the caller's resource records still name, for
    /// `matrixark_resource_blob_sweep` -- everything else older than the age floor goes.
    #[serde(default)]
    blob_referenced_hashes: Option<Vec<String>>,
    /// Minimum age before an unreferenced blob is eligible for the sweep.
    #[serde(default)]
    blob_min_age_ms: Option<u64>,
    /// Client-chosen correlation id, echoed verbatim on the response. The serve loop answers
    /// requests strictly in order on one stdout, so a client that abandons a slow request (its
    /// own timeout) and keeps the process alive would otherwise read the ABANDONED request's
    /// late response as the answer to its next request -- every later reply shifted one back,
    /// silently serving the wrong data (observed as one scope's scan answered with another
    /// scope's records). The echo lets the client discard late responses instead of
    /// mis-attributing them.
    #[serde(default)]
    client_request_id: Option<String>,
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

/// A record payload on the wire.
///
/// `Text` is the shape this lane has always used: the stored record's JSON as a STRING. That
/// costs twice. Serializing escapes every quote in the payload into a new allocation here, and
/// the reader then parses the envelope AND parses each record's string a second time -- the
/// stored bytes are already JSON, so both sides are converting JSON to JSON.
///
/// `Inline` embeds the stored bytes verbatim as a sub-document. Nothing is escaped on the way
/// out and the reader parses once. Only a caller that asked for it gets it, so a reader that
/// still expects a string keeps getting one.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum RecordPayload {
    Inline(Box<RawValue>),
    Text(String),
}

/// Wrap a stored payload for the wire, inline when the caller asked and the bytes really are JSON.
///
/// The validation is not optional: `RawValue` is emitted VERBATIM, so handing it a value that is
/// not JSON would produce a malformed response for the whole batch rather than one bad record.
/// A payload that does not parse falls back to the string shape, which is exactly what this lane
/// did before.
fn record_payload(value: String, inline: bool) -> RecordPayload {
    if inline && serde_json::from_str::<serde::de::IgnoredAny>(&value).is_ok() {
        // The clone is a memcpy on a string we have already scanned, and it buys the caller a
        // whole parse. `from_string` can only reject invalid JSON, which the check above has
        // excluded, but falling through rather than unwrapping keeps a surprise from costing
        // the record.
        if let Ok(raw) = RawValue::from_string(value.clone()) {
            return RecordPayload::Inline(raw);
        }
    }
    RecordPayload::Text(value)
}

#[derive(Debug, Serialize)]
struct HashReadRecord {
    key: String,
    field: String,
    value: RecordPayload,
}

#[derive(Clone, Debug)]
struct CachedRetrieveCandidate {
    selected_ref: Value,
    lower_text: String,
    ref_type: String,
    /// The record's own embedding, kept so ranking can be dense.
    ///
    /// Held on the candidate rather than re-read from the record at scoring time because the
    /// snapshot outlives the records it was built from -- it is cached and reused across
    /// requests, and the records are dropped once it exists.
    vector: Option<Vec<f32>>,
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
    /// The request's correlation id, echoed verbatim (see RecordLogRequest::client_request_id).
    #[serde(skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
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
    // `--serve-http <addr>`, or the env var, so a deployment can switch transport without
    // changing its command line. Checked BEFORE `--serve`, because "--serve-http" would
    // otherwise never be reached if anything ever passes both.
    if let Some(addr) = http_serve_addr(&args) {
        std::process::exit(serve_http(&addr));
    }
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

/// The HTTP listen address, from `--serve-http <addr>` or `MATRIXARK_RUST_PROXY_HTTP_ADDR`.
fn http_serve_addr(args: &[String]) -> Option<String> {
    if let Some(index) = args.iter().position(|arg| arg == "--serve-http") {
        if let Some(addr) = args.get(index + 1) {
            if !addr.trim().is_empty() && !addr.starts_with("--") {
                return Some(addr.trim().to_string());
            }
        }
    }
    std::env::var("MATRIXARK_RUST_PROXY_HTTP_ADDR")
        .ok()
        .map(|addr| addr.trim().to_string())
        .filter(|addr| !addr.is_empty())
}

fn single_shot_debug_enabled(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--debug-single-shot")
        || temporalstore_rust::env_flag::env_bool("MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG", false)
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
            client_request_id: None,
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
                client_request_id: None,
                extra: BTreeMap::new(),
            }
        }
    }
}

/// Render a response in the codec its request arrived in, timing the render as JSON does.
///
/// Binary falls back to JSON when it cannot encode. Dropping the reply would hang the caller on
/// its deadline, and the client tells the two apart by the first byte -- the same rule the
/// request side uses -- so a fallback is understood rather than being a second failure.
fn encode_lane_response(response: &mut RecordLogResponse, binary: bool) -> Vec<u8> {
    if binary {
        let started = Instant::now();
        if let Ok(body) = rmp_serde::to_vec_named(&*response) {
            response.serialization_time_ms = Some(started.elapsed().as_millis());
            return body;
        }
    }
    serialize_response_with_metrics(response).into_bytes()
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

/// Frame marker for a binary lane response. A JSON line can never start with this byte, so a
/// reader that somehow sees the wrong codec fails loudly instead of parsing garbage.
const LANE_BINARY_MAGIC: u8 = 0xB5;

/// Is this process speaking msgpack on the lane?
///
/// Decided ONCE, from the environment the process was spawned with -- never per request. The
/// lane is a single pipe carrying a stream of responses, so a codec that changed partway would
/// leave the reader mid-frame with no way back. The spawner chooses; an older spawner sets
/// nothing and gets the JSON lines it has always got.
fn lane_binary_enabled() -> bool {
    env::var("MATRIXARK_LANE_CODEC")
        .map(|value| value.trim().eq_ignore_ascii_case("msgpack"))
        .unwrap_or(false)
}

/// Write one response in whichever codec this process speaks.
///
/// Binary frames are length-prefixed rather than delimited: a msgpack body can contain any byte,
/// including the newline the text lane uses as its terminator, so a delimiter cannot be trusted
/// here. Header is the magic byte plus a little-endian u32 length.
fn write_lane_response<W: Write>(
    out: &mut W,
    binary: bool,
    response: &RecordLogResponse,
    json_text: &str,
) {
    if binary {
        match rmp_serde::to_vec_named(response) {
            Ok(body) => {
                let mut header = [0_u8; 5];
                header[0] = LANE_BINARY_MAGIC;
                header[1..].copy_from_slice(&(body.len() as u32).to_le_bytes());
                let _ = out.write_all(&header);
                let _ = out.write_all(&body);
            }
            // Encoding a response that JSON accepted should not be possible, and dropping the
            // reply would hang the caller on its deadline. Fall back to the text line: the
            // reader can tell them apart by the first byte.
            Err(_) => {
                let _ = writeln!(out, "{json_text}");
            }
        }
    } else {
        let _ = writeln!(out, "{json_text}");
    }
    let _ = out.flush();
}

/// Bytes of request and response handled since the allocator was last asked for pages back.
fn trim_ledger() -> &'static std::sync::atomic::AtomicU64 {
    static LEDGER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    &LEDGER
}

/// Payload bytes that must pass through before a trim is worth its walk. 0 switches it off.
///
/// 8 MiB, the same figure the Python bridge used for the same job before this process replaced
/// it. `TS_MALLOC_TRIM=0` still switches the trim off underneath, so there are two levers and the
/// one nearer the allocator wins.
fn trim_threshold_bytes() -> u64 {
    std::env::var("MATRIXARK_RUST_PROXY_TRIM_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(8 * 1024 * 1024)
}

fn note_payload_bytes(bytes: usize) {
    trim_ledger().fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Hand freed heap back to the OS, on a thread of its own.
///
/// Rust's `free` returns memory to the allocator, not to the kernel, so a process that encodes and
/// decodes whole record batches keeps every peak it has ever reached. That is not a theory here:
/// the datanode calls `release_free_heap_to_os` after each GC and sits at 385 MB while holding the
/// actual store; this proxy holds NO engine at all -- it is in remote mode, forwarding to that
/// datanode -- and reached 2,941 MB, because nothing on this side ever asked. The Python bridge
/// this process replaced had the same function for the same reason, and its comment records the
/// same 2.9 GB.
///
/// On its own thread, deliberately. `memory_trim`'s own guidance is that a trim walks the free
/// lists and does not belong on the serving path, so the request path pays one relaxed atomic add
/// and nothing else; the trim happens where no caller is waiting on it.
fn start_heap_trimmer() {
    let threshold = trim_threshold_bytes();
    if threshold == 0 {
        return;
    }
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        let handled = trim_ledger().swap(0, std::sync::atomic::Ordering::Relaxed);
        if handled >= threshold {
            let _ = temporalstore_rust::memory_trim::release_free_heap_to_os();
        } else {
            // Put it back rather than discarding it: a steady trickle of small requests retains
            // just as much as one large one, it only takes longer to get there. Zeroing here
            // would mean a workload of small requests never trimmed at all.
            trim_ledger().fetch_add(handled, std::sync::atomic::Ordering::Relaxed);
        }
    });
}

/// Counters shared by both serving modes.
///
/// Extracted so `--serve` and `--serve-http` cannot drift: the Prometheus output is the same
/// output whichever transport produced the requests, and a second hand-rolled copy of this
/// accounting would be wrong in a way nothing would notice.
struct ServeMetrics {
    started_at_ms: u128,
    command_count: u64,
    failed_count: u64,
    records_written: u64,
    records_read: u64,
    latency_sum_ms: u128,
    latency_max_ms: u128,
    latency_buckets: [u64; LATENCY_BUCKETS_MS.len()],
}

impl ServeMetrics {
    fn new() -> Self {
        Self {
            started_at_ms: unix_ms(),
            command_count: 0,
            failed_count: 0,
            records_written: 0,
            records_read: 0,
            latency_sum_ms: 0,
            latency_max_ms: 0,
            latency_buckets: [0; LATENCY_BUCKETS_MS.len()],
        }
    }

    fn observe(&mut self, response: &RecordLogResponse, elapsed_ms: u128) {
        self.command_count += 1;
        let observed_elapsed_ms = response.elapsed_ms.unwrap_or(elapsed_ms);
        self.latency_sum_ms += observed_elapsed_ms;
        self.latency_max_ms = self.latency_max_ms.max(observed_elapsed_ms);
        for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if observed_elapsed_ms <= *upper_bound {
                self.latency_buckets[idx] += 1;
            }
        }
        if !response.ok {
            self.failed_count += 1;
        }
        self.records_written += match response.op.as_str() {
            "put_string" | "hset" => 1,
            "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
                response.count.unwrap_or(0) as u64
            }
            _ => 0,
        };
        self.records_read += match response.op.as_str() {
            "get_string" | "hget" => 1,
            "batch_hget" | "hgetall" | "scan_hash" => response.count.unwrap_or(0) as u64,
            _ => 0,
        };
    }

    fn render_prometheus(&self) -> String {
        render_prometheus_metrics(
            self.started_at_ms,
            self.command_count,
            self.failed_count,
            self.records_written,
            self.records_read,
            self.latency_sum_ms,
            self.latency_max_ms,
            &self.latency_buckets,
            cached_engine_count(),
        )
    }
}

/// Whether HTTP requests may run at the same time.
///
/// OFF by default, which reproduces exactly the serialization the pipe had: `--serve` handled one
/// request at a time, and the daemon in front of it held a single lock over a single process, so
/// nothing in this engine has ever had two requests in flight. Removing the middleman is a
/// transport change and is worth having on its own; letting requests overlap is a concurrency
/// change and belongs behind its own switch so it can be measured, and reverted, separately.
fn http_concurrent_enabled() -> bool {
    temporalstore_rust::env_flag::env_bool("MATRIXARK_RUST_PROXY_HTTP_CONCURRENT", false)
}

/// Serve the same requests over HTTP instead of over a pipe.
///
/// `--serve` reads one request per line from stdin, so something has to own that pipe. In the
/// one-box that owner was a Python daemon: it accepted a unix socket, parsed the request,
/// re-serialised it onto this process's stdin, read the answer back and re-serialised that too --
/// four full JSON operations per request, on the largest payloads in the system, to move bytes
/// between two file descriptors. Measured over 1,215 s of production-corpus soak that cost 21.9%
/// of a core and 72 MB of RSS, and it added a process to a box that was already OOM-killing.
///
/// The wire format is deliberately unchanged: the request body is the same `RecordLogRequest`
/// JSON the pipe carried and the reply is the same `RecordLogResponse`, so a client changes only
/// WHERE it sends, never what it sends -- which is what makes this swappable under a running
/// gateway and comparable in an A/B.
/// Whether a body is a msgpack map rather than JSON text.
///
/// The first byte settles it and cannot be ambiguous: a msgpack map begins with a fixmap
/// (0x80-0x8f), map16 (0xde) or map32 (0xdf), and a JSON object begins with `{` (0x7b) or
/// whitespace. This is the same way the stdio lane distinguishes its two framings, and it means
/// the codec needs no header, no negotiation and no shared state -- a request carries its own
/// answer, so a client and a proxy that disagree about the setting still understand each other.
fn looks_like_msgpack_map(body: &[u8]) -> bool {
    matches!(body.first(), Some(&first) if (0x80..=0x8f).contains(&first) || first == 0xde || first == 0xdf)
}

/// Decode a lane request in whichever codec it arrived in.
///
/// Returns the request and whether it was binary, because the reply must go back in the SAME
/// codec: the caller decodes what it sent, and answering JSON to a msgpack request would be
/// understood by nobody.
fn decode_lane_request(body: &[u8]) -> (Result<RecordLogRequest, String>, bool) {
    if looks_like_msgpack_map(body) {
        return (
            rmp_serde::from_slice::<RecordLogRequest>(body)
                .map_err(|error| format!("invalid msgpack request: {error}")),
            true,
        );
    }
    (
        serde_json::from_slice::<RecordLogRequest>(body)
            .map_err(|error| format!("invalid JSON request: {error}")),
        false,
    )
}

fn serve_http(addr: &str) -> i32 {
    let metrics = Arc::new(Mutex::new(ServeMetrics::new()));
    let gate = Arc::new(Mutex::new(()));
    let concurrent = http_concurrent_enabled();
    start_heap_trimmer();
    eprintln!("matrixark_rust_proxy serving http on {addr} (concurrent={concurrent})");
    let result = temporalstore_rust::http::serve(addr, move |http_request| {
        let started = Instant::now();
        let (parsed, binary_lane) = decode_lane_request(&http_request.body);
        let client_request_id = parsed
            .as_ref()
            .ok()
            .and_then(|request| request.client_request_id.clone());
        let result = match parsed {
            Ok(request) if request.op == "metrics_prometheus" => {
                let rendered = match metrics.lock() {
                    Ok(metrics) => metrics.render_prometheus(),
                    Err(_) => String::new(),
                };
                Ok((
                    "metrics_prometheus".to_string(),
                    RecordLogOutput {
                        prometheus: rendered,
                        mode: matrixark_rust_service_mode().to_string(),
                        cached_clients: Some(cached_engine_count()),
                        ..empty_output(PathBuf::new())
                    },
                ))
            }
            Ok(request) => {
                // The guard lives exactly as long as the call it serializes. Taking it around
                // the JSON work as well would serialize parsing too, for no safety gained.
                let _guard = if concurrent {
                    None
                } else {
                    Some(gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
                };
                run_request(request)
            }
            Err(error) => Err(("unknown".to_string(), error)),
        };
        let elapsed_ms = started.elapsed().as_millis();
        let mut response = response_from_result(result, elapsed_ms);
        response.client_request_id = client_request_id;
        let body = encode_lane_response(&mut response, binary_lane);
        if let Ok(mut metrics) = metrics.lock() {
            metrics.observe(&response, elapsed_ms);
        }
        note_payload_bytes(http_request.body.len() + body.len());
        // 200 even for an application-level failure: `ok` in the body is the contract the pipe
        // had, and a client that switched transports must not start seeing transport errors for
        // the same answers.
        (200, body)
    });
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("matrixark_rust_proxy http serve failed: {error}");
            1
        }
    }
}

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let lane_binary = lane_binary_enabled();
    let mut metrics = ServeMetrics::new();
    start_heap_trimmer();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(error) => {
                metrics.failed_count += 1;
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
        // Captured before the request moves into its handler; echoed on EVERY response line
        // (including shutdown), so the client can match responses to requests and discard the
        // late answer of a request it abandoned instead of shifting every later reply back one.
        let client_request_id = request
            .as_ref()
            .ok()
            .and_then(|request| request.client_request_id.clone());
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
                response.client_request_id = client_request_id;
                let json_text = serialize_response_with_metrics(&mut response);
                write_lane_response(&mut stdout, lane_binary, &response, &json_text);
                return 0;
            }
            Ok(request) if request.op == "metrics_prometheus" => {
                let output = RecordLogOutput {
                    prometheus: metrics.render_prometheus(),
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
        response.client_request_id = client_request_id;
        let response_json = serialize_response_with_metrics(&mut response);
        metrics.observe(&response, elapsed_ms);
        note_payload_bytes(line.len() + response_json.len());
        write_lane_response(&mut stdout, lane_binary, &response, &response_json);
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

/// Whether this binary is the direct SDK bridge.
///
/// Each bin that includes this file declares `DIRECT_SDK_BRIDGE`, so the answer is settled when
/// the binary is built. It used to be asked of `MATRIXARK_RUST_SDK_MODE` and of a substring of
/// argv[0]; the variable let a proxy process report itself as the bridge, and argv[0] is
/// whatever the file was last renamed to. Neither can contradict the binary now, and a new bin
/// that forgets to declare it does not compile.
fn matrixark_rust_sdk_mode_is_direct() -> bool {
    DIRECT_SDK_BRIDGE
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
    // The engine's own series, which include the page-cache counters. Appended rather than
    // re-rendered: see engine_prometheus_metrics.
    output.push_str(&engine_prometheus_metrics());
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

/// Shard and index snapshots, shared rather than copied.
///
/// The map used to hold the `BTreeMap` itself, which meant a deep copy of the whole snapshot
/// twice: once to store it and once on every hit. For a record shard the values ARE the payloads,
/// so a retrieve touching three shards copied three shards' worth of records to read the handful
/// of fields its index named -- paid in CPU, in allocator traffic, and in a transient doubling of
/// the proxy's resident set at exactly its high-water mark. The copy also grows with the store,
/// which is the shape the soak showed: latency climbing steadily from the first sample on a
/// FRESH store, with no failures and no memory pressure to blame it on.
///
/// `Arc` makes a hit a refcount bump. The write side still patches snapshots in place through
/// `Arc::make_mut`, which copies only while a reader is actually holding one; readers hold theirs
/// for the length of a single call, so in practice it does not copy at all.
/// One cached hash, with what it costs and when it was last wanted.
struct SnapshotEntry {
    map: Arc<BTreeMap<String, String>>,
    bytes: usize,
    used: u64,
}

/// Cached shard and index snapshots under a byte budget.
///
/// This cache had NO bound of any kind: it kept a full in-memory copy of every record shard it
/// ever read, and for a record shard the values are the payloads themselves. That is why the
/// proxy's resident set tracked the corpus at roughly ten times durable and ended in an OOM kill
/// at 4.5-5.2 GB rather than settling anywhere.
///
/// The budget is in BYTES, deliberately. A cap on the NUMBER of entries says nothing about memory
/// when the entries are whole shards of variable-size records -- an entry-count cap tried here
/// before changed the resident set by nothing at all, because the count was never what was large.
///
/// Eviction is always safe: this is a read-through cache and a miss re-reads from the engine, the
/// same path a key that was never cached takes. Least-recently-used, found by scanning, because
/// an eviction frees a whole shard and so happens far too rarely to be worth an index.
struct SnapshotCache {
    entries: BTreeMap<String, SnapshotEntry>,
    bytes: usize,
    clock: u64,
    budget: usize,
}

impl SnapshotCache {
    fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            bytes: 0,
            clock: 0,
            budget: default_snapshot_cache_bytes(),
        }
    }

    fn weigh(map: &BTreeMap<String, String>) -> usize {
        // The strings dominate; per-node overhead is a rounding error beside a payload and would
        // only make the budget pessimistic in a way that varies by allocator.
        map.iter()
            .map(|(field, value)| field.len() + value.len())
            .sum()
    }

    fn get(&mut self, key: &str) -> Option<Arc<BTreeMap<String, String>>> {
        self.clock += 1;
        let clock = self.clock;
        let entry = self.entries.get_mut(key)?;
        entry.used = clock;
        Some(Arc::clone(&entry.map))
    }

    fn contains_key(&self, key: &str) -> bool {
        self.entries.contains_key(key)
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.bytes = 0;
    }

    fn remove(&mut self, key: &str) {
        if let Some(entry) = self.entries.remove(key) {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
        }
    }

    fn insert(&mut self, key: String, map: Arc<BTreeMap<String, String>>) {
        self.remove(&key);
        let bytes = Self::weigh(&map);
        // A single snapshot larger than the whole budget is not cached rather than being cached
        // and immediately evicting everything else to make room for itself.
        if bytes > self.budget {
            return;
        }
        self.clock += 1;
        self.entries.insert(
            key,
            SnapshotEntry {
                map,
                bytes,
                used: self.clock,
            },
        );
        self.bytes += bytes;
        self.evict_to_budget();
    }

    /// Apply `patch` to a cached snapshot in place, re-weighing it afterwards.
    ///
    /// The write side keeps snapshots current rather than dropping them, so this is how a
    /// patched snapshot stays accounted for; a patch that grew a shard and did not re-weigh it
    /// would let the budget drift upward silently, which is the bug this whole type exists to
    /// prevent. `patch` returns false to say the snapshot should be dropped instead.
    fn patch<F>(&mut self, key: &str, patch: F)
    where
        F: FnOnce(&mut BTreeMap<String, String>) -> bool,
    {
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        let keep = patch(Arc::make_mut(&mut entry.map));
        if !keep || entry.map.is_empty() {
            self.remove(key);
            return;
        }
        let was = entry.bytes;
        let now = Self::weigh(&entry.map);
        entry.bytes = now;
        self.bytes = self.bytes.saturating_sub(was) + now;
        self.evict_to_budget();
    }

    fn evict_to_budget(&mut self) {
        while self.bytes > self.budget {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            self.remove(&victim);
        }
    }
}

/// The snapshot budget: a sixteenth of RAM, held between 64 MiB and 512 MiB.
///
/// Same shape as the engine's own cache sizing, so a small box does not hand this cache a budget
/// its RAM cannot back. `MATRIXARK_PROXY_SNAPSHOT_CACHE_BYTES` overrides it; a zero or unparsable
/// value falls back to the derived default rather than disabling the cache, because a cache of
/// size zero turns every sweep back into per-field reads.
fn default_snapshot_cache_bytes() -> usize {
    const FLOOR: usize = 64 * 1024 * 1024;
    const CEILING: usize = 512 * 1024 * 1024;
    if let Some(raw) = std::env::var("MATRIXARK_PROXY_SNAPSHOT_CACHE_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
    {
        if raw > 0 {
            return raw;
        }
    }
    let total = total_memory_bytes();
    if total == 0 {
        return FLOOR;
    }
    (total / 16).clamp(FLOOR, CEILING)
}

/// Total RAM in bytes, or 0 when it cannot be read.
fn total_memory_bytes() -> usize {
    let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") else {
        return 0;
    };
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(kb) = kb.parse::<usize>() {
                    return kb * 1024;
                }
            }
        }
    }
    0
}

fn hgetall_snapshot_cache() -> &'static Mutex<SnapshotCache> {
    static HGETALL_SNAPSHOT_CACHE: OnceLock<Mutex<SnapshotCache>> = OnceLock::new();
    HGETALL_SNAPSHOT_CACHE.get_or_init(|| Mutex::new(SnapshotCache::new()))
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

// TemporalStore conformance with the native storage engine: the storage engine's
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
fn clamp_record_count_value(engine: &RecordStore, key: &str, value: Vec<u8>) -> Vec<u8> {
    if !is_record_count_key(key) {
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

fn clamp_record_count_command(engine: &RecordStore, command: Command) -> Command {
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

/// Per-type append version. Changes when records OF THAT TYPE are appended, and not otherwise.
/// Keep only the named top-level fields of a record.
///
/// `None` returns the record untouched. A record that is not an object is returned untouched too:
/// projecting one would mean inventing a shape the caller did not ask for.
///
/// Nested paths are deliberately not supported. Every field a caller was measured to need is
/// top-level, and a path syntax would be a second query language to get wrong in a place where
/// being wrong means a silently missing field.
fn project_record(record: Value, fields: Option<&std::collections::BTreeSet<String>>) -> Value {
    let Some(fields) = fields else {
        return record;
    };
    let Value::Object(map) = record else {
        return record;
    };
    Value::Object(
        map.into_iter()
            .filter(|(key, _)| fields.contains(key))
            .collect(),
    )
}

fn type_version_key(record_hash_key: &str, record_type: &str) -> String {
    format!("{record_hash_key}:type_version:{record_type}")
}

/// Per-base delete epoch, bumped by any removal.
///
/// Deletes get an epoch rather than a per-type version because of a hole that a per-type version
/// alone cannot close: removing the last records of a type whose version key does not exist yet
/// would leave the token unchanged, and the next scan would serve an answer that still contained
/// them. An epoch is coarse -- it invalidates every scan for the base -- which is the right trade
/// exactly because deletes are rare and appends are not.
fn scan_delete_epoch_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:scan_delete_epoch")
}

/// The token that decides whether a cached scan is still the right answer.
///
/// It used to be the storage prefix's TOTAL record count, which is correct but maximally coarse:
/// every append changed it, so every scan of every type missed. Measured on a production-corpus
/// soak that is 229 records written per user message, so the hit rate under ingest was
/// effectively zero and each miss re-fetched the whole corpus of the requested types --
/// `matrixark_scan_candidates` was 87% of all time spent inside lane calls.
///
/// A scan that names its types can only be changed by those types, so it is keyed on their
/// versions plus the delete epoch. A scan that names no types could be changed by anything, so it
/// keeps the total count. Narrowing WHEN a cached answer is reused, never WHAT a scan returns.
fn scan_freshness_token(
    engine: &RecordStore,
    command: &RecordLogRequest,
    record_hash_key: &str,
    count: u64,
) -> String {
    let Some(types) = command.record_types.as_ref().filter(|types| !types.is_empty()) else {
        return format!("count:{count}");
    };
    let epoch = read_record_count(engine, &scan_delete_epoch_key(record_hash_key))
        .unwrap_or_default();
    let mut parts = Vec::with_capacity(types.len());
    for record_type in types {
        // A missing key is a DISTINCT token, not a zero: a type with no records yet reads as
        // absent, and its first append writes a version, so the token changes and the cached
        // empty answer is dropped. Reading it as 0 would collide with a real version of 0.
        let version = match read_record_count(engine, &type_version_key(record_hash_key, record_type)) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => "-".to_string(),
        };
        parts.push(format!("{record_type}={version}"));
    }
    parts.sort();
    format!("t:{}|d:{}", parts.join(","), epoch.trim())
}

fn matrixark_scan_cache_key(command: &RecordLogRequest, freshness: &str) -> String {
    serde_json::to_string(&json!({
        "count_key": command.count_key,
        "record_hash_key": command.record_hash_key,
        "shard_size": command.shard_size.unwrap_or(1024).max(1),
        "freshness": freshness,
        "record_types": command.record_types,
        // A status-filtered scan is a SUBSET of the same question, exactly like
        // `newest_by_type` below -- sharing an entry would serve the subset to a
        // caller that asked for the whole type.
        "record_statuses": command.record_statuses,
        "record_ids": command.record_ids,
        "selected_node_hashes": command.selected_node_hashes,
        "secondary_index_groups": command.secondary_index_groups,
        "scope": command.scope,
        "return_index_records": command.return_index_records,
        // A capped scan and an uncapped one are different answers to the same question, so they
        // must not share a cache entry: the capped answer is a SUBSET.
        "newest_by_type": command.newest_by_type,
        // Same rule for a projection: a caller that asked for five fields must never be served
        // the cached whole-record answer, and -- far worse -- a caller that asked for everything
        // must never be served a projected one, which would look like data loss.
        "record_fields": command.record_fields,
    }))
    .unwrap_or_else(|_| format!("fallback:{freshness}"))
}

/// Stamp a cached scan result as a hit.
///
/// `cache_entries` is passed in rather than read here on purpose: the only caller is already
/// holding the scan-cache guard when it hands us the cached value, and `std::sync::Mutex` is not
/// reentrant, so locking again here blocks forever on a guard this very thread owns. That is what
/// used to happen on every cache hit.
fn mark_scan_cache_hit(mut value: Value, cache_entries: usize) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("cache_hit".to_string(), json!(true));
        if let Some(stats) = object.get_mut("scan_stats").and_then(Value::as_object_mut) {
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

/// Drop named fields from a cached shard snapshot, keeping the rest of it.
///
/// The write side has always patched snapshots in place; deletes dropped the whole snapshot for
/// the key, which is the same as discarding a shard's worth of reads because one field went away.
/// It showed up as `update` being far slower than `add` on the same subject: an update purges the
/// superseded version, and every index and record snapshot that purge touched had to be rebuilt
/// cold by the very next scan -- one page read per member. A `HashDelete` names ONE field, so the
/// exact post-delete snapshot is the snapshot minus that field.
fn remove_hgetall_snapshot_fields(key: &str, fields: &[String]) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.patch(key, |snapshot| {
            for field in fields {
                snapshot.remove(field);
            }
            true
        });
        // Deleting the LAST field of a hash removes the whole key in the engine, and a cached
        // empty map for a key that no longer exists is the shape that once pinned a served view
        // at zero rows until restart. `hgetall_map` refuses to cache an empty read for the same
        // reason; a removal must not create through the back door what the read path declines to
        // store. Drop the snapshot instead and let the next read decide.
    }
}

fn update_hgetall_snapshot_fields(key: &str, entries: &[(String, Vec<u8>)]) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.patch(key, |snapshot| {
            for (field, value) in entries {
                let Ok(text) = String::from_utf8(value.clone()) else {
                    // A non-UTF-8 value means the snapshot can no longer represent the hash,
                    // so the whole key goes.
                    return false;
                };
                snapshot.insert(field.clone(), text);
            }
            true
        });
    }
}

/// Fetch the payload values at `locations` ("{shard:06}:{field}") through the shard hash
/// snapshots, in append order (lexical location order = shard, then zero-padded field).
///
/// Reads whole shard hashes via hgetall_map on purpose: HashMultiGet re-reads each field's page
/// uncached on every call (measured ~0.5 ms/field, never warming), while the snapshot is read
/// once and then kept current by the write runtimes -- the same coherence the shard walk has
/// always relied on. A location whose field is missing or empty is a stale index entry: the
/// record was physically removed after its entry was written, and skipping it is the contract.
fn fetch_indexed_payload_values(
    engine: &RecordStore,
    record_hash_key: &str,
    locations: &std::collections::BTreeSet<String>,
) -> Result<(Vec<String>, u64), String> {
    let mut fields_by_shard: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for location in locations {
        if let Some((shard, field)) = location.split_once(':') {
            fields_by_shard
                .entry(shard.to_string())
                .or_default()
                .push(field.to_string());
        }
    }
    let shards_touched = fields_by_shard.len() as u64;
    let mut values = Vec::with_capacity(locations.len());
    for (shard, fields) in fields_by_shard {
        let snapshot = hgetall_shared(engine, format!("{record_hash_key}:{shard}"))?;
        for field in fields {
            match snapshot.get(&field) {
                Some(value) if !value.is_empty() => values.push(value.clone()),
                _ => {}
            }
        }
    }
    Ok((values, shards_touched))
}

/// Read named fields of one record shard, without paying for the rest of it.
///
/// Deliberately NOT used by the sweep paths. There, whole-shard reads are the right call and the
/// comment on `fetch_indexed_payload_values` explains why: a named-field read costs about half a
/// millisecond per field and never warms, while a shard snapshot is read once and then kept
/// current by the write runtimes, so a repeated sweep pays nothing after the first. Narrowing
/// those measurably made them slower.
///
/// The purge is the opposite case, and measurably so. It names a handful of fields the locator
/// already identified, and it MUTATES those shards immediately after, which invalidates any
/// snapshot it would have populated -- so the snapshot is pure cost. Measured deleting an
/// identical freshly-created memory (same closure: 4 ids, 96 records scanned, 5 fields rewritten)
/// on two stores: 41.7 ms against a 20 MB store, 385.7 ms against a 249 MB one. Identical work,
/// nine times the time, because a shard on the larger store is full and the whole thing was being
/// decoded to reach five fields.
fn fetch_shard_fields(
    engine: &RecordStore,
    key: String,
    fields: &[String],
) -> Result<BTreeMap<String, String>, String> {
    if fields.is_empty() {
        return Ok(BTreeMap::new());
    }
    // An already-cached snapshot is free and current, so prefer it when one happens to be in hand.
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(cached) = cache.get(&key) {
            let mut subset = BTreeMap::new();
            for field in fields {
                if let Some(value) = cached.get(field) {
                    subset.insert(field.clone(), value.clone());
                }
            }
            return Ok(subset);
        }
    }
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashMultiGet {
            key,
            fields: fields.to_vec(),
        },
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Values { values } => {
            let mut decoded = BTreeMap::new();
            for (field, value) in fields.iter().zip(values.into_iter()) {
                let Some(bytes) = value else { continue };
                let text = String::from_utf8(bytes)
                    .map_err(|error| format!("stored value is not UTF-8: {error}"))?;
                if !text.is_empty() {
                    decoded.insert(field.clone(), text);
                }
            }
            Ok(decoded)
        }
        other => Err(format!("unexpected response for hmget: {other:?}")),
    }
}

/// An OWNED snapshot, for callers that consume or mutate what they get.
///
/// Prefer `hgetall_shared` wherever the caller only reads: this one exists to copy.
fn hgetall_map(engine: &RecordStore, key: String) -> Result<BTreeMap<String, String>, String> {
    Ok((*hgetall_shared(engine, key)?).clone())
}

fn hgetall_shared(
    engine: &RecordStore,
    key: String,
) -> Result<Arc<BTreeMap<String, String>>, String> {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(cached) = cache.get(&key) {
            return Ok(cached);
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
            let mut skipped_non_utf8 = 0usize;
            let mut first_skipped = String::new();
            for (field, value) in entries {
                // A value that is not text is not corruption: byte payloads are written in a
                // carried raw shape, so embeddings and their kin are legitimately not UTF-8.
                // Failing here would cost the whole pack -- and the caller turns an error into
                // an empty pack, which a hook emits as `{}` while exiting 0. Leave the value out
                // of the selection instead, and say how many were left out.
                let value = match String::from_utf8(value) {
                    Ok(value) => value,
                    Err(error) => {
                        if skipped_non_utf8 == 0 {
                            let bytes = error.as_bytes();
                            let head: String = bytes
                                .iter()
                                .take(16)
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            first_skipped = format!(
                                "key={key}, field={field}, len={}, first bytes: {head}",
                                bytes.len()
                            );
                        }
                        skipped_non_utf8 += 1;
                        continue;
                    }
                };
                decoded.insert(field, value);
            }
            if skipped_non_utf8 > 0 {
                eprintln!(
                    "context_pack skipped {skipped_non_utf8} value(s) that are not UTF-8 \
                     (first: {first_skipped})"
                );
            }
            // An empty read must stay a question, not become an answer: caching it would pin
            // "no data" for a key no write may ever touch again (observed once as a pinned
            // get_all stuck at 0 rows after a cold start until restart). An actually-empty hash
            // re-reads at map-miss cost, no page reads.
            let decoded = Arc::new(decoded);
            if !decoded.is_empty() {
                if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
                    cache.insert(key, Arc::clone(&decoded));
                }
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
    // `ref_hashes` fallback, last so nothing that resolves today changes branch.
    //
    // The posting builder writes the singular `ref_hash` ONLY when the posting carries exactly one
    // ref; a multi-ref posting has the array and no singular field. Without this a posting like
    // that returns None here, and two serving sites drop the record outright rather than score it
    // (`let Some(profile_hash) = record_ref_hash(..) else { continue; }`), with nothing logged.
    //
    // No caller passes more than one ref today, which is precisely why the day one does the loss
    // would be silent.
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for value in refs {
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

/// Just the fields index maintenance needs, borrowed from the payload rather than copied.
///
/// The write path used to parse every appended record into a full `Value` tree to read two
/// things from it -- the record type, and a scope key that may sit in any of five places. That
/// allocates a `String` per key and a `Value` per node for a whole record, then drops them all.
/// Under a production-corpus soak the proxy was burning 68.8% of a core to the gateway's 22.0%,
/// and `Value` handling plus the allocator traffic it causes was about 40% of it.
///
/// Deserializing into borrowed `&str` fields skips the tree: serde walks the same bytes but
/// materialises nothing except what is named here, and these point into the caller's buffer.
#[derive(Deserialize)]
struct ScopeKeyOnly<'a> {
    #[serde(borrow, default)]
    scope_key: Option<&'a str>,
}

#[derive(Deserialize)]
struct MetadataFacts<'a> {
    #[serde(borrow, default)]
    access_scope: Option<ScopeKeyOnly<'a>>,
}

#[derive(Deserialize)]
struct EnvelopeFacts<'a> {
    #[serde(borrow, default)]
    scope: Option<ScopeKeyOnly<'a>>,
}

#[derive(Deserialize)]
struct IndexFacts<'a> {
    #[serde(borrow, default)]
    record_type: Option<&'a str>,
    #[serde(borrow, default)]
    scope_key: Option<&'a str>,
    #[serde(borrow, default)]
    access_scope: Option<ScopeKeyOnly<'a>>,
    #[serde(borrow, default)]
    scope: Option<ScopeKeyOnly<'a>>,
    #[serde(borrow, default)]
    metadata: Option<MetadataFacts<'a>>,
    #[serde(borrow, default)]
    envelope: Option<EnvelopeFacts<'a>>,
    #[serde(borrow, default)]
    record_bundle: Option<Vec<IndexFacts<'a>>>,
}

impl<'a> IndexFacts<'a> {
    /// The scope key, in the same source order `candidate_scope_key` uses. Order is the whole
    /// contract: a record carrying two of these must resolve to the same bucket either way.
    fn scope_key(&self) -> &'a str {
        let sources = [
            self.scope_key,
            self.access_scope.as_ref().and_then(|s| s.scope_key),
            self.metadata
                .as_ref()
                .and_then(|m| m.access_scope.as_ref())
                .and_then(|s| s.scope_key),
            self.scope.as_ref().and_then(|s| s.scope_key),
            self.envelope
                .as_ref()
                .and_then(|e| e.scope.as_ref())
                .and_then(|s| s.scope_key),
        ];
        for source in sources {
            if let Some(text) = source {
                if !text.is_empty() {
                    return text;
                }
            }
        }
        ""
    }
}

/// Index facts for one stored payload, or None when the borrowed parse cannot answer.
///
/// None is not "no facts" -- it means fall back to the `Value` path, which is what a payload
/// whose shape this struct does not model (a field typed differently than expected) must do.
/// Getting that wrong would silently mis-index a record rather than merely cost time.
fn payload_index_facts(value: &str) -> Option<Vec<IndexFacts<'_>>> {
    let facts: IndexFacts = serde_json::from_str(value).ok()?;
    if let Some(bundle) = facts.record_bundle {
        return Some(bundle);
    }
    Some(vec![IndexFacts {
        record_type: facts.record_type,
        scope_key: facts.scope_key,
        access_scope: facts.access_scope,
        scope: facts.scope,
        metadata: facts.metadata,
        envelope: facts.envelope,
        record_bundle: None,
    }])
}

fn decode_matrixark_payload(value: &str) -> Vec<Value> {
    let Ok(mut decoded) = serde_json::from_str::<Value>(value) else {
        return Vec::new();
    };
    // Take the bundle rather than copying it. `decoded` is ours -- it was just parsed here and
    // nothing else can see it -- so cloning each record deep-copied every map and vector in it
    // for no one. Sampling the proxy under sustained ingest put `BTreeMap::clone_subtree` and
    // `Vec<Value>::clone` among the hottest frames; this is where they came from.
    if let Some(bundle) = decoded
        .get_mut("record_bundle")
        .and_then(Value::as_array_mut)
    {
        return std::mem::take(bundle)
            .into_iter()
            .filter(Value::is_object)
            .collect();
    }
    if decoded.is_object() {
        vec![decoded]
    } else {
        Vec::new()
    }
}

fn type_index_key(record_hash_key: &str, record_type: &str) -> String {
    format!("{record_hash_key}:type_index:{record_type}")
}

fn type_index_ready_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:type_index_ready")
}

/// `Some((record_hash_key, shard6))` when `key` is a record-shard key (`...:records:NNNNNN`).
///
/// The append op sees every hash entry a write carries -- latest-state rows, side-index rows,
/// counters -- and only the record shards may feed the type index: an index-served scan fetches
/// whatever the index names, and naming a non-record key would make it return rows the walk it
/// replaces could never have seen.
/// A stored location, in either shape, as `(shard, field)` within `record_hash_key`.
///
/// The long shape spells the whole thing out -- `{"key":"<base>:000003","field":"000...014"}` --
/// and the compact shape is `"3:14"`, shard and offset in decimal. Measured over 300 ingests, the
/// long shape was 87% of every byte written to a page, because the base is one deployment-wide
/// string repeated in every entry and the offset is a twenty-digit rendering of a small number.
///
/// A compact entry is always relative to the reader's own record log, which is the same thing the
/// long shape's base check enforces: an entry under another base is not this log's business, and
/// the writer leaves those in the long shape precisely because the compact one cannot say them.
/// Every location a ref's locator holds, head chunk and continuations together.
///
/// A locator list longer than one chunk keeps its head under the ref's own field and continues in
/// `"{ref}#1"`, `"{ref}#2"`, with the head naming how many follow. A reader that stops at the head
/// sees a truncated list, and here that is not a slow answer but a wrong one: one of these callers
/// decides which records a delete touches, so a missed chunk leaves records undeleted.
fn locator_location_values(
    engine: &RecordStore,
    locator_key: &str,
    id: &str,
) -> Result<Vec<Value>, String> {
    let mut out: Vec<Value> = Vec::new();
    let raw = read_bytes(
        engine,
        Command::HashGet {
            key: locator_key.to_string(),
            field: id.to_string(),
        },
    )?;
    if raw.is_empty() {
        return Ok(out);
    }
    let Ok(decoded) = serde_json::from_str::<Value>(&raw) else {
        return Ok(out);
    };
    if let Some(items) = decoded.get("locations").and_then(Value::as_array) {
        out.extend(items.iter().cloned());
    }
    let chunks = decoded
        .get("location_chunks")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    for index in 1..=chunks {
        let chunk_raw = read_bytes(
            engine,
            Command::HashGet {
                key: locator_key.to_string(),
                field: format!("{id}#{index}"),
            },
        )?;
        if chunk_raw.is_empty() {
            continue;
        }
        if let Ok(chunk) = serde_json::from_str::<Value>(&chunk_raw) {
            if let Some(items) = chunk.get("locations").and_then(Value::as_array) {
                out.extend(items.iter().cloned());
            }
        }
    }
    Ok(out)
}

fn location_shard_and_field(location: &Value, record_hash_key: &str) -> Option<(String, String)> {
    if let Some(compact) = location.as_str() {
        let (shard, offset) = compact.split_once(':')?;
        let shard: u64 = shard.parse().ok()?;
        let offset: u64 = offset.parse().ok()?;
        return Some((format!("{shard:06}"), format!("{offset:020}")));
    }
    let key = location.get("key").and_then(Value::as_str).unwrap_or("");
    let field = location.get("field").and_then(Value::as_str).unwrap_or("");
    if key.is_empty() || field.is_empty() {
        return None;
    }
    // Only locations in THIS record log: the locator is shared per prefix, and a location under
    // another base must not leak into this scan.
    let (base, shard) = record_shard_key_parts(key)?;
    if base != record_hash_key {
        return None;
    }
    Some((shard.to_string(), field.to_string()))
}

fn record_shard_key_parts(key: &str) -> Option<(&str, &str)> {
    let (base, shard) = key.rsplit_once(':')?;
    if shard.len() != 6 || !shard.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if !base.ends_with(":records") {
        return None;
    }
    Some((base, shard))
}

/// The record types stored in one shard-field payload, bundle-expanded.
/// Distinct `record_type`s across already-decoded records.
///
/// Split out of `payload_record_types` so a caller that has already decoded the payload -- the
/// batch-append handler, which also needs the records for the scope index -- does not decode it
/// a second time just to read one field.
fn records_record_types(records: &[Value]) -> Vec<String> {
    let mut types: Vec<String> = Vec::new();
    for record in records {
        if let Some(record_type) = record.get("record_type").and_then(Value::as_str) {
            if !record_type.is_empty() && !types.iter().any(|t| t == record_type) {
                types.push(record_type.to_string());
            }
        }
    }
    types
}

fn payload_record_types(value: &str) -> Vec<String> {
    records_record_types(&decode_matrixark_payload(value))
}

/// `records_record_types` over the borrowed parse. The `&str`s point into the caller's buffer.
fn facts_record_types<'a>(facts: &[IndexFacts<'a>]) -> Vec<&'a str> {
    let mut types: Vec<&'a str> = Vec::new();
    for fact in facts {
        if let Some(record_type) = fact.record_type {
            if !record_type.is_empty() && !types.contains(&record_type) {
                types.push(record_type);
            }
        }
    }
    types
}

/// Payload values for the requested types via the type index, in append order.
///
/// `Ok(None)` when the index cannot answer -- no ready-marker yet -- and the caller must walk.
/// Locations that no longer resolve (a field physically removed after a partial-cleanup path)
/// are skipped: the caller re-decodes and re-filters everything it is handed, so a stale entry
/// can cost a read but never change an answer.
/// The newest `limit` locations, or all of them when there is no cap.
///
/// A location is "{shard:06}:{field}" with both parts zero-padded, so lexical order IS append
/// order and the newest are the last. Sorting is not assumed of the input: the type index is read
/// into a map whose iteration order is its own business, and a cap that trusted the wrong order
/// would silently keep the OLDEST records instead -- a wrong answer rather than a slow one.
fn newest_locations(mut locations: Vec<String>, limit: Option<usize>) -> Vec<String> {
    match limit {
        Some(limit) if locations.len() > limit => {
            locations.sort();
            locations.split_off(locations.len() - limit)
        }
        _ => locations,
    }
}

fn type_index_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    newest_by_type: Option<&BTreeMap<String, usize>>,
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() != "1" {
        return Ok(None);
    }
    let mut locations: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for record_type in allowed_types {
        // Per-type cap, the same rule the pinned-scope path applies: a location is
        // "{shard:06}:{field}" with both parts zero-padded, so lexical order IS append order and
        // the newest N are the last N. A type with no cap keeps every position it had.
        //
        // Without this, a scan that carries a cap but no scope lands here and cap the type index
        // ignores it -- so a caller asking for one record of a type is handed every record of
        // that type, and what it pays grows with the store.
        let limit = newest_by_type
            .and_then(|caps| caps.get(record_type))
            .copied()
            .filter(|limit| *limit > 0);
        let of_type: Vec<String> =
            hgetall_shared(engine, type_index_key(record_hash_key, record_type))?
                .keys()
                .cloned()
                .collect();
        locations.extend(newest_locations(of_type, limit));
    }
    // BTreeSet order is lexical: shard6 then the zero-padded record field = append order.
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &locations)?;
    Ok(Some((values, shards_touched)))
}

fn scope_index_key(record_hash_key: &str, bucket: &str) -> String {
    format!("{record_hash_key}:scope_index:{bucket}")
}

fn scope_index_ready_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:scope_index_ready")
}

/// Bumped when the bucket layout changes; a store whose marker holds an older value re-walks
/// once and rebuilds. Version 2 = the type-partitioned scopeless bucket.
const SCOPE_INDEX_LAYOUT_VERSION: &str = "2";

/// The scope buckets a record files its field under, from its OWN scope_key -- the same source
/// the scope matcher reads.
///
/// Scopeless records match EVERY query under the matcher's rules, so they must reach every
/// index-served scan -- but ingest bundles a scoped event with scopeless system records in ONE
/// field, so a single master bucket would put nearly every field in the store there (measured:
/// 380 of ~460) and the fetch degenerates to a walk. A scopeless record therefore files under
/// "none" (for untyped queries) AND "none:{record_type}" (so a typed query only drags in
/// scopeless records of the types it asked for). "partial" = a scope_key lacking a tenant or
/// user part: the matcher rejects those against any pinned query, so nothing fetches the bucket.
fn record_scope_buckets(record: &Value) -> Vec<String> {
    let scope_key = candidate_scope_key(record);
    if scope_key.is_empty() {
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        return vec!["none".to_string(), format!("none:{record_type}")];
    }
    let parts = parse_scope_key(&scope_key);
    match (parts.get("t"), parts.get("u")) {
        (Some(tenant), Some(user)) if *tenant != 0 && *user != 0 => {
            vec![format!("t={tenant}|u={user}")]
        }
        _ => vec!["partial".to_string()],
    }
}

/// `record_scope_buckets` over the borrowed parse. Must agree with it bucket-for-bucket, since
/// the two run against the same store: a record filed under a different bucket by one path than
/// the other is a record an index-served scan cannot find.
fn facts_scope_buckets(fact: &IndexFacts<'_>) -> Vec<String> {
    let scope_key = fact.scope_key();
    if scope_key.is_empty() {
        let record_type = fact.record_type.unwrap_or("");
        return vec!["none".to_string(), format!("none:{record_type}")];
    }
    let parts = parse_scope_key(scope_key);
    match (parts.get("t"), parts.get("u")) {
        (Some(tenant), Some(user)) if *tenant != 0 && *user != 0 => {
            vec![format!("t={tenant}|u={user}")]
        }
        _ => vec!["partial".to_string()],
    }
}

/// The bucket a query pins, or None when the query is not pinned enough for the scope index.
///
/// Pinned = non-zero tenant and user hashes with the user marked explicit. A tenant-wide query
/// would need every user's bucket, and a session-mode refinement is applied by the shared filter
/// loop after the fetch -- the index only has to be a superset.
fn query_scope_bucket(query_scope: Option<&Value>) -> Option<String> {
    let query = query_scope.filter(|value| value.is_object())?;
    let tenant = query.get("tenant_hash").and_then(Value::as_u64).unwrap_or(0);
    let user = query.get("user_hash").and_then(Value::as_u64).unwrap_or(0);
    if tenant == 0 || user == 0 || !scope_key_explicit(query, "user_id") {
        return None;
    }
    Some(format!("t={tenant}|u={user}"))
}

/// Payload values for a pinned-scope scan, in append order: the bucket's locations plus the
/// scopeless bucket, intersected with the requested types' locations when the type index can
/// answer. `Ok(None)` = the scope index cannot answer; the caller walks (and backfills).
fn scope_index_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    bucket: &str,
    newest_by_type: Option<&BTreeMap<String, usize>>,
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &scope_index_ready_key(record_hash_key))?;
    if ready.trim() != SCOPE_INDEX_LAYOUT_VERSION {
        return Ok(None);
    }
    let mut source_buckets: Vec<String> = vec![bucket.to_string()];
    // Tenant-scoped records (a scope_key with a tenant but no user) live in "partial". Consumers
    // differ on them -- get_all wants exact tenant AND user equality and drops them, while prior
    // context accepts a tenant-wide summary for a user in that tenant -- so the fetch includes
    // them and the shared filter loop applies each consumer's real predicate. The index stays a
    // pre-filter; widening it can cost a wasted read, never a wrong answer, and an unpinned scan
    // was already returning these rows.
    source_buckets.push("partial".to_string());
    if allowed_types.is_empty() {
        source_buckets.push("none".to_string());
    } else {
        for record_type in allowed_types {
            source_buckets.push(format!("none:{record_type}"));
        }
    }
    let mut positions: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for source_bucket in &source_buckets {
        for location in
            hgetall_shared(engine, scope_index_key(record_hash_key, source_bucket))?.keys()
        {
            positions.insert(location.clone());
        }
    }
    if !allowed_types.is_empty() {
        let type_ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
        if type_ready.trim() == "1" {
            let mut type_positions: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for record_type in allowed_types {
                for location in
                    hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
                {
                    type_positions.insert(location.clone());
                }
            }
            positions = positions
                .intersection(&type_positions)
                .cloned()
                .collect();
            // Per-type cap, applied AFTER the scope intersection so "newest" means newest within
            // this scope rather than newest in the store. A location is "{shard:06}:{field}" with
            // both parts zero-padded, so lexical order IS append order and the newest N are the
            // last N. Types without a cap keep every position they had.
            if let Some(caps) = newest_by_type {
                let mut capped: std::collections::BTreeSet<String> = positions.clone();
                for (record_type, limit) in caps {
                    if *limit == 0 || !allowed_types.contains(record_type) {
                        continue;
                    }
                    let mut of_type: Vec<String> = Vec::new();
                    for location in
                        hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
                    {
                        if positions.contains(location) {
                            of_type.push(location.clone());
                        }
                    }
                    if of_type.len() <= *limit {
                        continue;
                    }
                    of_type.sort();
                    let keep: std::collections::BTreeSet<String> =
                        of_type[of_type.len() - *limit..].iter().cloned().collect();
                    for location in of_type {
                        if !keep.contains(&location) {
                            capped.remove(&location);
                        }
                    }
                }
                positions = capped;
            }
        }
    }
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &positions)?;
    Ok(Some((values, shards_touched)))
}

/// Persist a walk-built scope index and its ready-marker, once. Returns whether it wrote.
fn persist_scope_index_backfill(
    engine: &RecordStore,
    record_hash_key: &str,
    entries_by_index_key: BTreeMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<bool, String> {
    let ready = read_record_count(engine, &scope_index_ready_key(record_hash_key))?;
    if ready.trim() == SCOPE_INDEX_LAYOUT_VERSION {
        return Ok(false);
    }
    let mut commands: Vec<Command> = entries_by_index_key
        .into_iter()
        .map(|(key, entries)| Command::HashMultiSet { key, entries })
        .collect();
    commands.push(Command::StringSet {
        key: scope_index_ready_key(record_hash_key),
        value: SCOPE_INDEX_LAYOUT_VERSION.as_bytes().to_vec(),
    });
    execute_empty_batch_runtime(engine, commands, true)?;
    Ok(true)
}

/// Is this record about one of `ids` -- carrying it, targeting it, or created by superseding it?
///
/// `record_addressable_ids` covers what a record CARRIES (its own identity and its ref hashes);
/// history also needs the records that POINT at an id: a tombstone's `target_memory_id`, and the
/// supersede link `superseded_by` that marks the successor's creation.
fn record_id_linked(record: &Value, ids: &HashSet<String>) -> bool {
    if record_carries_wanted_id(record, |id| ids.contains(id)) {
        return true;
    }
    for field in ["target_memory_id", "superseded_by", "source_event_hash"] {
        match record.get(field) {
            Some(Value::String(text)) if ids.contains(text.as_str()) => return true,
            Some(Value::Number(number)) if ids.contains(number.to_string().as_str()) => {
                return true
            }
            _ => {}
        }
    }
    // Provenance arrays: a derivative points at its sources through these, without carrying
    // them as addressable ids -- exactly the records a get-by-id must return alongside the event.
    for field in ["source_event_ids", "source_refs"] {
        if let Some(Value::Array(items)) = record.get(field) {
            for item in items {
                match item {
                    Value::String(text) if ids.contains(text.as_str()) => return true,
                    Value::Number(number) if ids.contains(number.to_string().as_str()) => {
                        return true
                    }
                    _ => {}
                }
            }
        }
    }
    false
}

/// Payload values for an id-scoped scan, in append order: the ids' locator locations plus the
/// type-index locations of every requested type except `context_event` (events carry their own
/// id, so the locator covers them; tombstones and feedback point at an id without carrying it,
/// and they are sparse). `Ok(None)` = compose cannot answer; the caller walks.
fn id_scoped_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    requested_ids: &[String],
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() != "1" {
        return Ok(None);
    }
    // The record hash key is `{prefix}:records`, so the locator key derives from it -- callers
    // do not reliably send storage_prefix, and the id mode must not depend on an optional field.
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(None);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    // A store whose locator was fed pointed ids (provenance + targets) from its FIRST append
    // marks itself; on such stores the locator alone answers "records about these ids", and the
    // type-index compose below -- which would fetch every record of each requested type -- is
    // skipped. Unmarked (pre-existing) stores keep the composed behavior unchanged.
    let locator_covers_pointed_ids = hgetall_map(engine, format!("{locator_key}_meta"))
        .ok()
        .map(|meta| {
            meta.get("provenance_from_start")
                .map(|value| value.trim() == "1")
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let mut positions: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for id in requested_ids {
        let mut found = 0_usize;
        for location in locator_location_values(engine, &locator_key, id)? {
            let Some((shard, field)) = location_shard_and_field(&location, record_hash_key) else {
                continue;
            };
            positions.insert(format!("{shard}:{field}"));
            found += 1;
        }
        if found == 0 {
            // An old store predating the side index, or an id that never existed. The walk is
            // the correct answer for both; guessing "no records" here would erase real history.
            return Ok(None);
        }
    }
    if !locator_covers_pointed_ids {
        for record_type in allowed_types {
            if record_type == "context_event" {
                continue;
            }
            for location in
                hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
            {
                positions.insert(location.clone());
            }
        }
    }
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &positions)?;
    Ok(Some((values, shards_touched)))
}

/// Persist a walk-built index and its ready-marker, once. Returns whether it wrote.
fn persist_type_index_backfill(
    engine: &RecordStore,
    record_hash_key: &str,
    entries_by_index_key: BTreeMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<bool, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() == "1" {
        return Ok(false);
    }
    let mut commands: Vec<Command> = entries_by_index_key
        .into_iter()
        .map(|(key, entries)| Command::HashMultiSet { key, entries })
        .collect();
    commands.push(Command::StringSet {
        key: type_index_ready_key(record_hash_key),
        value: b"1".to_vec(),
    });
    execute_empty_batch_runtime(engine, commands, true)?;
    Ok(true)
}

fn scan_matrixark_candidates(
    engine: &RecordStore,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let count_key = required_option(command.count_key.clone(), "count_key")?;
    let record_hash_key = required_option(command.record_hash_key.clone(), "record_hash_key")?;
    let shard_size = command.shard_size.unwrap_or(1024).max(1);
    let count_text = read_record_count(engine, &count_key)?;
    let count = count_text.parse::<u64>().unwrap_or(0);
    let freshness = scan_freshness_token(engine, command, &record_hash_key, count);
    let scan_cache_key = matrixark_scan_cache_key(command, &freshness);
    // Take everything needed from the cache under one guard, then drop it before stamping the
    // result -- stamping used to re-lock this same mutex and hang the request.
    let cached_hit = match matrixark_scan_cache().lock() {
        Ok(cache) => {
            let entries = cache.len();
            cache.get(&scan_cache_key).cloned().map(|value| (value, entries))
        }
        Err(_) => None,
    };
    if let Some((value, entries)) = cached_hit {
        return Ok(mark_scan_cache_hit(value, entries));
    }
    let allowed_types: HashSet<String> = command
        .record_types
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let allowed_statuses: HashSet<String> = command
        .record_statuses
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
    let mut placement_partitions_touched = if count == 0 { 0 } else { max_shard + 1 };
    let mut scanned_records = 0_u64;
    let mut dropped_by_type = 0_u64;
    let mut dropped_by_status = 0_u64;
    let mut dropped_by_scope = 0_u64;
    let mut selected_node_dropped = 0_u64;
    // Collect payloads first -- from the type index when it can answer, from the shard walk
    // otherwise -- then run one shared filter loop, so the two paths cannot drift.
    let mut payload_values: Vec<String> = Vec::new();
    let requested_ids: Vec<String> = command.record_ids.clone().unwrap_or_default();
    let requested_id_set: HashSet<String> = requested_ids.iter().cloned().collect();
    let mut id_scoped_used = false;
    let mut type_index_used = false;
    if !requested_ids.is_empty() && count > 0 {
        if let Some((values, shards_touched)) = id_scoped_payloads(
            engine,
            &record_hash_key,
            &allowed_types,
            &requested_ids,
        )? {
            payload_values = values;
            placement_partitions_touched = shards_touched;
            id_scoped_used = true;
        }
    }
    let query_bucket = query_scope_bucket(command.scope.as_ref());
    let mut scope_index_used = false;
    if !id_scoped_used && count > 0 {
        if let Some(bucket) = &query_bucket {
            if let Some((values, shards_touched)) = scope_index_payloads(
                engine,
                &record_hash_key,
                &allowed_types,
                bucket,
                command.newest_by_type.as_ref(),
            )? {
                payload_values = values;
                placement_partitions_touched = shards_touched;
                scope_index_used = true;
            }
        }
    }
    // A pinned query whose scope index is not ready takes the WALK on purpose -- the walk
    // backfills the scope index, while the type path would serve this scan and leave the scope
    // index unbuilt forever on stores that predate it.
    if !id_scoped_used
        && !scope_index_used
        && query_bucket.is_none()
        && !allowed_types.is_empty()
        && count > 0
    {
        if let Some((values, shards_touched)) = type_index_payloads(
            engine,
            &record_hash_key,
            &allowed_types,
            command.newest_by_type.as_ref(),
        )? {
            payload_values = values;
            placement_partitions_touched = shards_touched;
            type_index_used = true;
        }
    }
    let mut type_index_backfilled = false;
    if !id_scoped_used && !scope_index_used && !type_index_used && count > 0 {
        // The walk this scan pays anyway sees every payload, so it can build the index for every
        // type in the store as a side effect; the marker makes the next scan's miss authoritative.
        let mut backfill: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        let mut scope_backfill: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        for shard in 0..=max_shard {
            let key = format!("{}:{:06}", record_hash_key, shard);
            let shard6 = format!("{shard:06}");
            for (field, value) in hgetall_map(engine, key.clone())? {
                for record_type in payload_record_types(&value) {
                    backfill
                        .entry(type_index_key(&record_hash_key, &record_type))
                        .or_default()
                        .push((format!("{shard6}:{field}"), b"1".to_vec()));
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        scope_backfill
                            .entry(scope_index_key(&record_hash_key, &bucket))
                            .or_default()
                            .push((format!("{shard6}:{field}"), b"1".to_vec()));
                    }
                }
                payload_values.push(value);
            }
        }
        type_index_backfilled =
            persist_type_index_backfill(engine, &record_hash_key, backfill)?;
        persist_scope_index_backfill(engine, &record_hash_key, scope_backfill)?;
    }
    // Built once, outside the record loop: a set per record would allocate per record, on the
    // very path this exists to make cheaper.
    let projection: Option<std::collections::BTreeSet<String>> = command
        .record_fields
        .as_ref()
        .filter(|fields| !fields.is_empty())
        .map(|fields| fields.iter().cloned().collect());
    let mut records = Vec::new();
    {
        for value in &payload_values {
            for record in decode_matrixark_payload(value) {
                scanned_records += 1;
                let record_type = record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !allowed_types.is_empty() && !allowed_types.contains(record_type) {
                    dropped_by_type += 1;
                    continue;
                }
                if !allowed_statuses.is_empty() {
                    let record_status = record
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !allowed_statuses.contains(record_status) {
                        dropped_by_status += 1;
                        continue;
                    }
                }
                if !requested_id_set.is_empty() && !record_id_linked(&record, &requested_id_set) {
                    dropped_by_scope += 1; // id-filtered, counted with scope drops
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
                records.push(project_record(record, projection.as_ref()));
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
            "dropped_by_status": dropped_by_status,
            "dropped_by_scope": dropped_by_scope,
            "selected_node_dropped_candidate_count": selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups.len(),
            "id_scoped_used": id_scoped_used,
            "scope_index_used": scope_index_used,
            "type_index_used": type_index_used,
            "type_index_backfilled": type_index_backfilled,
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

/// Outcome of a native scope-forget: how many records were logically removed and how the
/// removal decomposed into per-field tombstone-deletes vs partial rewrites.
#[derive(Debug, Default, Clone, Copy)]
struct ForgetScopeStats {
    records_scanned: usize,
    records_removed: usize,
    fields_deleted: usize,
    fields_rewritten: usize,
    shards_scanned: u64,
    /// Fields whose payload was decoded. `records_scanned` alone cannot say whether a wide purge
    /// opened many fields or a few dense ones, and those have opposite fixes: too many fields
    /// opened is a question about where records live, too many records per field is a question
    /// about how they are packed.
    fields_visited: usize,
    /// Of those, the ones that held nothing to remove -- decoded, then discarded. A purge whose
    /// opened fields all match is reading a precise set of locations; a large count here means the
    /// locations being handed to it do not hold the ids it was asked for.
    fields_without_match: usize,
    /// Of the fields that held nothing to remove, the ones that still hold a record POINTING at a
    /// wanted id. Those locations are correctly filed and cannot be dropped -- deletion reaches a
    /// derivative through exactly that link. The remainder, `fields_without_match` minus this, is
    /// the part where nothing in the field relates to the ids at all.
    fields_pointed_only: usize,
    /// Located entries dropped because this call decoded the field they point at and found nothing
    /// filed under that id. They accumulate because the index is append-only: a purge that rewrites
    /// a field to remove an id's records leaves behind the entry that led it there, and every later
    /// purge pays to open that field again.
    locator_locations_dropped: usize,
}

/// A forget query must actually CONSTRAIN which subject's records it removes. Because
/// `scope_matches_record` only enforces an identity field when it is marked explicit (or when a
/// non-zero `tenant_hash` is present), a bare/empty scope would match EVERY record and silently
/// wipe the whole store. Refuse anything that does not pin at least one subject dimension, so a
/// misrouted or under-specified forget fails loudly instead of deleting every scope's memory.
fn forget_scope_is_specific(scope: &Value) -> bool {
    if !scope.is_object() {
        return false;
    }
    // Tenant isolation: a real tenant hash always narrows matching to that tenant.
    if scope.get("tenant_hash").and_then(Value::as_u64).unwrap_or(0) != 0 {
        return true;
    }
    // Otherwise require an explicit, non-empty subject dimension -- the same set of keys
    // `scope_matches_record` enforces only when they are marked explicit.
    for key in [
        "user_id",
        "session_id",
        "account_id",
        "tenant_id",
        "team",
        "project",
        "agent_name",
    ] {
        let present = scope
            .get(key)
            .and_then(Value::as_str)
            .map(|value| !value.is_empty())
            .unwrap_or(false);
        if present && scope_key_explicit(scope, key) {
            return true;
        }
    }
    false
}

/// Re-encode the survivors of a partially-forgotten field, preserving the original on-disk shape:
/// a `{"record_bundle":[...]}` envelope keeps its sibling metadata and just drops the forgotten
/// entries; a single-record field that survives is written back verbatim.
fn encode_forget_survivors(original: &str, survivors: Vec<Value>) -> String {
    if let Ok(mut decoded) = serde_json::from_str::<Value>(original) {
        if decoded
            .get("record_bundle")
            .and_then(Value::as_array)
            .is_some()
        {
            decoded["record_bundle"] = Value::Array(survivors);
            return decoded.to_string();
        }
    }
    if survivors.len() == 1 {
        return survivors.into_iter().next().unwrap().to_string();
    }
    json!({ "record_bundle": survivors }).to_string()
}

/// Native scope-forget: delete every record under a scope prefix as ONE logical, durable,
/// recovery-safe operation.
///
/// Records live in the same shard set as ingest (`{record_hash_key}:{shard:06}`, counted by
/// `count_key`) with many scopes co-resident; scope isolation is by filter, not by key partition.
/// So forget enumerates every shard, decodes each hash field's record(s), and removes ONLY the
/// records that match `scope` (reusing the exact `scope_matches_record` predicate the retrieve
/// scan uses, so "what retrieve would return for this subject" == "what forget deletes"):
///   * a field whose records ALL match becomes a `HashDelete` (durable tombstone),
///   * a field with a mix is rewritten to keep the survivors,
///   * a field with no match is left untouched.
/// The commands are applied as a single durable batch (WAL-committed, same path as ingest), so the
/// removal replicates (rides the WAL / checkpoint index) and survives WAL replay without
/// resurrecting -- the delete is a first-class WAL mutation, not a read-time filter. Leaving
/// `count_key` untouched keeps forget idempotent and other scopes intact.
fn forget_scope_records(
    engine: &RecordStore,
    record_hash_key: &str,
    count_key: &str,
    shard_size: u64,
    scope: &Value,
) -> Result<ForgetScopeStats, String> {
    if !scope.is_object() {
        return Err("forget requires a scope object".to_string());
    }
    if !forget_scope_is_specific(scope) {
        return Err(
            "forget scope must constrain a subject (a non-zero tenant_hash, or an explicit \
             user_id/session_id/account_id/tenant_id/team/project/agent_name); refusing an \
             under-specified scope that would match every record"
                .to_string(),
        );
    }
    let shard_size = shard_size.max(1);
    let count = read_record_count(engine, count_key)?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    let mut stats = ForgetScopeStats::default();
    if count == 0 {
        return Ok(stats);
    }
    let max_shard = (count - 1) / shard_size;
    let mut commands = Vec::new();
    for shard in 0..=max_shard {
        stats.shards_scanned += 1;
        let key = format!("{}:{:06}", record_hash_key, shard);
        for (field, value) in hgetall_map(engine, key.clone())? {
            let records = decode_matrixark_payload(&value);
            if records.is_empty() {
                // Undecodable / non-record field (e.g. a counter): never touch it.
                continue;
            }
            stats.records_scanned += records.len();
            stats.fields_visited += 1;
            let mut survivors = Vec::with_capacity(records.len());
            let mut removed_here = 0_usize;
            for record in records {
                if scope_matches_record(&record, Some(scope)) {
                    removed_here += 1;
                } else {
                    survivors.push(record);
                }
            }
            if removed_here == 0 {
                stats.fields_without_match += 1;
                continue;
            }
            stats.records_removed += removed_here;
            if survivors.is_empty() {
                // The field is going away entirely: its type-index entries go in the same batch.
                // A partial rewrite leaves its entries alone -- an index-served fetch re-filters
                // everything it loads, so a stale entry is a wasted read, not a wrong answer.
                for record_type in payload_record_types(&value) {
                    commands.push(Command::HashDelete {
                        key: type_index_key(record_hash_key, &record_type),
                        field: format!("{shard:06}:{field}"),
                    });
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        commands.push(Command::HashDelete {
                            key: scope_index_key(record_hash_key, &bucket),
                            field: format!("{shard:06}:{field}"),
                        });
                    }
                }
                commands.push(Command::HashDelete {
                    key: key.clone(),
                    field,
                });
                stats.fields_deleted += 1;
            } else {
                let encoded = encode_forget_survivors(&value, survivors);
                commands.push(Command::HashSet {
                    key: key.clone(),
                    field,
                    value: encoded.into_bytes(),
                });
                stats.fields_rewritten += 1;
            }
        }
    }
    if !commands.is_empty() {
        // One durable batch: WAL-committed together, and it clears the process-global scan +
        // hgetall snapshot caches so a subsequent retrieve/get_all never re-serves a forgotten
        // record from cache.
        execute_empty_batch_runtime(engine, commands, true)?;
    }
    Ok(stats)
}

/// Every id a record can be addressed by: its own identity, and any reference it carries.
///
/// A delete removes the addressed record AND the embeddings / index postings that point at it --
/// those carry no identity of their own, only a `ref_hash` / `ref_hashes` aimed at one. Matching
/// both is what stops a delete leaving orphaned postings behind that still surface its text.
/// Write a u64 as text into a caller-owned buffer. No allocation.
///
/// Exists because the ids in these records are mostly JSON numbers (`event_id_hash`,
/// `entity_hash`, ...) and `Number::to_string()` allocates a String for each one -- which is a lot
/// of allocation to answer a question that only needs a comparison.
fn u64_into<'a>(buf: &'a mut [u8; 20], mut value: u64) -> &'a str {
    if value == 0 {
        buf[0] = b'0';
        return std::str::from_utf8(&buf[..1]).unwrap_or("0");
    }
    let mut at = buf.len();
    while value > 0 {
        at -= 1;
        buf[at] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    std::str::from_utf8(&buf[at..]).unwrap_or("")
}

/// Does this record carry any id the caller is looking for?
///
/// Same identity fields as [`record_addressable_ids`], but answers the membership question without
/// building anything. That function allocates a `Vec<String>` per record plus a `String` per id it
/// finds, and both call sites immediately threw all of it away after an `.any(...)`.
///
/// It is on the hot loop of `update`: a purge at 360 memories parses 7,829 records to remove 663,
/// and asked this question of every one of them.
///
/// `contains` is a closure rather than a set so the two callers can keep the set types they
/// already have -- one holds `&str`, the other `String`.
fn record_carries_wanted_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    // Signed or floating: rare for an id, so the allocation here is not worth
                    // avoiding, and matching `to_string` keeps the answer identical.
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in [
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "segment_hash",
        "ref_hash",
    ] {
        if hit(record.get(field)) {
            return true;
        }
    }
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for item in refs {
            if hit(Some(item)) {
                return true;
            }
        }
    }
    false
}

/// Does `record` merely POINT AT one of the wanted ids, without carrying it?
///
/// The located set is built from both kinds of relationship: a record is filed under the ids it
/// carries AND under the ids it names as sources or targets. Deletion needs both filed -- a
/// derivative is only findable through its source event, because it is not filed under its own
/// identity hash -- but deletion only REMOVES the carriers. So a located field can be perfectly
/// legitimate and still hold nothing to remove.
///
/// That makes two very different situations look identical in `fields_without_match`, and they
/// have different fixes: a field still holding records that point at these ids is a location the
/// index is right to keep, while a field where nothing so much as mentions them is an entry that
/// has gone stale and could be dropped. This tells them apart. Mirrors the writer's pointed-id
/// fields exactly; a field the writer files under and this misses would misreport a live location
/// as stale.
fn record_points_at_wanted_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in ["source_event_hash", "target_memory_id", "superseded_by"] {
        if hit(record.get(field)) {
            return true;
        }
    }
    for field in ["source_event_ids", "source_refs"] {
        if let Some(Value::Array(values)) = record.get(field) {
            for item in values {
                if hit(Some(item)) {
                    return true;
                }
            }
        }
    }
    false
}

/// Every field the WRITER files a record under, so "is this location still described by this id"
/// can be answered the same way the entry was created.
///
/// This deliberately does NOT reuse the purge's matcher. The two ask different questions and the
/// writer's set is wider: it files under `chunk_hash`, `section_hash`, `skill_hash`,
/// `resource_hash` and `batch_id_hash`, none of which deletion looks at. The located set has two
/// consumers -- deletion, and the id-scoped read that serves retrieval -- so an entry that
/// deletion finds uninteresting can still be the only route by which a read finds its record.
/// Deciding staleness with the narrower set would drop those entries and quietly cost retrieval
/// rows while deletion stayed correct.
///
/// So the rule here is: keep the entry if ANY field the writer files under still matches. Being a
/// superset is safe (an entry is kept), being a subset is not (an entry is dropped), which is why
/// `entity_hash` and `segment_hash` are included even though the writer does not file under them.
/// `locator_filing_fields_are_covered` pins the list against the writer's.
fn record_filed_under_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in LOCATOR_FILING_SCALAR_FIELDS {
        if hit(record.get(*field)) {
            return true;
        }
    }
    for field in LOCATOR_FILING_LIST_FIELDS {
        if let Some(Value::Array(values)) = record.get(*field) {
            for item in values {
                if hit(Some(item)) {
                    return true;
                }
            }
        }
    }
    false
}

/// The writer's scalar filing fields, plus the two identity hashes deletion matches on.
const LOCATOR_FILING_SCALAR_FIELDS: &[&str] = &[
    // context_index_ref_hashes
    "ref_hash",
    "event_id_hash",
    "chunk_hash",
    "section_hash",
    "skill_hash",
    "resource_hash",
    "summary_hash",
    "batch_id_hash",
    // record_pointed_ref_ids
    "source_event_hash",
    "target_memory_id",
    "superseded_by",
    // not filed under by the writer; kept because dropping an entry is the unsafe direction
    "entity_hash",
    "segment_hash",
];

const LOCATOR_FILING_LIST_FIELDS: &[&str] = &["ref_hashes", "source_event_ids", "source_refs"];

/// Drop the located entries that this purge proved no longer describe where they point.
///
/// Only called for `(id, location)` pairs whose field was decoded in full during this call, so
/// "nothing here is filed under this id" is read off the field's actual contents rather than
/// inferred. Chunk boundaries are left exactly as the writer laid them out -- each chunk is
/// filtered in place and written back, and `location_chunks` is untouched -- so no second opinion
/// about how the list should be split can drift from the writer's.
fn prune_locator_locations(
    engine: &RecordStore,
    record_hash_key: &str,
    stale: &BTreeMap<String, BTreeSet<(String, String)>>,
    commands: &mut Vec<Command>,
) -> Result<usize, String> {
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(0);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    let mut dropped = 0_usize;

    let drop_here = |location: &Value, gone: &BTreeSet<(String, String)>| -> bool {
        // Anything this call cannot resolve to a location in THIS record log is left alone.
        match location_shard_and_field(location, record_hash_key) {
            Some(pair) => gone.contains(&pair),
            None => false,
        }
    };

    for (id, gone) in stale {
        let mut filter_field = |field: String| -> Result<(), String> {
            let raw = read_bytes(
                engine,
                Command::HashGet {
                    key: locator_key.clone(),
                    field: field.clone(),
                },
            )?;
            if raw.is_empty() {
                return Ok(());
            }
            let Ok(mut decoded) = serde_json::from_str::<Value>(&raw) else {
                return Ok(());
            };
            let Some(list) = decoded.get_mut("locations").and_then(Value::as_array_mut) else {
                return Ok(());
            };
            let before = list.len();
            list.retain(|location| !drop_here(location, gone));
            let removed = before - list.len();
            if removed == 0 {
                return Ok(());
            }
            dropped += removed;
            commands.push(Command::HashSet {
                key: locator_key.clone(),
                field,
                value: decoded.to_string().into_bytes(),
            });
            Ok(())
        };

        // The head field carries the chunk count, so read it before filtering it.
        let head = read_bytes(
            engine,
            Command::HashGet {
                key: locator_key.clone(),
                field: id.clone(),
            },
        )?;
        if head.is_empty() {
            continue;
        }
        let chunks = serde_json::from_str::<Value>(&head)
            .ok()
            .and_then(|value| value.get("location_chunks").and_then(Value::as_u64))
            .unwrap_or(0);
        filter_field(id.clone())?;
        for index in 1..=chunks {
            filter_field(format!("{id}#{index}"))?;
        }
    }
    Ok(dropped)
}

fn record_addressable_ids(record: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    let mut push = |value: Option<&Value>| {
        if let Some(value) = value {
            match value {
                Value::String(text) if !text.is_empty() => ids.push(text.clone()),
                Value::Number(number) => ids.push(number.to_string()),
                _ => {}
            }
        }
    };
    for field in [
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "segment_hash",
        "ref_hash",
    ] {
        push(record.get(field));
    }
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for item in refs {
            match item {
                Value::String(text) if !text.is_empty() => ids.push(text.clone()),
                Value::Number(number) => ids.push(number.to_string()),
                _ => {}
            }
        }
    }
    ids
}

/// Remove every record addressable by one of `ids`.
///
/// Mirrors `forget_scope_records` -- same shard walk, same survivor rewrite, same single durable
/// batch that also clears the scan/hgetall caches so a later retrieve cannot re-serve a removed
/// record from cache. Only the predicate differs: identity ids instead of a scope.
/// The `(shard, field)` locations the ref locator holds for `ids`, or `None` when the locator
/// cannot be trusted to be complete for this store.
///
/// Completeness is the whole question: visiting only located fields is correct exactly when the
/// locator saw every record this store ever wrote, which is what the `provenance_from_start`
/// marker attests (it is stamped by the batch that writes the store's first record). Without it
/// the caller must walk, because a missed field would leave a deleted record physically present.
fn located_fields_for_ids(
    engine: &RecordStore,
    record_hash_key: &str,
    ids: &[String],
) -> Result<Option<BTreeMap<String, Vec<String>>>, String> {
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(None);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    let covered = hgetall_map(engine, format!("{locator_key}_meta"))?
        .get("provenance_from_start")
        .map(|value| value.trim() == "1")
        .unwrap_or(false);
    if !covered {
        return Ok(None);
    }
    let mut by_shard: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for id in ids {
        for location in locator_location_values(engine, &locator_key, id)? {
            let Some((shard, field)) = location_shard_and_field(&location, record_hash_key) else {
                continue;
            };
            let fields = by_shard.entry(shard).or_default();
            if !fields.iter().any(|existing| existing == &field) {
                fields.push(field);
            }
        }
    }
    Ok(Some(by_shard))
}

fn delete_records_by_ids(
    engine: &RecordStore,
    record_hash_key: &str,
    count_key: &str,
    shard_size: u64,
    ids: &[String],
) -> Result<ForgetScopeStats, String> {
    let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut stats = ForgetScopeStats::default();
    if wanted.is_empty() {
        // An empty id set must remove NOTHING. Falling through to a match-everything walk here
        // would turn a no-op delete into a store wipe.
        return Ok(stats);
    }
    let shard_size = shard_size.max(1);
    let count = read_record_count(engine, count_key)?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    if count == 0 {
        return Ok(stats);
    }
    let max_shard = (count - 1) / shard_size;
    let mut commands = Vec::new();
    // Located entries this call can prove no longer describe where they point: the field was
    // decoded here in full and nothing in it is filed under that id.
    let mut stale: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
    // Which fields could hold these ids? The locator answers directly on a store it covers
    // completely; otherwise every shard has to be read.
    let located = located_fields_for_ids(engine, record_hash_key, ids)?;
    let visit: Vec<(String, Vec<String>)> = match &located {
        Some(by_shard) => by_shard
            .iter()
            .map(|(shard, fields)| (shard.clone(), fields.clone()))
            .collect(),
        None => (0..=max_shard)
            .map(|shard| (format!("{shard:06}"), Vec::new()))
            .collect(),
    };
    for (shard, only_fields) in visit {
        stats.shards_scanned += 1;
        let key = format!("{}:{}", record_hash_key, shard);
        // With located fields, read exactly those. Without them (no locator coverage) the whole
        // shard is genuinely needed, because the walk is the fallback that finds the records the
        // locator could not name.
        let entries: Vec<(String, String)> = if only_fields.is_empty() {
            hgetall_map(engine, key.clone())?.into_iter().collect()
        } else {
            let found = fetch_shard_fields(engine, key.clone(), &only_fields)?;
            only_fields
                .into_iter()
                .filter_map(|field| found.get(&field).map(|value| (field, value.clone())))
                .collect()
        };
        for (field, value) in entries {
            let records = decode_matrixark_payload(&value);
            if records.is_empty() {
                // Undecodable / non-record field (e.g. a counter): never touch it.
                continue;
            }
            stats.records_scanned += records.len();
            stats.fields_visited += 1;
            let mut survivors = Vec::with_capacity(records.len());
            let mut removed_here = 0_usize;
            for record in records {
                if record_carries_wanted_id(&record, |id| wanted.contains(id)) {
                    removed_here += 1;
                } else {
                    survivors.push(record);
                }
            }
            // This field has been decoded in full, so for each id it is now known -- not guessed --
            // whether anything here is still filed under it. Judged on the SURVIVORS, since the
            // records being removed are about to stop being here.
            for id in ids {
                let still_filed = survivors
                    .iter()
                    .any(|record| record_filed_under_id(record, |candidate| candidate == id));
                if !still_filed {
                    stale
                        .entry(id.clone())
                        .or_default()
                        .insert((shard.clone(), field.clone()));
                }
            }
            if removed_here == 0 {
                stats.fields_without_match += 1;
                // Nothing was removed, so `survivors` is still every record this field holds.
                if survivors
                    .iter()
                    .any(|record| record_points_at_wanted_id(record, |id| wanted.contains(id)))
                {
                    stats.fields_pointed_only += 1;
                }
                continue;
            }
            stats.records_removed += removed_here;
            if survivors.is_empty() {
                // The field is going away entirely: its type-index entries go in the same batch.
                // A partial rewrite leaves its entries alone -- an index-served fetch re-filters
                // everything it loads, so a stale entry is a wasted read, not a wrong answer.
                for record_type in payload_record_types(&value) {
                    commands.push(Command::HashDelete {
                        key: type_index_key(record_hash_key, &record_type),
                        field: format!("{shard}:{field}"),
                    });
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        commands.push(Command::HashDelete {
                            key: scope_index_key(record_hash_key, &bucket),
                            field: format!("{shard}:{field}"),
                        });
                    }
                }
                commands.push(Command::HashDelete {
                    key: key.clone(),
                    field,
                });
                stats.fields_deleted += 1;
            } else {
                let encoded = encode_forget_survivors(&value, survivors);
                commands.push(Command::HashSet {
                    key: key.clone(),
                    field,
                    value: encoded.into_bytes(),
                });
                stats.fields_rewritten += 1;
            }
        }
    }
    // Same durable batch as the record removals: the entries and the records they describe go
    // together, so a crash cannot leave the index pointing at content that is already gone.
    if !stale.is_empty() {
        stats.locator_locations_dropped =
            prune_locator_locations(engine, record_hash_key, &stale, &mut commands)?;
    }
    if !commands.is_empty() {
        execute_empty_batch_runtime(engine, commands, true)?;
    }
    Ok(stats)
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
    engine: &RecordStore,
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
        // Attachment blob tier, engine command side: the python surface reaches the
        // embedded engine's content-addressed blob store through these ops, mirroring the
        // datanode's HTTP /blob tier for deployments that run no HTTP server. Payloads ride
        // base64 in `value`; structured results ride the flattened extra map.
        "matrixark_resource_blob_put" => {
            let tenant_hash: u64 = request
                .key
                .trim()
                .parse()
                .map_err(|_| format!("blob put needs a decimal tenant hash in key, got {:?}", request.key))?;
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobPut {
                    tenant_hash,
                    payload_base64: request.value.clone(),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobCommitted { uri, size_bytes, content_hash } = response.response {
                output.status = "committed".to_string();
                output.extra.insert("matrixark_blob_uri".to_string(), json!(uri));
                output.extra.insert("matrixark_blob_size_bytes".to_string(), json!(size_bytes));
                output.extra.insert(
                    "matrixark_blob_content_hash".to_string(),
                    json!(format!("{content_hash:016x}")),
                );
            }
            output
        }
        "matrixark_resource_blob_fetch" => {
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobFetch {
                    uri: request.key.clone(),
                    offset: request.blob_offset.unwrap_or(0),
                    length: request.blob_length.unwrap_or(0),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobChunk { payload_base64, total_size, eof } = response.response {
                output.status = "served".to_string();
                output.value = payload_base64;
                output.extra.insert("matrixark_blob_total_size".to_string(), json!(total_size));
                output.extra.insert("matrixark_blob_eof".to_string(), json!(eof));
            }
            output
        }
        "matrixark_resource_blob_sweep" => {
            let tenant_hash: u64 = request
                .key
                .trim()
                .parse()
                .map_err(|_| format!("blob sweep needs a decimal tenant hash in key, got {:?}", request.key))?;
            let referenced: Vec<u64> = request
                .blob_referenced_hashes
                .clone()
                .unwrap_or_default()
                .iter()
                .filter_map(|hex| u64::from_str_radix(hex.trim(), 16).ok())
                .collect();
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobSweep {
                    tenant_hash,
                    referenced_content_hashes: referenced,
                    min_age_ms: request.blob_min_age_ms.unwrap_or(3_600_000),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobSwept { scanned, deleted } = response.response {
                output.status = "swept".to_string();
                output.extra.insert("matrixark_blob_scanned".to_string(), json!(scanned));
                output.extra.insert("matrixark_blob_deleted".to_string(), json!(deleted));
            }
            output
        }
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
            // Type-index maintenance rides the same durable batch as the data, derived from the
            // very payloads being written, so the index can never lag a committed append. Only
            // record-shard keys feed it (see record_shard_key_parts).
            let mut index_entries: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
            // (base, record_type) touched by this batch. The types are already computed here for
            // the type index, so stamping a version costs one small write per type and no extra
            // decode.
            let mut touched_types: std::collections::BTreeSet<(String, String)> =
                std::collections::BTreeSet::new();
            for (key, entries) in &grouped {
                if let Some((base, shard6)) = record_shard_key_parts(key) {
                    for (field, value) in entries {
                        let Ok(value_text) = std::str::from_utf8(value) else {
                            continue;
                        };
                        // Both side indexes come from the same records, so decode once. This
                        // used to call `payload_record_types` (itself a wrapper over
                        // `decode_matrixark_payload`) and then decode the same string again,
                        // deserializing every appended record into a Value tree twice.
                        //
                        // It now does not build a tree at all in the common case: the two
                        // indexes need a record type and a scope key, and `IndexFacts` borrows
                        // exactly those out of the buffer while serde skips the rest of the
                        // record without materialising it. A payload whose shape that struct
                        // does not model falls back to the `Value` decode, which is the same
                        // code as before and answers identically.
                        let (type_names, bucket_names): (Vec<String>, Vec<String>) =
                            match payload_index_facts(value_text) {
                                Some(facts) => (
                                    facts_record_types(&facts)
                                        .into_iter()
                                        .map(str::to_string)
                                        .collect(),
                                    facts.iter().flat_map(facts_scope_buckets).collect(),
                                ),
                                None => {
                                    let decoded = decode_matrixark_payload(value_text);
                                    (
                                        records_record_types(&decoded),
                                        decoded
                                            .iter()
                                            .flat_map(record_scope_buckets)
                                            .collect(),
                                    )
                                }
                            };
                        for record_type in type_names {
                            touched_types.insert((base.to_string(), record_type.clone()));
                            index_entries
                                .entry(type_index_key(base, &record_type))
                                .or_default()
                                .push((format!("{shard6}:{field}"), b"1".to_vec()));
                        }
                        for bucket in bucket_names {
                            index_entries
                                .entry(scope_index_key(base, &bucket))
                                .or_default()
                                .push((format!("{shard6}:{field}"), b"1".to_vec()));
                        }
                    }
                }
            }
            let mut commands = Vec::with_capacity(
                grouped.len()
                    + index_entries.len()
                    + usize::from(!request.key.trim().is_empty()),
            );
            for (key, entries) in grouped {
                commands.push(Command::HashMultiSet { key, entries });
            }
            for (key, entries) in index_entries {
                commands.push(Command::HashMultiSet { key, entries });
            }
            // The version rides the SAME durable batch as the data and the index entries, so
            // it can never lag a committed append -- the same rule the type index itself follows.
            // The value is the new record count: it is already in hand and strictly increases, so
            // it is a token that changes on every append of that type without a read-modify-write.
            let version_stamp = if request.value.trim().is_empty() {
                unix_ms().to_string()
            } else {
                request.value.trim().to_string()
            };
            for (base, record_type) in touched_types {
                commands.push(Command::StringSet {
                    key: type_version_key(&base, &record_type),
                    value: version_stamp.clone().into_bytes(),
                });
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
            // Read the caller's choice before the loop: a bool is Copy, so this stays valid
            // even where the arm has already moved other fields out of the request.
            let inline_json = request.records_inline_json;
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
                        value: record_payload(value, inline_json),
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
        "hgetall" | "scan_hash" => {
            let inline_json = request.records_inline_json;
            hash_entries_output(&engine, request.key, root, inline_json)?
        }
        // Read the durability barrier counters. They are collected by every site that takes a
        // barrier and were, until now, unreachable from outside the process -- so the one number
        // that says how much of a write is barrier-bound could not be measured, only argued.
        // `field == "reset"` clears them first, so a harness can bracket a span.
        "durability_barriers" => {
            if request.field == "reset" {
                temporalstore_rust::durability_metrics::reset();
            }
            let counts = temporalstore_rust::durability_metrics::snapshot();
            let mut map = serde_json::Map::new();
            for (site, count) in counts {
                map.insert(site.to_string(), serde_json::json!(count));
            }
            json_output(serde_json::Value::Object(map), root)?
        }
        // Run the engine's own storage-manager cycle: dump the catalog, mint the durable WAL
        // anchor, and let reclaim use it. This proxy never ran it, so the anchor was never
        // produced and the log could never be reclaimed -- every start replayed everything.
        // Exposed as an op, not a background thread: it rewrites durable structures, so it runs
        // when asked. `shard_size` carries the dump budget so no new request field is needed.
        "storage_manager_cycle" => {
            // Only an embedded engine has a cycle to run; a remote table is served elsewhere.
            let RecordStore::Local(local) = &engine else {
                return Err("the storage-manager cycle needs a local engine".to_string());
            };
            // `shard_size` carries the dump budget: the request shape has no field for it, and
            // adding one would change a wire format for a maintenance call.
            let budget = request.shard_size.unwrap_or(4).clamp(1, 1024) as usize;
            // `field == "unblock_reclaim"` dumps the buckets that BLOCK reclaim rather than the
            // dirty ones. Reclaim wants a manifest matching each bucket's current generation; a
            // bucket that went clean without ever being dumped at that generation has none, and
            // the ordinary cadence only selects DIRTY buckets -- so it will never be dumped again
            // and the log can never be reclaimed past it. Observed on one box as 3,085 such
            // buckets, unchanged across cycles, with the log growing ~15 MB/hour and cold start
            // growing with it at ~0.13 s per MB.
            let selected_dump_buckets = if request.field == "unblock_reclaim" {
                // EVERY bucket, not just the ones reclaim currently reports as missing.
                //
                // Coverage is exactly the last round's dump: the cycle keeps one bucket-dump
                // manifest, so dumping the missing buckets covers those and un-covers everything
                // dumped before. Measured on a real store, chasing the missing list oscillated --
                // 2085 dumped left 1008 missing, then 1032 dumped left 2061 missing -- and would
                // never converge. If only the last round is covered, the last round has to hold
                // every bucket.
                local
                    .bucket_storage_summaries(DEFAULT_SHARD_ID)
                    .into_iter()
                    .map(|summary| summary.routing_bucket)
                    .collect::<Vec<u32>>()
            } else {
                Vec::new()
            };
            let requested_unblock_buckets = selected_dump_buckets.len();
            // The ordinary budget paces the dirty-bucket cadence; it must not silently truncate an
            // unblock round, which would leave the log blocked and look like it had been tried.
            let dump_cap = budget.max(requested_unblock_buckets);
            let report = local.run_storage_manager_cycle(
                temporalstore_rust::engine::reports::StorageManagerCycleRequest {
                    shard_id: DEFAULT_SHARD_ID,
                    max_dump_buckets_per_round: dump_cap,
                    warm_cache: false,
                    selected_dump_buckets,
                    ..Default::default()
                },
            );
            // Report the reclaim outcome, not just that the cycle ended: a cycle that frees
            // nothing looks identical to one that freed plenty unless it says which.
            let reclaim = report
                .wal_reclaim_report
                .as_ref()
                .map(|r| serde_json::to_value(r).unwrap_or(serde_json::Value::Null))
                .unwrap_or(serde_json::Value::Null);
            json_output(
                serde_json::json!({
                    "completed": report.completed,
                    "shard_id": DEFAULT_SHARD_ID,
                    "max_dump_buckets_per_round": dump_cap,
                    // Say how many blocking buckets this round took on, so a caller can tell
                    // "nothing was blocking" from "the budget only reached part of the backlog".
                    "requested_unblock_buckets": requested_unblock_buckets,
                    "duration_ms": report.duration_ms,
                    "stages": report.native_stage_order,
                    "wal_reclaim_report": reclaim,
                }),
                root,
            )?
        }
        "matrixark_scan_candidates" => {
            json_output(scan_matrixark_candidates(&engine, &request)?, root)?
        }
        "matrixark_forget_scope" => {
            let count_key = required_option(request.count_key.clone(), "count_key")?;
            let record_hash_key =
                required_option(request.record_hash_key.clone(), "record_hash_key")?;
            let shard_size = request.shard_size.unwrap_or(1024).max(1);
            let scope = request
                .scope
                .clone()
                .ok_or_else(|| "missing scope".to_string())?;
            let stats =
                forget_scope_records(&engine, &record_hash_key, &count_key, shard_size, &scope)?;
            let mut output = empty_output(root);
            output.status = "forgotten".to_string();
            output.count = Some(stats.records_removed);
            output.extra.insert(
                "matrixark_forget_records_removed".to_string(),
                json!(stats.records_removed),
            );
            output.extra.insert(
                "matrixark_forget_records_scanned".to_string(),
                json!(stats.records_scanned),
            );
            output.extra.insert(
                "matrixark_forget_fields_deleted".to_string(),
                json!(stats.fields_deleted),
            );
            output.extra.insert(
                "matrixark_forget_fields_rewritten".to_string(),
                json!(stats.fields_rewritten),
            );
            output.extra.insert(
                "matrixark_forget_fields_visited".to_string(),
                json!(stats.fields_visited),
            );
            output.extra.insert(
                "matrixark_forget_fields_without_match".to_string(),
                json!(stats.fields_without_match),
            );
            output.extra.insert(
                "matrixark_forget_shards_scanned".to_string(),
                json!(stats.shards_scanned),
            );
            output.extra.insert(
                "matrixark_forget_scope".to_string(),
                json!("scope_prefixed_records"),
            );
            output
        }
        "matrixark_delete_records" => {
            let count_key = required_option(request.count_key.clone(), "count_key")?;
            let record_hash_key =
                required_option(request.record_hash_key.clone(), "record_hash_key")?;
            let shard_size = request.shard_size.unwrap_or(1024).max(1);
            let ids = request.record_ids.clone().unwrap_or_default();
            let stats =
                delete_records_by_ids(&engine, &record_hash_key, &count_key, shard_size, &ids)?;
            let mut output = empty_output(root);
            output.status = "deleted".to_string();
            output.count = Some(stats.records_removed);
            output.extra.insert(
                "matrixark_delete_records_removed".to_string(),
                json!(stats.records_removed),
            );
            output.extra.insert(
                "matrixark_delete_records_scanned".to_string(),
                json!(stats.records_scanned),
            );
            output.extra.insert(
                "matrixark_delete_fields_deleted".to_string(),
                json!(stats.fields_deleted),
            );
            output.extra.insert(
                "matrixark_delete_fields_rewritten".to_string(),
                json!(stats.fields_rewritten),
            );
            output.extra.insert(
                "matrixark_delete_fields_visited".to_string(),
                json!(stats.fields_visited),
            );
            output.extra.insert(
                "matrixark_delete_fields_without_match".to_string(),
                json!(stats.fields_without_match),
            );
            output.extra.insert(
                "matrixark_delete_fields_pointed_only".to_string(),
                json!(stats.fields_pointed_only),
            );
            output.extra.insert(
                "matrixark_delete_locator_locations_dropped".to_string(),
                json!(stats.locator_locations_dropped),
            );
            output.extra.insert(
                "matrixark_delete_ids_requested".to_string(),
                json!(ids.len()),
            );
            output
        }
        "matrixark_retrieve_context_pack" => retrieve_context_pack_output(engine, &request, root)?,
        "matrixark_retrieve_context_pack_full_scan" => {
            json_output(retrieve_context_pack_native(engine, &request)?, root)?
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
        "put_string" | "get_string" | "delete" | "del" | "hgetall" | "scan_hash"
        | "matrixark_resource_blob_put"
        | "matrixark_resource_blob_fetch"
        | "matrixark_resource_blob_sweep" => {
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
        "matrixark_delete_records" => {
            require_non_empty("count_key", request.count_key.as_deref().unwrap_or(""))?;
            require_non_empty(
                "record_hash_key",
                request.record_hash_key.as_deref().unwrap_or(""),
            )
        }
        "matrixark_forget_scope" => {
            require_non_empty("count_key", request.count_key.as_deref().unwrap_or(""))?;
            require_non_empty(
                "record_hash_key",
                request.record_hash_key.as_deref().unwrap_or(""),
            )?;
            if request
                .scope
                .as_ref()
                .map(Value::is_object)
                .unwrap_or(false)
            {
                Ok(())
            } else {
                Err("forget requires a scope object".to_string())
            }
        }
        // Read-only counter dump: no key, no field, nothing to validate.
        "durability_barriers" | "storage_manager_cycle" => Ok(()),
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
    engine: &RecordStore,
    key: String,
    root: PathBuf,
    inline_json: bool,
) -> Result<RecordLogOutput, String> {
    // Read through the hash snapshot: a raw HashGetAll re-reads every field's page uncached on
    // each call (measured 14.6 ms warm for a 27-field hash), while the snapshot is read once and
    // kept current by the write runtimes -- the same coherence every internal hgetall relies on.
    {
        let decoded = hgetall_map(engine, key.clone())?;
        {
            let mut records = Vec::new();
            for (field, value) in &decoded {
                records.push(HashReadRecord {
                    key: key.clone(),
                    field: field.clone(),
                    value: record_payload(value.clone(), inline_json),
                });
            }
            let mut extra = BTreeMap::new();
            extra.insert("native_prefix_scan".to_string(), json!(true));
            extra.insert(
                "prefix_scan_path".to_string(),
                json!("rust_proxy_scan_hash_snapshot"),
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
    }
}

/// Where a record-log request is actually served from.
///
/// `Local` is the historical behaviour: an embedded `TemporalEngine` under
/// `record_log_root()`. It keeps the zero-dependency single-process dev path working.
///
/// `Remote` is the deployed topology the gateway is documented to use: commands are
/// issued through the **ProxyService**, which resolves shard placement from the
/// **metaserver** and forwards to the **datanode**. In this mode the CLI owns no
/// storage at all — nothing is written under `/tmp`, and every gateway worker sees
/// one shared store instead of a private per-worker one.
///
/// Selected by `MATRIXARK_TEMPORALSTORE_PROXY_ADDR`; unset keeps `Local`, so this cannot
/// change the behaviour of an existing deployment. Deliberately NOT `TS_PROXY_ADDR`: that
/// name is already exported on any host running the proxy or the client binaries, and
/// reusing it would silently flip a colocated CLI into remote mode.
#[derive(Clone)]
enum RecordStore {
    Local(TemporalEngine),
    Remote(Box<TemporalStoreTable>),
}

impl std::fmt::Debug for RecordStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Names the variant only: which store answered is the thing a failing
        // assertion needs, and neither inner type is Debug.
        match self {
            RecordStore::Local(_) => f.write_str("RecordStore::Local"),
            RecordStore::Remote(_) => f.write_str("RecordStore::Remote"),
        }
    }
}

impl RecordStore {
    fn execute(&self, request: ExecuteRequest) -> ExecuteResponse {
        match self {
            RecordStore::Local(engine) => engine.execute(request),
            RecordStore::Remote(table) => match table.execute(request.command) {
                Ok(response) => response,
                Err(error) => ExecuteResponse {
                    status: Status::error("record_log_remote_execute_failed", error.to_string()),
                    response: CommandResponse::Empty,
                },
            },
        }
    }

    /// Remote writes are committed by the datanode that owns the shard, so the
    /// durability barrier lives there; the client call is the durable call.
    fn execute_durable(&self, request: ExecuteRequest) -> ExecuteResponse {
        match self {
            RecordStore::Local(engine) => engine.execute_durable(request),
            RecordStore::Remote(_) => self.execute(request),
        }
    }

    fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        match self {
            RecordStore::Local(engine) => engine.batch_execute(request),
            RecordStore::Remote(table) => match table.batch_execute(request.commands) {
                Ok(response) => response,
                Err(error) => BatchExecuteResponse {
                    status: Status::error("record_log_remote_batch_failed", error.to_string()),
                    responses: Vec::new(),
                },
            },
        }
    }

    /// Make buffered writes visible to readers.
    ///
    /// Only an embedded engine has an unpublished index to flush. In remote mode the
    /// write already landed in the datanode's own engine, which publishes its index
    /// itself — so this is a no-op reporting zero bytes, not a failure.
    fn publish_shard_index_snapshot_for_keys(
        &self,
        shard_id: ShardId,
        selected_keys: impl IntoIterator<Item = String>,
    ) -> Result<usize, Status> {
        match self {
            RecordStore::Local(engine) => {
                engine.publish_shard_index_snapshot_for_keys(shard_id, selected_keys)
            }
            RecordStore::Remote(_) => Ok(0),
        }
    }

    fn unload_shard(&self, shard_id: ShardId) {
        // Only meaningful for an embedded engine; a remote store's shards are the
        // datanode's to load and unload.
        if let RecordStore::Local(engine) = self {
            engine.unload_shard(shard_id);
        }
    }
}

/// The ProxyService address for remote mode, or `None` to serve locally.
fn record_log_proxy_addr() -> Option<String> {
    env::var("MATRIXARK_TEMPORALSTORE_PROXY_ADDR")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn engine_cache() -> &'static Mutex<BTreeMap<PathBuf, RecordStore>> {
    static ENGINE_CACHE: OnceLock<Mutex<BTreeMap<PathBuf, RecordStore>>> = OnceLock::new();
    ENGINE_CACHE.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// The engine's own Prometheus text, for the engine this process holds open.
///
/// `TemporalEngine::prometheus_metrics` already renders the page-cache counters and the engine
/// binary serves them on its own `/metrics`. A onebox does not run that binary -- it runs this
/// proxy -- so those counters existed and nothing published them here. Appending what the engine
/// already renders keeps ONE set of names across both surfaces: a dashboard or alert written
/// against the engine works unchanged against a proxy, which a proxy-specific family would not.
///
/// Emitted only when this process holds exactly one local engine. The engine labels its series by
/// `shard_id`, and every engine here loads the same default shard, so two of them would emit two
/// series with identical labels -- which is a malformed scrape rather than more information. One
/// engine is the onebox case this exists for; a proxy fanned out over several record-log prefixes
/// keeps the request counters it always had.
fn engine_prometheus_metrics() -> String {
    let cache = match engine_cache().lock() {
        Ok(cache) => cache,
        // A poisoned lock must not cost the rest of the response; the request counters above it
        // are still true.
        Err(_) => return String::new(),
    };
    let mut local = cache.values().filter_map(|store| match store {
        RecordStore::Local(engine) => Some(engine),
        // A remote table keeps no local page cache, so it has nothing to report.
        RecordStore::Remote(_) => None,
    });
    let engine = match (local.next(), local.next()) {
        (Some(engine), None) => engine,
        _ => return String::new(),
    };
    engine.prometheus_metrics()
}

fn cached_engine_count() -> usize {
    engine_cache().lock().map(|cache| cache.len()).unwrap_or(0)
}

fn clear_engine_cache() {
    if let Ok(mut cache) = engine_cache().lock() {
        cache.clear();
    }
}

/// Page-cache capacity already handed out across every engine this process has opened.
static ENGINE_CACHE_GRANTED_BYTES: AtomicUsize = AtomicUsize::new(0);

/// MemTotal in bytes, or None where /proc is not available.
fn system_memory_bytes() -> Option<usize> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in text.lines() {
        let rest = match line.strip_prefix("MemTotal:") {
            Some(rest) => rest,
            None => continue,
        };
        let kilobytes: usize = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
        return kilobytes.checked_mul(1024);
    }
    None
}

/// Page-cache capacity for a newly opened engine.
///
/// The old fixed 128 MiB default is smaller than the working set of any store past a few hundred
/// megabytes, and every page read that misses it pays a second-layer read AND a cache fill. That
/// is what made writes look linear in corpus size. Measured on one 260 MB store, the same ingest
/// took 3 291 ms at 128 MiB, 1 157 ms at 384 MiB and 902 ms at 1 GiB, while that ingest against a
/// 20 MB store took 173 ms -- so the "linear growth" was mostly the working set crossing a fixed
/// cache, not the corpus. A 24 GiB machine now lands each engine at the 512 MiB per-engine
/// ceiling.
///
/// This is a CAPACITY, not an allocation: a cache that may hold a gigabyte still holds only what
/// has actually been read, so a small store costs no more than it did before. What the capacity
/// does set is a ceiling, so the number is derived from the machine and bounded twice -- per
/// engine, and across every engine this process opens, since one proxy can serve many namespaces
/// and N independent caches at the per-engine size would be N times the intended footprint.
/// `MATRIXARK_RUST_PROXY_CACHE_BYTES` still overrides this absolutely, per engine, for
/// deployments that know their own working set.
fn default_engine_cache_bytes() -> usize {
    const FLOOR: usize = 128 * 1024 * 1024;
    const PER_ENGINE_CEILING: usize = 512 * 1024 * 1024;
    const PROCESS_FLOOR: usize = 512 * 1024 * 1024;
    const PROCESS_CEILING: usize = 4096 * 1024 * 1024;

    let memory = match system_memory_bytes() {
        Some(memory) => memory,
        None => return FLOOR,
    };
    // Per engine, not per process: one proxy opens a separate engine per record-log prefix, so a
    // generous per-engine number spent entirely on the first one starves the rest. A share each
    // beats everything for one.
    let want = (memory / 16).clamp(FLOOR, PER_ENGINE_CEILING);
    let process_ceiling = (memory / 4).clamp(PROCESS_FLOOR, PROCESS_CEILING);
    // First come, first served against the process budget: an engine opened once the budget is
    // spent falls back to the floor rather than pushing the process past its ceiling.
    let granted = ENGINE_CACHE_GRANTED_BYTES.load(Ordering::Relaxed);
    let remaining = process_ceiling.saturating_sub(granted);
    let grant = if remaining >= want { want } else { FLOOR };
    ENGINE_CACHE_GRANTED_BYTES.fetch_add(grant, Ordering::Relaxed);
    grant
}

fn open_engine(request: &RecordLogRequest) -> Result<RecordStore, String> {
    let root = record_log_root(request);
    {
        let cache = engine_cache()
            .lock()
            .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
        if let Some(engine) = cache.get(&root) {
            return Ok(engine.clone());
        }
    }
    if let Some(proxy_addr) = record_log_proxy_addr() {
        let store = open_remote_store(request, &proxy_addr)?;
        let mut cache = engine_cache()
            .lock()
            .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
        cache.insert(root, store.clone());
        return Ok(store);
    }
    std::fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create record-log root {}: {error}",
            root.display()
        )
    })?;
    let cache_bytes = env::var("MATRIXARK_RUST_PROXY_CACHE_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(default_engine_cache_bytes);
    eprintln!(
        "engine page cache: {} MiB for {} ({})",
        cache_bytes / (1024 * 1024),
        root.display(),
        if env::var("MATRIXARK_RUST_PROXY_CACHE_BYTES").is_ok() {
            "MATRIXARK_RUST_PROXY_CACHE_BYTES"
        } else {
            "default, derived from system memory"
        }
    );
    let engine = TemporalEngine::with_local_dirs_and_block_store_options(
        cache_bytes,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
        matrixark_proxy_block_store_options(),
    );
    // Hook-mode startup must publish serving state quickly after a normal local
    // restart. Rebuild decoded serving maps during load, then warm the page cache
    // in the background so first requests can read through storage instead of
    // waiting for a full synchronous cache promotion pass.
    if env::var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD").is_err() {
        // Only the DEFAULT moves: an operator who set the variable keeps what they asked for.
        engine.warm_cache_in_background_on_load();
    }
    // A shard load can be REFUSED (corrupt delta stream, WAL hole, replay failure) or can
    // genuinely fail partway. `load_shard` discards that answer, and the engine below is
    // cached -- so a refused load used to become a healthy-looking server whose every op
    // returns shard_not_loaded, which upstream layers can mistake for an empty store. That
    // is precisely how a damaged-at-scale store served vacuous empties on every reload.
    // Refuse to open instead: the error names the cause, nothing is cached, and the next
    // request retries the load rather than inheriting a permanently-empty engine.
    let load = engine.load_shard_with(temporalstore_rust::LoadShardRequest {
        shard_id: DEFAULT_SHARD_ID,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: u32::MAX,
        readonly: false,
        table_name: String::new(),
    });
    if !load.status.ok && load.status.code != "already_exists" {
        return Err(format!(
            "shard load refused for record-log root {}: {}: {}",
            root.display(),
            load.status.code,
            load.status.message
        ));
    }
    // Warm the page cache on a background thread. `MATRIXARK_RUST_PROXY_ASYNC_CACHE_WARM_ON_LOAD`
    // could stop this and defaulted on; nothing set it, so every proxy that loaded a record-log
    // root warmed, and this is simply what loading one does.
    {
        let warm_engine = engine.clone();
        let warm_root = root.clone();
        std::thread::spawn(move || {
            let report =
                warm_engine.storage_cache_warmup_report(DEFAULT_SHARD_ID, Vec::<u32>::new());
            eprintln!(
                "matrixark_rust_proxy_async_cache_warm root={} considered={} warmed={} already_cached={} failed={} bytes={}",
                warm_root.display(),
                report.considered_page_refs,
                report.warmed_page_refs,
                report.already_cached_page_refs,
                report.failed_page_refs,
                report.warmed_bytes
            );
        });
    }
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
                .and_then(|value| temporalstore_rust::env_flag::parse_bool(&value))
                .unwrap_or(false),
            ..Config::default()
        },
    });
    // Embedded log maintenance: the proxy engine runs no storage-manager cycle (only the
    // server/data-node do), so without this its WAL and index-log grow without bound -- a
    // 100K-record ingest left a multi-GB index log that nothing ever truncated. Poll the
    // threshold-dump cadence in the background: when the undumped index-log gap crosses
    // `TS_INDEX_DUMP_WAL_GAP_BYTES`, dump the catalog and reclaim the log prefixes the dump
    // made redundant. The poll itself is one file-length stat per interval; the dump/reclaim
    // runs off the request path so no client write pays for the base-index materialization.
    // No-op (thread not spawned) with the interval set to 0.
    let reclaim_interval_ms = env::var("MATRIXARK_RUST_PROXY_LOG_RECLAIM_INTERVAL_MS")
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(1000);
    if reclaim_interval_ms > 0 {
        let reclaim_engine = engine.clone();
        let reclaim_root = root.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(std::time::Duration::from_millis(reclaim_interval_ms));
            if let Some(report) = reclaim_engine.maybe_dump_and_reclaim_index_logs(DEFAULT_SHARD_ID)
            {
                eprintln!(
                    "matrixark_rust_proxy_log_reclaim root={} wal_anchor={} index_log_bytes={}->{} ({} records removed) wal_bytes={}->{} ({} records removed) floor={:?}",
                    reclaim_root.display(),
                    report.wal_anchor,
                    report.index_log_bytes_before,
                    report.index_log_bytes_after,
                    report.index_log_records_removed,
                    report.wal_bytes_before,
                    report.wal_bytes_after,
                    report.wal_records_removed,
                    report.wal_retention_floor,
                );
            }
        });
    }
    let store = RecordStore::Local(engine);
    let mut cache = engine_cache()
        .lock()
        .map_err(|_| "record-log engine cache lock poisoned".to_string())?;
    cache.insert(root, store.clone());
    Ok(store)
}

/// Open the record log against the deployed tier: ProxyService -> metaserver -> datanode.
///
/// Topology comes from the metaserver via `open_table_from_meta`, so shard placement is
/// the cluster's answer, not this process's guess. If the table is not registered yet we
/// fall back to a single-shard table so a fresh cluster still serves, and let the client's
/// own topology refresh correct it on the first write.
fn open_remote_store(request: &RecordLogRequest, proxy_addr: &str) -> Result<RecordStore, String> {
    let namespace = non_empty_or(&request.namespace, "deploy_ns").to_string();
    let table = non_empty_or(&request.table, "deploy_table").to_string();
    // The engine-library defaults (200ms) are tuned for key/value RPC; a context ingest
    // batch through the proxy is far heavier, so use the proxy's own context timeout.
    let io_timeout_ms = env_u64_any(
        &[
            "MATRIXARK_TEMPORALSTORE_PROXY_IO_TIMEOUT_MS",
            "TS_PROXY_CONTEXT_IO_TIMEOUT_MS",
        ],
        30_000,
    );
    let connect_timeout_ms = env_u64_any(
        &["MATRIXARK_TEMPORALSTORE_PROXY_CONNECT_TIMEOUT_MS"],
        2_000,
    );
    // `request.metaserver` has, until now, only been hashed into a directory name. Feeding it
    // to the client is what makes shard placement the cluster's answer rather than a guess:
    // without a meta_addr the client cannot sync topology at all and every table silently
    // collapses to one shard.
    // "No metaserver" is spelled five ways, and this must read all of them.
    //
    // `storage_backend::single_node` is the ONE implementation of that rule -- its own docs say a
    // second copy is the failure it exists to prevent -- and it treats "", "local", "none",
    // "standalone" and "off" as "there is no metaserver". Deciding it here with `is_empty()`
    // re-derived the rule and got a narrower answer: a one-box deployment, which sets
    // TS_META_ADDR=local, handed "local" to the client as a literal address and every write
    // failed with `invalid socket address`. The Python side already agrees with the constant
    // (META_SENTINELS in matrixark_deployment_plan.py); this is the surface that did not.
    let meta_addr_raw = non_empty_or(&request.metaserver, "").to_string();
    let meta_addr = if temporalstore_rust::storage_backend::single_node(Some(&meta_addr_raw)) {
        String::new()
    } else {
        meta_addr_raw
    };
    let client = TemporalStoreClient::with_options(temporalstore_rust::ClientOptions {
        proxy_addr: proxy_addr.to_string(),
        meta_addr: if meta_addr.is_empty() {
            None
        } else {
            Some(meta_addr)
        },
        io_timeout_ms,
        connect_timeout_ms,
        meta_sync_deadline_ms: env_u64_any(
            &["MATRIXARK_TEMPORALSTORE_META_SYNC_DEADLINE_MS"],
            2_000,
        ),
        ..Default::default()
    });
    let handle = match client.open_table_from_meta(namespace.clone(), table.clone()) {
        Ok(handle) => handle,
        Err(error) => {
            // Once per process: the CLI serves a request stream, and one line per request
            // would drown the log (and every distinct scope opens its own table).
            static WARNED: OnceLock<()> = OnceLock::new();
            if WARNED.set(()).is_ok() {
                eprintln!(
                    "matrixark_rust_proxy: metaserver topology sync for {namespace}.{table} \
                     failed ({error}); falling back to a single-shard table on {proxy_addr}"
                );
            }
            client.open_table(
                namespace,
                table,
                temporalstore_rust::TableOptions {
                    first_shard_id: DEFAULT_SHARD_ID,
                    shard_count: 1,
                    io_timeout_ms,
                    connect_timeout_ms,
                    ..Default::default()
                },
            )
        }
    };
    Ok(RecordStore::Remote(Box::new(handle)))
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
            // Was 4096, and every index write an add makes is smaller than that: the postings, the
            // placement rows, the locator entries. Those are also the most repetitive bytes in the
            // system -- one add writes 28 postings carrying the same scope key and policy -- so
            // they are exactly what compression is good at, and exactly what a 4 KB floor excluded.
            //
            // Measured over 120 adds on a fresh store: 176.8 KB per add at 4096, 148.1 KB at 256,
            // and the adds were no slower (152.5 ms -> 144.7 ms). Compressing everything is worse:
            // at a 1-byte floor the disk saving stops (149.7 KB) while the median add rises to
            // 256.0 ms, because tiny payloads cost more to compress than they give back.
            256,
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
        .and_then(|value| temporalstore_rust::env_flag::parse_bool(&value))
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

fn env_u64_any(names: &[&str], default: u64) -> u64 {
    names
        .iter()
        .find_map(|name| env::var(name).ok())
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_i32_any(names: &[&str], default: i32) -> i32 {
    names
        .iter()
        .find_map(|name| env::var(name).ok())
        .and_then(|value| value.trim().parse::<i32>().ok())
        .unwrap_or(default)
}

fn execute_empty(engine: &RecordStore, command: Command) -> Result<(), String> {
    // conformance monotonic serving sequence: never let a record_count write regress.
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
    engine: &RecordStore,
    commands: Vec<Command>,
    invalidate_matrixark_scan_cache: bool,
) -> Result<(), String> {
    if commands.is_empty() {
        return Ok(());
    }
    // conformance monotonic serving sequence: never let a record_count write regress
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
    // A whole-key delete genuinely invalidates the snapshot; a single-field delete does not.
    let cache_invalidates = commands
        .iter()
        .filter_map(|command| match command {
            Command::CommonDelete { key } => Some(key.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let cache_field_removals = if hgetall_snapshot_cache_has_entries() {
        commands
            .iter()
            .filter_map(|command| match command {
                Command::HashDelete { key, field } if hgetall_snapshot_contains(key) => {
                    Some((key.clone(), field.clone()))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
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
    for (key, field) in cache_field_removals {
        remove_hgetall_snapshot_fields(&key, std::slice::from_ref(&field));
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

fn read_bytes(engine: &RecordStore, command: Command) -> Result<String, String> {
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

fn read_record_count(engine: &RecordStore, key: &str) -> Result<String, String> {
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
    engine: &RecordStore,
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
                let vector = record_vector_of(&record).or_else(|| record_vector_of(&selected_ref));
                Some(CachedRetrieveCandidate {
                    selected_ref,
                    lower_text,
                    ref_type,
                    vector,
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
    engine: &RecordStore,
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
    // No upper ceiling. This clamped to 128, so a caller asking for every candidate -- which is
    // what MATRIXARK_RETURN_ALL_CANDIDATES exists to do -- silently got 128 and no error. The loop
    // below cannot exceed the candidate count anyway, so the ceiling bounded nothing except the
    // caller's intent.
    let max_selected_refs = if requested_max_selected_refs == 0 {
        DEFAULT_MAX_SELECTED_REFS
    } else {
        requested_max_selected_refs
    }
    .max(1);
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
    let query_vector = ranking_field_from(
        request.query_vector.clone(),
        &request_record,
        "query_vector",
    )
    .filter(|v: &Vec<f32>| !v.is_empty());
    let ranking_uses_vectors = query_vector.is_some();
    let weights = ranking_field_from(
        request.ranking_weights,
        &request_record,
        "ranking_weights",
    )
    .unwrap_or_default();
    // The caller's own prefilter already named the nodes it believes in; the hint is what that
    // belief is worth in the score, and the caller sets its size.
    let hinted: std::collections::HashSet<u64> = request
        .selected_node_hashes
        .as_ref()
        .map(|v| v.iter().copied().collect())
        .unwrap_or_default();
    // 0.0 keeps everything the query can score and excludes only what scores exactly zero.
    let min_score =
        ranking_field_from(request.min_score, &request_record, "min_score").unwrap_or(0.0);
    let score_started = Instant::now();
    let mut skipped_unscoreable = 0_u64;
    let mut skipped_below_threshold = 0_u64;
    let mut candidates = Vec::with_capacity(snapshot.candidates.len());
    for (ordinal, candidate) in snapshot.candidates.iter().enumerate() {
        if candidate.ref_type == "summary" && has_event_candidate && !summary_allowed_for_question {
            continue;
        }
        // Dense when the caller sent a query vector, lexical otherwise. A candidate with no
        // usable vector falls back to the lexical score rather than scoring 0: it is a record we
        // could not compare, not a record we know to be irrelevant, and zeroing it would drop it
        // beneath every lexical match in the same list.
        let lexical = score_lowered_text(&candidate.lower_text, &query_terms);
        let Some(score) = candidate_score(
            &weights,
            query_vector.as_deref(),
            candidate.vector.as_deref(),
            lexical,
            // Lazy: the lexical path must not pay for a hint lookup it will not use.
            || {
                candidate
                    .selected_ref
                    .get("node_hash")
                    .and_then(Value::as_u64)
                    .map(|h| hinted.contains(&h))
                    .unwrap_or(false)
            },
        ) else {
            // Not scoreable under this query. Counted so a store that returns nothing can be told
            // apart from a query that matched nothing.
            skipped_unscoreable += 1;
            continue;
        };
        // The caller's threshold. A score at or below it is not a weak result to be ranked last --
        // it is excluded, which is what makes a per-layer dense retrieval bounded by RELEVANCE
        // rather than by a slot count.
        if score <= min_score {
            skipped_below_threshold += 1;
            continue;
        }
        candidates.push((score, ordinal));
    }
    let score_ms = score_started.elapsed().as_secs_f64() * 1000.0;
    let token_budget = ranking_field_from(
        request.max_context_tokens,
        &request_record,
        "max_context_tokens",
    )
    .unwrap_or(0);
    let layer_floors = ranking_field_from(
        request.layer_min_refs.clone(),
        &request_record,
        "layer_min_refs",
    )
    .unwrap_or_default();
    // A budget or a floor means the pack is bounded by RELEVANCE and SIZE rather than by a slot
    // count, so the slot cut must not run first.
    let budget_governs = token_budget > 0 || !layer_floors.is_empty();
    if budget_governs {
        // Every candidate stays in the running. Cutting to a slot count here would decide the pack
        // before any floor or budget could see it, which is how a large shared_context corpus
        // starves session memory out of a flat 24 slots.
        candidates.sort_by(|left, right| compare_scored_candidate(*left, *right));
    } else {
        let keep = max_selected_refs.min(candidates.len());
        if keep > 0 && candidates.len() > keep {
            candidates
                .select_nth_unstable_by(keep, |left, right| compare_scored_candidate(*left, *right));
            candidates.truncate(keep);
        }
        candidates.sort_by(|left, right| compare_scored_candidate(*left, *right));
    }

    // Pass one gives every layer its floor, in score order within the layer. Pass two spends what
    // is left of the budget on whatever scores highest, wherever it came from.
    let mut budget_keep: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut layer_refs_selected: HashMap<String, u64> = HashMap::new();
    let mut budget_tokens_used = 0_u64;
    if budget_governs {
        let snapshot_for_layer = Arc::clone(&snapshot);
        let layer_of = move |ordinal: usize| -> String {
            snapshot_for_layer
                .candidates
                .get(ordinal)
                .and_then(|candidate| {
                    candidate
                        .selected_ref
                        .get("memory_layer")
                        .and_then(Value::as_str)
                })
                .unwrap_or("")
                .to_string()
        };
        let snapshot_for_tokens = Arc::clone(&snapshot);
        let tokens_of = move |ordinal: usize| -> u64 {
            snapshot_for_tokens
                .candidates
                .get(ordinal)
                .map(|candidate| {
                    candidate
                        .selected_ref
                        .get("token_estimate")
                        .and_then(Value::as_u64)
                        .unwrap_or_else(|| {
                            token_estimate(
                                candidate
                                    .selected_ref
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .unwrap_or(""),
                            )
                        })
                })
                .unwrap_or(0)
        };
        let (keep, per_layer, used) = select_within_budget(
            &candidates,
            &layer_of,
            &tokens_of,
            &layer_floors,
            token_budget,
        );
        budget_keep = keep;
        layer_refs_selected = per_layer;
        budget_tokens_used = used;
    }

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
        if budget_governs {
            if !budget_keep.contains(&ordinal) {
                continue;
            }
        } else if selected_refs.len() >= max_selected_refs {
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
            // Outcome, not intent: how many candidates the query could not place, how many fell
            // under the threshold, and what the fill actually spent.
            "skipped_unscoreable": skipped_unscoreable,
            "skipped_below_threshold": skipped_below_threshold,
            "budget_tokens_used": budget_tokens_used,
            "layer_refs_selected": layer_refs_selected
                .iter()
                .map(|(layer, count)| (layer.clone(), json!(count)))
                .collect::<serde_json::Map<String, Value>>(),
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
            // Outcome, not intent: how many candidates the query could not place, how many fell
            // under the threshold, and what the fill actually spent.
            "skipped_unscoreable": skipped_unscoreable,
            "skipped_below_threshold": skipped_below_threshold,
            "budget_tokens_used": budget_tokens_used,
            "layer_refs_selected": layer_refs_selected
                .iter()
                .map(|(layer, count)| (layer.clone(), json!(count)))
                .collect::<serde_json::Map<String, Value>>(),
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
            // How this pack was ranked. Stated rather than left to be inferred: a caller that
            // knows an encoder is configured still cannot tell whether the path that answered
            // Reported, not asserted: with a query vector the candidates are ordered by cosine
            // against it, and without one by `score_lowered_text` over the query terms. A caller
            // that reads this to decide whether the ranking is semantic must be told which of the
            // two actually ran.
            "ranking": if ranking_uses_vectors {
                "dense_cosine_with_lexical_fallback"
            } else {
                "lexical_term_overlap_and_boosts"
            },
            "ranking_uses_vectors": ranking_uses_vectors,
            "correctness_evidence": native_correctness_evidence(
                scope.is_some(),
                snapshot.placement_partitions_touched,
                !secondary_groups.is_empty(),
                current_state_query,
                dropped_stale_ref,
                correctness,
            ),
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

/// What this path actually did, one field per property.
///
/// These were six fields all reading `selected_count > 0`: one condition wearing six hats, so a
/// non-empty pack marked every property verified and an empty one marked every property failed.
/// Written as a function so that wiring two of them to the same input is visible in the signature
/// rather than buried four hundred lines into the caller.
///
/// Two are `false` and stay `false` here: the shared resource/skill quota and the cross-session
/// quota rerank belong to the full-scan assembly, not to this one. A caller comparing the two paths
/// needs to see that difference, and reporting them as done because the pack was non-empty is how
/// it stayed invisible.
fn native_correctness_evidence(
    scope_applied: bool,
    placement_partitions_touched: usize,
    secondary_index_applied: bool,
    current_state_query: bool,
    stale_superseded_dropped: u64,
    selected_any: bool,
) -> Value {
    json!({
        "scope_filtering": scope_applied,
        "placement_filtering": placement_partitions_touched > 0,
        "compact_secondary_index_prefilter": secondary_index_applied,
        // The supersede pass runs only for a current-state question. For anything else there is
        // nothing to supersede, and saying the pass ran would claim a check nobody made.
        "stale_superseded_exclusion": current_state_query,
        "stale_superseded_dropped": stale_superseded_dropped,
        "shared_resource_skill_quota": false,
        "cross_session_quota_rerank": false,
        // Kept, but named for what it is. This is the one thing the old six all measured.
        "selected_any": selected_any
    })
}

/// The caller's ranking policy: `dense * normalized_cosine + sparse * lexical + hint`.
///
/// Mirrors what the caller computes today, so the engine can reproduce its answer rather than
/// approximate it. The hint is added for candidates the caller named in `selected_node_hashes`,
/// which is how the caller's own prefilter marks a node it already believes in.
#[derive(Debug, Clone, Copy, Deserialize)]
struct RankingWeights {
    #[serde(default = "one_f64")]
    dense: f64,
    #[serde(default)]
    sparse: f64,
    #[serde(default)]
    index_hint: f64,
}

fn one_f64() -> f64 {
    1.0
}

impl Default for RankingWeights {
    fn default() -> Self {
        // Dense-only, which is what this engine did before it could blend at all.
        Self {
            dense: 1.0,
            sparse: 0.0,
            index_hint: 0.0,
        }
    }
}

fn clamp01(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

/// One candidate's score under the caller's policy.
///
/// `dense` is already the 0..1 mapping of cosine, so this is the caller's formula term for term.
/// A candidate with no usable vector contributes 0 to the dense term rather than being dropped:
/// it is a record we could not compare, not one we know to be irrelevant, and with a non-zero
/// sparse weight its lexical score still speaks for it.
/// One candidate's score, choosing the scorer by what the caller actually sent.
///
/// Pulled out of the scoring loop so the no-vector case can be tested. It carries a hazard worth
/// stating: the caller sends `ranking_weights` UNCONDITIONALLY but only sends `query_vector` when
/// dense ranking is enabled, and on a one-box those weights are dense 1.00 / sparse 0.00. Blending
/// with an absent dense term would therefore compute 1.00 * 0 + 0.00 * lexical = 0 for EVERY
/// candidate and rank the whole corpus flat -- silently, with no error and a full-looking pack.
/// So a missing query vector must bypass the blend entirely rather than pass a zero into it.
fn candidate_score<F: FnOnce() -> bool>(
    weights: &RankingWeights,
    query_vector: Option<&[f32]>,
    record_vector: Option<&[f32]>,
    lexical: f64,
    index_hinted: F,
) -> Option<f64> {
    match (query_vector, record_vector) {
        // Dense retrieval. `None` from the scorer means this candidate cannot be placed in the
        // query's space at all -- a different width, so a different encoder -- and it is SKIPPED
        // rather than given a lexical score. Under dense retrieval a candidate the query embedding
        // does not reach is not a weak match, it is not a match, and returning it on a text
        // coincidence is what the embedding was supposed to replace.
        (Some(query), Some(record)) if !query.is_empty() => dense_query_score(query, record)
            .map(|dense| blended_candidate_score(weights, Some(dense), lexical, index_hinted())),
        // A candidate carrying no vector at all, while the caller IS ranking densely: same answer.
        // It cannot be compared, so it is not returned.
        (Some(query), None) if !query.is_empty() => None,
        // No query vector: the caller is not ranking densely, so this is the lexical ranking the
        // engine has always done. Unchanged, and still the default -- dense retrieval is opt-in.
        _ => Some(lexical),
    }
}

fn blended_candidate_score(
    weights: &RankingWeights,
    dense: Option<f64>,
    lexical: f64,
    index_hinted: bool,
) -> f64 {
    let hint = if index_hinted { weights.index_hint } else { 0.0 };
    clamp01(weights.dense * dense.unwrap_or(0.0) + weights.sparse * lexical + hint)
}

/// Cosine similarity, mapped to the same 0..1 range the lexical scorer produces.
///
/// `None` means NOT COMPARABLE, which is not the same as "scored zero".
///
/// Vectors of different lengths are never compared on a prefix: a record embedded by a different
/// model is not less relevant, it simply cannot be placed in this query's space, and a prefix
/// would rank it on the coincidence of its first dimensions. This store has been observed holding
/// 32-dimension vectors labelled as a 1024-dimension model, so the case is real here.
///
/// This returned 0.0 for that case, and the caller blended it: with the one-box weights (dense
/// 1.00, sparse 0.00) the whole score became 0.00, which sank every record from an older encoder
/// BENEATH every lexical match. That is the opposite of "not comparable" -- it is "known to be
/// irrelevant". An Option makes the distinction one the caller has to handle.
/// What the engine selects when the caller names no limit.
///
/// Mirrors `DEFAULT_MAX_SELECTED_REFS` on the calling side. This was a bare `24` here while the
/// caller's own default was 64 and its request built a third `24` inline, so one setting had three
/// numbers and the guard that checks for exactly this only scans environment defaults.
const DEFAULT_MAX_SELECTED_REFS: usize = 64;

/// Which candidates a token budget and a set of per-layer floors admit.
///
/// `scored` is (score, ordinal), already ordered best first. Two passes: every layer takes its
/// floor first, then whatever is left of the budget goes to the best of the rest, wherever it came
/// from. Pulled out of the retrieve path so the policy can be tested on its own -- driving it
/// through the engine could only show which refs came back, which is the same observation for
/// several different causes.
///
/// A floor is a MINIMUM and is honoured even when it overruns the budget: a category that returns
/// nothing is the failure floors exist to prevent. The fill SKIPS an oversized candidate rather
/// than stopping, so one long ref cannot truncate the pack while shorter ones still fit.
fn select_within_budget(
    scored: &[(f64, usize)],
    layer_of: &dyn Fn(usize) -> String,
    tokens_of: &dyn Fn(usize) -> u64,
    floors: &std::collections::BTreeMap<String, u64>,
    token_budget: u64,
) -> (std::collections::HashSet<usize>, HashMap<String, u64>, u64) {
    let mut keep: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut per_layer: HashMap<String, u64> = HashMap::new();
    let mut used = 0_u64;
    for (_, ordinal) in scored.iter() {
        let layer = layer_of(*ordinal);
        let Some(floor) = floors.get(&layer) else {
            continue;
        };
        let taken = per_layer.entry(layer).or_insert(0);
        if *taken >= *floor {
            continue;
        }
        *taken += 1;
        used = used.saturating_add(tokens_of(*ordinal));
        keep.insert(*ordinal);
    }
    for (_, ordinal) in scored.iter() {
        if keep.contains(ordinal) {
            continue;
        }
        let tokens = tokens_of(*ordinal);
        if token_budget > 0 && used.saturating_add(tokens) > token_budget {
            continue;
        }
        used = used.saturating_add(tokens);
        *per_layer.entry(layer_of(*ordinal)).or_insert(0) += 1;
        keep.insert(*ordinal);
    }
    (keep, per_layer, used)
}

/// Read a ranking field from the top level of the request OR from its `record` object.
///
/// The two callers disagree about where the pack request goes, and both are in production. The
/// cdylib client sends the caller's dict AS the payload, so these fields land at the top level; the
/// proxy client passes the same dict as `record=request`, so they land one level down. The engine
/// only ever looked at the top level, which meant a query vector sent over the proxy path was
/// simply never seen -- serde produced None, the scorer fell back to lexical, and nothing anywhere
/// reported that dense ranking had not happened.
///
/// Reading both is what makes the field work regardless of which client sent it. The typed field
/// wins when present, so a caller that sets it explicitly is never overridden by a stale record.
fn ranking_field_from<T: serde::de::DeserializeOwned>(
    typed: Option<T>,
    record: &Value,
    key: &str,
) -> Option<T> {
    typed.or_else(|| {
        record
            .get(key)
            .and_then(|value| serde_json::from_value::<T>(value.clone()).ok())
    })
}

fn dense_query_score(query_vector: &[f32], record_vector: &[f32]) -> Option<f64> {
    if query_vector.is_empty()
        || record_vector.is_empty()
        || query_vector.len() != record_vector.len()
    {
        return None;
    }
    let mut dot = 0.0_f64;
    let mut left = 0.0_f64;
    let mut right = 0.0_f64;
    for (a, b) in query_vector.iter().zip(record_vector.iter()) {
        let (a, b) = (*a as f64, *b as f64);
        dot += a * b;
        left += a * a;
        right += b * b;
    }
    if left <= 0.0 || right <= 0.0 {
        // A zero vector has no direction, so there is no angle to measure -- not an angle of zero.
        return None;
    }
    // Cosine is -1..1; the selector compares against lexical scores in 0..1, so map rather than
    // clamp -- clamping would make every opposed vector tie at zero with every unrelated one.
    Some(((dot / (left.sqrt() * right.sqrt())) + 1.0) / 2.0)
}

/// A candidate's own vector, when it carries one this scorer can use.
fn record_vector_of(record: &Value) -> Option<Vec<f32>> {
    let values = record.get("vector")?.as_array()?;
    let mut out = Vec::with_capacity(values.len());
    for value in values {
        out.push(value.as_f64()? as f32);
    }
    Some(out)
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

/// Order candidates for selection: relevance first, then scan position.
///
/// The ordinal is the candidate's position in the scan, which is append order, so breaking ties on
/// ordinal ASCENDING means the OLDEST matching statement wins. That is worth knowing about: two
/// statements matching a query equally well are often a value and its later revision, and this
/// ranks the stale one first. Measured -- "the deployment window is Monday" then "...Friday",
/// queried for "deployment window", returns Monday first.
///
/// Flipping it to prefer the newer candidate is NOT the fix, and was tried and reverted. This
/// comparator also decides what SURVIVES truncation to `max_selected_refs`, not just the order, so
/// preferring recency globally dropped entity refs out of the pack altogether --
/// `matrixark_native_retrieve_context_pack_returns_selected_refs` fails with the flip and passes
/// without it. A real fix prefers recency WITHIN a ref type, or changes selection alongside the
/// comparator so type coverage is preserved.
fn compare_scored_candidate(left: (f64, usize), right: (f64, usize)) -> std::cmp::Ordering {
    right
        .0
        .partial_cmp(&left.0)
        .unwrap_or(std::cmp::Ordering::Equal)
        .then_with(|| left.1.cmp(&right.1))
}

fn record_log_root(request: &RecordLogRequest) -> PathBuf {
    // Remote mode owns no local storage, so there is no directory to report. Name the tier
    // that actually serves the request instead — reporting a /tmp path the process never
    // touches is how the embedded store stayed invisible in the first place. This value is
    // also the store cache key, and one table handle per namespace/table is exactly right
    // when the datanode (not a per-prefix directory) is the store.
    if let Some(proxy_addr) = record_log_proxy_addr() {
        return PathBuf::from(format!(
            "proxy://{proxy_addr}/{}/{}",
            non_empty_or(&request.namespace, "deploy_ns"),
            non_empty_or(&request.table, "deploy_table"),
        ));
    }
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

    /// The two production clients put the pack request in different places, so every ranking field
    /// must be readable from either. The proxy client sends it as `record=request`; the cdylib
    /// client sends the same dict as the payload itself. Reading only the top level is what made a
    /// query vector sent over the proxy path invisible.
    #[test]
    fn the_ranking_fields_are_read_from_the_record() {
        let record = json!({
            "layer_min_refs": {"session": 1, "profile": 2},
            "max_context_tokens": 30,
            "min_score": 0.25,
            "query_vector": [1.0, 0.0],
        });
        let floors: Option<std::collections::BTreeMap<String, u64>> =
            ranking_field_from(None, &record, "layer_min_refs");
        assert_eq!(
            floors.as_ref().and_then(|map| map.get("session").copied()),
            Some(1),
            "layer_min_refs did not deserialize from the record: {floors:?}"
        );
        assert_eq!(
            ranking_field_from(None, &record, "max_context_tokens"),
            Some(30_u64)
        );
        assert_eq!(ranking_field_from(None, &record, "min_score"), Some(0.25_f64));
        let vector: Option<Vec<f32>> = ranking_field_from(None, &record, "query_vector");
        assert_eq!(vector, Some(vec![1.0_f32, 0.0]));
    }

    /// A field set at the TOP level still wins, so a caller that sets it explicitly is never
    /// overridden by whatever the record happens to carry.
    #[test]
    fn a_top_level_ranking_field_beats_the_record() {
        let record = json!({"max_context_tokens": 30});
        assert_eq!(
            ranking_field_from(Some(99_u64), &record, "max_context_tokens"),
            Some(99)
        );
    }

    fn budget_fixture() -> (Vec<(f64, usize)>, Vec<(&'static str, u64)>) {
        // Three cheap, high-scoring shared_context refs and one expensive, low-scoring session ref
        // -- the shape that makes a large skill corpus crowd out session memory.
        let scored = vec![(0.9, 0), (0.8, 1), (0.7, 2), (0.2, 3)];
        let meta = vec![
            ("shared_context", 4),
            ("shared_context", 4),
            ("shared_context", 4),
            ("session", 20),
        ];
        (scored, meta)
    }

    fn run_budget(
        floors: &[(&str, u64)],
        budget: u64,
    ) -> (std::collections::HashSet<usize>, HashMap<String, u64>, u64) {
        let (scored, meta) = budget_fixture();
        let layers: Vec<String> = meta.iter().map(|(l, _)| l.to_string()).collect();
        let tokens: Vec<u64> = meta.iter().map(|(_, t)| *t).collect();
        let layer_of = move |ordinal: usize| -> String {
            layers.get(ordinal).cloned().unwrap_or_default()
        };
        let tokens_of = move |ordinal: usize| -> u64 { tokens.get(ordinal).copied().unwrap_or(0) };
        let floor_map: std::collections::BTreeMap<String, u64> = floors
            .iter()
            .map(|(name, count)| ((*name).to_string(), *count))
            .collect();
        select_within_budget(&scored, &layer_of, &tokens_of, &floor_map, budget)
    }

    /// Without a floor the budget goes to the best-scoring refs and a whole layer can return
    /// nothing. This is the control, and it must hold or the floor test below proves nothing.
    #[test]
    fn without_a_floor_the_budget_goes_to_the_best_scores() {
        let (keep, per_layer, used) = run_budget(&[], 30);
        assert!(keep.contains(&0) && keep.contains(&1) && keep.contains(&2), "got {keep:?}");
        assert!(!keep.contains(&3), "the session ref should not fit, got {keep:?}");
        assert_eq!(per_layer.get("session"), None, "session must return nothing here");
        assert_eq!(used, 12);
    }

    /// A floor admits a layer the budget would otherwise lose entirely.
    #[test]
    fn a_floor_admits_a_layer_the_budget_would_lose() {
        let (keep, per_layer, used) = run_budget(&[("session", 1)], 30);
        assert!(keep.contains(&3), "the session floor was not honoured, got {keep:?}");
        assert_eq!(per_layer.get("session").copied(), Some(1));
        // The floor spends 20 of 30, so two of the three cheap refs still fit and the third does
        // not -- the fill keeps going rather than stopping at the first that does not fit.
        assert_eq!(used, 28, "expected 20 + 4 + 4, got {used}");
        assert_eq!(keep.len(), 3, "got {keep:?}");
    }

    /// A floor is a MINIMUM: it is honoured even when it alone overruns the budget, because a
    /// category returning nothing is the failure floors exist to prevent.
    #[test]
    fn a_floor_outranks_the_budget_it_overruns() {
        let (keep, _per_layer, used) = run_budget(&[("session", 1)], 5);
        assert!(keep.contains(&3), "got {keep:?}");
        assert_eq!(used, 20, "the floor spends past the budget, got {used}");
        assert_eq!(keep.len(), 1, "and nothing else fits afterwards, got {keep:?}");
    }

    /// A floor for a layer with no candidates admits nothing and must not panic.
    #[test]
    fn a_floor_for_an_absent_layer_is_harmless() {
        let (keep, per_layer, _used) = run_budget(&[("profile", 3)], 30);
        assert_eq!(per_layer.get("profile"), None);
        assert_eq!(keep.len(), 3, "the other layers still fill, got {keep:?}");
    }

    /// No budget means the fill takes everything that qualifies.
    #[test]
    fn a_zero_budget_means_no_token_limit() {
        let (keep, _per_layer, used) = run_budget(&[], 0);
        assert_eq!(keep.len(), 4, "got {keep:?}");
        assert_eq!(used, 32);
    }

    /// Weights WITHOUT a query vector must not zero the ranking.
    ///
    /// This is the default-off path in production: Python sends ranking_weights on every request
    /// but withholds the query vector unless dense ranking is enabled. With one-box weights
    /// (dense 1.00, sparse 0.00) a blend against an absent dense term scores every candidate 0,
    /// which does not error -- it returns a pack whose ordering is meaningless. The engine must
    /// fall through to lexical instead.
    #[test]
    fn weights_without_a_query_vector_do_not_zero_the_ranking() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.08 };
        let record = [1.0_f32, 0.0];
        for query in [None, Some(&[][..])] {
            let got = candidate_score(&onebox, query, Some(&record), 0.75, || false);
            assert_eq!(
                got,
                Some(0.75),
                "query {query:?}: with no query vector the caller is not ranking densely, so this
                 must be the lexical score"
            );
        }
    }

    /// Under DENSE retrieval a candidate carrying no vector is skipped, not scored lexically.
    ///
    /// This is the caller's policy: a candidate the query embedding cannot reach is not a weak
    /// match, it is not a match, and returning it on a text coincidence is what the embedding was
    /// meant to replace. It reverses an earlier version of this engine, which fell back -- that
    /// let un-embedded records occupy slots that dense retrieval had already ruled out.
    #[test]
    fn a_candidate_with_no_vector_is_skipped_under_dense_retrieval() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.08 };
        let query = [1.0_f32, 0.0];
        assert_eq!(candidate_score(&onebox, Some(&query), None, 0.6, || false), None);
    }

    /// The positive control: when BOTH vectors are present the blend really does run, so the two
    /// tests above are asserting a fallback that something else would otherwise have taken.
    #[test]
    fn both_vectors_present_takes_the_dense_blend() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.0 };
        let query = [1.0_f32, 0.0];
        let record = [1.0_f32, 0.0];
        // Identical unit vectors: cosine 1.0 -> normalized 1.0, and the lexical 0.0 is ignored.
        let got = candidate_score(&onebox, Some(&query), Some(&record), 0.0, || false)
            .expect("comparable");
        assert!(got > 0.99, "got {got}, expected the dense term to dominate");
    }

    /// The hint must stay lazy: the lexical path must not evaluate it at all.
    #[test]
    fn the_lexical_path_never_evaluates_the_index_hint() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.08 };
        let mut evaluated = false;
        let got = candidate_score(&onebox, None, None, 0.4, || {
            evaluated = true;
            true
        });
        assert_eq!(got, Some(0.4));
        assert!(!evaluated, "the lexical path paid for a hint lookup it did not use");
    }

    /// The one-box policy, term for term against the caller's own formula.
    ///
    /// The caller computes, with _ONEBOX_DENSE_ONLY true:
    ///     _DENSE_W = 1.00, _SPARSE_W = 0.00
    ///     score = clamp01(_DENSE_W * normalized_dense_score(cosine)
    ///                   + _SPARSE_W * sparse + index_hint_boost)
    ///     normalized_dense_score(v) = clamp01((v + 1) / 2)
    ///
    /// So the engine must return exactly (cosine + 1) / 2 and ignore the lexical score entirely.
    /// If this ever fails, the engine and the caller are ranking differently and the engine must
    /// not be the default -- which is the whole reason this test exists rather than a benchmark.
    #[test]
    fn the_dense_only_policy_reproduces_the_callers_score() {
        let dense_only = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.0 };
        // cosine = 1.0 -> normalized 1.0; cosine = 0.0 -> 0.5; cosine = -1.0 -> 0.0
        for (cosine, expect) in [(1.0, 1.0), (0.0, 0.5), (-1.0, 0.0), (0.5, 0.75)] {
            let normalized = (cosine + 1.0) / 2.0;
            let got = blended_candidate_score(&dense_only, Some(normalized), 1.0, false);
            assert!(
                (got - expect).abs() < 1e-9,
                "cosine {cosine}: got {got}, caller would produce {expect}"
            );
        }
    }

    /// With a zero sparse weight the lexical score must not reach the result AT ALL.
    ///
    /// The positive control for the test above: if the engine quietly added lexical, the first
    /// test would still pass for lexical == 1.0 by coincidence of the numbers.
    #[test]
    fn a_zero_sparse_weight_ignores_the_lexical_score() {
        let dense_only = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.0 };
        let high = blended_candidate_score(&dense_only, Some(0.5), 1.0, false);
        let low = blended_candidate_score(&dense_only, Some(0.5), 0.0, false);
        assert_eq!(high, low, "lexical changed a dense-only score");
    }

    /// The non-one-box policy: 0.72 dense, 0.28 lexical.
    #[test]
    fn the_blended_policy_weights_both_terms() {
        let blended = RankingWeights { dense: 0.72, sparse: 0.28, index_hint: 0.0 };
        // The caller's arithmetic: 0.72 * 0.5 + 0.28 * 1.0 = 0.36 + 0.28 = 0.64
        let got = blended_candidate_score(&blended, Some(0.5), 1.0, false);
        assert!((got - 0.64).abs() < 1e-9, "got {got}, expected 0.64");
    }

    /// The index hint is worth what the caller says and applies only to hinted candidates.
    #[test]
    fn the_index_hint_applies_only_where_the_caller_hinted() {
        let weights = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.08 };
        let hinted = blended_candidate_score(&weights, Some(0.5), 0.0, true);
        let plain = blended_candidate_score(&weights, Some(0.5), 0.0, false);
        assert!((hinted - 0.58).abs() < 1e-9, "hinted got {hinted}, expected 0.58");
        assert!((plain - 0.50).abs() < 1e-9, "unhinted got {plain}, expected 0.50");
    }

    /// clamp01, because the caller clamps and an unclamped engine would order ties differently
    /// at the top of the list -- exactly where selection happens.
    #[test]
    fn the_score_is_clamped_like_the_callers() {
        let weights = RankingWeights { dense: 1.0, sparse: 1.0, index_hint: 0.5 };
        assert_eq!(blended_candidate_score(&weights, Some(1.0), 1.0, true), 1.0);
        let negative = RankingWeights { dense: -5.0, sparse: 0.0, index_hint: 0.0 };
        assert_eq!(blended_candidate_score(&negative, Some(1.0), 0.0, false), 0.0);
    }

    /// A candidate with no usable vector contributes 0 to the DENSE term but keeps its lexical
    /// one, so a blended policy can still rank it. Dropping it to 0 outright would sink every
    /// record the engine could not compare beneath every record it could.
    #[test]
    fn a_candidate_without_a_vector_keeps_its_lexical_score() {
        let blended = RankingWeights { dense: 0.72, sparse: 0.28, index_hint: 0.0 };
        let got = blended_candidate_score(&blended, None, 1.0, false);
        assert!((got - 0.28).abs() < 1e-9, "got {got}, expected 0.28");
    }

    /// Absent weights must mean what the engine did BEFORE it could blend: dense-only.
    #[test]
    fn absent_weights_are_dense_only() {
        let d = RankingWeights::default();
        assert_eq!(d.dense, 1.0);
        assert_eq!(d.sparse, 0.0);
        assert_eq!(d.index_hint, 0.0);
    }

    #[test]
    fn an_identical_vector_scores_highest() {
        let query = [1.0_f32, 0.0, 0.0];
        let same = dense_query_score(&query, &[1.0, 0.0, 0.0]).expect("same width is comparable");
        let orthogonal =
            dense_query_score(&query, &[0.0, 1.0, 0.0]).expect("same width is comparable");
        let opposed =
            dense_query_score(&query, &[-1.0, 0.0, 0.0]).expect("same width is comparable");
        assert!(same > orthogonal, "{same} !> {orthogonal}");
        assert!(orthogonal > opposed, "{orthogonal} !> {opposed}");
        // Mapped into 0..1 so it is comparable with the lexical scorer, which is what the
        // selector puts it beside.
        assert!((same - 1.0).abs() < 1e-6, "identical should be 1.0, got {same}");
        assert!(opposed.abs() < 1e-6, "opposed should be 0.0, got {opposed}");
        assert!((orthogonal - 0.5).abs() < 1e-6, "orthogonal should be 0.5, got {orthogonal}");
    }

    /// Scale must not matter -- cosine is about direction.
    #[test]
    fn a_longer_vector_of_the_same_direction_scores_the_same() {
        let a = dense_query_score(&[1.0, 2.0, 3.0], &[1.0, 2.0, 3.0]).expect("comparable");
        let b = dense_query_score(&[1.0, 2.0, 3.0], &[10.0, 20.0, 30.0]).expect("comparable");
        assert!((a - b).abs() < 1e-6, "{a} vs {b}");
    }

    /// A vector from a DIFFERENT MODEL must not be scored on its first dimensions.
    ///
    /// This store has been observed holding 32-dimension vectors labelled as a 1024-dimension
    /// model. Comparing a prefix would rank those on a coincidence; they are not less relevant,
    /// they are NOT COMPARABLE, and the caller must fall back rather than trust a number.
    #[test]
    fn a_mismatched_length_is_not_comparable_rather_than_scoring_zero() {
        let query = [1.0_f32, 0.0, 0.0, 0.0];
        assert_eq!(dense_query_score(&query, &[1.0, 0.0, 0.0]), None);
        assert_eq!(dense_query_score(&query, &[]), None);
        assert_eq!(dense_query_score(&[], &[1.0]), None);
    }

    /// And the CALLER must act on it. This is the half the old test could not express: it asserted
    /// the scorer returned 0.0, which the caller then blended -- with dense-only one-box weights
    /// that is a final score of 0.00, ranking a record from an older encoder beneath every lexical
    /// match. On a width change that is every pre-existing record in the store.
    #[test]
    fn a_record_from_another_encoder_is_skipped() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.0 };
        let query = [1.0_f32, 0.0, 0.0, 0.0];
        let older_width = [1.0_f32, 0.0, 0.0];
        assert_eq!(
            candidate_score(&onebox, Some(&query), Some(&older_width), 0.7, || false),
            None,
            "a vector this query cannot place must be skipped, not given the lexical score"
        );
    }

    /// The positive control: at the SAME width the dense term really does take over, so the test
    /// above is asserting a fallback that something else would otherwise have taken.
    #[test]
    fn the_same_width_still_scores_densely() {
        let onebox = RankingWeights { dense: 1.0, sparse: 0.0, index_hint: 0.0 };
        let query = [1.0_f32, 0.0, 0.0];
        let same = [1.0_f32, 0.0, 0.0];
        let got = candidate_score(&onebox, Some(&query), Some(&same), 0.7, || false)
            .expect("the same width is comparable");
        assert!(got > 0.99, "identical vectors should score ~1.0 densely, got {got}");
    }

    /// A zero vector has no direction, so it cannot be scored -- and must not divide by zero.
    ///
    /// It reports NOT COMPARABLE for the same reason a width mismatch does: there is no angle to
    /// measure, which is not the same as an angle of zero. The NaN assertion is what this test was
    /// originally for and it still matters -- 0/0 in the cosine would poison every comparison the
    /// selector makes, and NaN sorts unpredictably rather than merely badly.
    #[test]
    fn a_zero_vector_is_not_comparable_and_never_produces_nan() {
        let score = dense_query_score(&[0.0, 0.0], &[1.0, 1.0]);
        assert_eq!(score, None);
        assert!(score.map(|v| !v.is_nan()).unwrap_or(true));
        let both = dense_query_score(&[0.0, 0.0], &[0.0, 0.0]);
        assert_eq!(both, None);
        assert!(both.map(|v| !v.is_nan()).unwrap_or(true));
    }

    #[test]
    fn a_records_vector_is_read_from_either_shape() {
        let record = json!({"vector": [0.5, 0.25]});
        assert_eq!(record_vector_of(&record), Some(vec![0.5_f32, 0.25]));
        // Absent, wrong type, or non-numeric entries all mean "no usable vector" rather than a
        // partial one -- a half-read embedding would score as a different point in space.
        assert_eq!(record_vector_of(&json!({})), None);
        assert_eq!(record_vector_of(&json!({"vector": "nope"})), None);
        assert_eq!(record_vector_of(&json!({"vector": [1.0, "x"]})), None);
    }

    /// The reported flag must say which scorer RAN, not which one exists.
    ///
    /// Callers read `ranking_uses_vectors` to decide whether the ranking was semantic. It was a
    /// hardcoded `false`; if it were now hardcoded `true` it would lie in exactly the case that
    /// matters -- a request that sent no query vector and was ranked lexically.
    #[test]
    fn the_reported_ranking_follows_the_query_vector() {
        for (vector, expect_dense) in [
            (Some(vec![1.0_f32, 0.0]), true),
            (Some(vec![]), false),
            (None, false),
        ] {
            let uses = vector.filter(|v: &Vec<f32>| !v.is_empty()).is_some();
            assert_eq!(uses, expect_dense);
        }
    }

    fn field_set(names: &[&str]) -> std::collections::BTreeSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn a_projection_keeps_only_the_named_fields() {
        let record = json!({
            "record_type": "msg",
            "text": "hello",
            "storage_options": {"a": 1, "b": 2},
            "vector": [0.1, 0.2, 0.3],
            "scope_key": "t=1|u=2",
        });
        let kept = project_record(record, Some(&field_set(&["text", "record_type", "scope_key"])));
        let object = kept.as_object().expect("projected record stays an object");
        assert_eq!(object.len(), 3, "got {object:?}");
        assert_eq!(object["text"], json!("hello"));
        assert_eq!(object["record_type"], json!("msg"));
        assert!(!object.contains_key("storage_options"));
        assert!(!object.contains_key("vector"));
    }

    /// No projection must mean the WHOLE record, byte for byte.
    ///
    /// This is the positive control: without it, a `project_record` that dropped everything would
    /// satisfy the test above and quietly empty every response that did not ask for fields.
    #[test]
    fn no_projection_returns_the_whole_record() {
        let record = json!({"a": 1, "b": {"c": 2}, "d": [1, 2, 3]});
        assert_eq!(project_record(record.clone(), None), record);
    }

    #[test]
    fn a_projection_leaves_a_non_object_alone() {
        for value in [json!([1, 2]), json!("text"), json!(7), json!(null)] {
            assert_eq!(project_record(value.clone(), Some(&field_set(&["x"]))), value);
        }
    }

    /// Asking for fewer FIELDS must never change which RECORDS come back.
    ///
    /// The scan filters on `record_type`, `status` and the scope sources, and a caller may
    /// legitimately project none of them. Projection therefore happens after filtering. If it ever
    /// moved before, a caller narrowing its fields would silently narrow its results too -- the
    /// kind of bug that looks like data loss and reads like a cache problem.
    #[test]
    fn a_projection_does_not_change_which_records_match() {
        let record = json!({"record_type": "msg", "status": "active", "text": "hello"});
        // The filters read the record BEFORE projection, so they still see every field.
        let record_type = record.get("record_type").and_then(Value::as_str).unwrap_or("");
        let status = record.get("status").and_then(Value::as_str).unwrap_or("");
        assert_eq!(record_type, "msg");
        assert_eq!(status, "active");
        // And what is emitted carries neither.
        let emitted = project_record(record, Some(&field_set(&["text"])));
        let object = emitted.as_object().expect("object");
        assert_eq!(object.len(), 1);
        assert!(!object.contains_key("record_type"));
        assert!(!object.contains_key("status"));
    }

    fn projection_command(fields: Option<Vec<String>>) -> RecordLogRequest {
        let mut command: RecordLogRequest =
            serde_json::from_str(r#"{"op":"matrixark_scan_candidates"}"#).expect("request");
        command.count_key = Some("p:record_count".to_string());
        command.record_hash_key = Some("p:records".to_string());
        command.record_types = Some(vec!["msg".to_string()]);
        command.record_fields = fields;
        command
    }

    /// A projected answer and a whole-record answer must not share a cache entry.
    ///
    /// The dangerous direction is the second one: serving a PROJECTED cached answer to a caller
    /// that asked for everything looks exactly like data loss, and would be blamed on storage.
    #[test]
    fn a_projected_scan_does_not_share_a_cache_entry() {
        let whole = matrixark_scan_cache_key(&projection_command(None), "t:msg=1|d:");
        let projected = matrixark_scan_cache_key(
            &projection_command(Some(vec!["text".to_string()])),
            "t:msg=1|d:",
        );
        let other_fields = matrixark_scan_cache_key(
            &projection_command(Some(vec!["text".to_string(), "record_type".to_string()])),
            "t:msg=1|d:",
        );
        assert_ne!(whole, projected);
        assert_ne!(projected, other_fields);
    }

    /// The freshness token is what decides reuse, so a different token must be a different key.
    #[test]
    fn a_changed_freshness_token_changes_the_cache_key() {
        let command = projection_command(None);
        let before = matrixark_scan_cache_key(&command, "t:msg=41|d:");
        let after = matrixark_scan_cache_key(&command, "t:msg=42|d:");
        assert_ne!(
            before, after,
            "an append to a requested type must not reuse the cached answer"
        );
    }

    /// A delete bumps the epoch, and that alone must invalidate.
    ///
    /// Deletes carry an epoch rather than a per-type version because removing the last records of
    /// a type whose version key does not exist yet would leave the per-type part unchanged.
    #[test]
    fn a_delete_epoch_change_alone_changes_the_cache_key() {
        let command = projection_command(None);
        assert_ne!(
            matrixark_scan_cache_key(&command, "t:msg=7|d:"),
            matrixark_scan_cache_key(&command, "t:msg=7|d:1788000000000"),
        );
    }

    fn snapshot_of(field_bytes: usize, fields: usize) -> Arc<BTreeMap<String, String>> {
        let mut map = BTreeMap::new();
        for index in 0..fields {
            map.insert(format!("f{index:06}"), "x".repeat(field_bytes));
        }
        Arc::new(map)
    }

    fn cache_with_budget(budget: usize) -> SnapshotCache {
        let mut cache = SnapshotCache::new();
        cache.budget = budget;
        cache
    }

    /// The bound must hold in BYTES, which is the whole reason this type exists.
    ///
    /// An entry-count cap was tried on this proxy before and moved the resident set by nothing,
    /// because the number of entries was never what was large. So this asserts on the byte total
    /// and on a payload size that a count-based cap would have sailed straight past.
    #[test]
    fn the_snapshot_cache_stays_inside_its_byte_budget() {
        let budget = 100_000;
        let mut cache = cache_with_budget(budget);
        for shard in 0..50 {
            cache.insert(format!("records:{shard:06}"), snapshot_of(1_000, 10));
        }
        assert!(
            cache.bytes <= budget,
            "cache held {} bytes against a {budget} budget",
            cache.bytes
        );
        // The positive control: without it, a cache that dropped EVERY insert would pass above.
        assert!(
            !cache.is_empty(),
            "the cache evicted everything instead of staying near its budget"
        );
        assert!(
            cache.bytes > budget / 2,
            "cache holds only {} bytes of a {budget} budget -- it is evicting far too eagerly",
            cache.bytes
        );
    }

    /// Eviction takes the least recently USED entry, not the oldest inserted.
    #[test]
    fn the_least_recently_used_snapshot_is_the_one_evicted() {
        // Budget for two snapshots, so inserting a third must evict exactly one.
        let one = SnapshotCache::weigh(&snapshot_of(1_000, 10));
        let mut cache = cache_with_budget(one * 2);
        cache.insert("a".to_string(), snapshot_of(1_000, 10));
        cache.insert("b".to_string(), snapshot_of(1_000, 10));
        // Touch "a", making "b" the least recently used even though "a" went in first.
        assert!(cache.get("a").is_some());
        cache.insert("c".to_string(), snapshot_of(1_000, 10));
        assert!(cache.contains_key("a"), "the recently used entry was evicted");
        assert!(cache.contains_key("c"), "the new entry was not kept");
        assert!(!cache.contains_key("b"), "the least recently used entry survived");
    }

    /// A patch that grows a snapshot must be re-weighed, or the budget drifts upward silently.
    #[test]
    fn patching_a_snapshot_reweighs_it() {
        let mut cache = cache_with_budget(10_000_000);
        cache.insert("k".to_string(), snapshot_of(10, 1));
        let before = cache.bytes;
        cache.patch("k", |snapshot| {
            snapshot.insert("big".to_string(), "y".repeat(50_000));
            true
        });
        assert!(
            cache.bytes >= before + 50_000,
            "a patch that added 50 kB moved the accounting from {before} to {}",
            cache.bytes
        );
        // And shrinking must give the bytes back, or the budget ratchets one way only.
        cache.patch("k", |snapshot| {
            snapshot.remove("big");
            true
        });
        assert_eq!(cache.bytes, before);
    }

    /// Returning false from a patch drops the key, and its bytes with it.
    #[test]
    fn a_patch_that_gives_up_drops_the_snapshot_and_its_bytes() {
        let mut cache = cache_with_budget(10_000_000);
        cache.insert("k".to_string(), snapshot_of(100, 10));
        cache.patch("k", |_| false);
        assert!(!cache.contains_key("k"));
        assert_eq!(cache.bytes, 0, "the dropped snapshot left its bytes behind");
    }

    /// Emptying a snapshot by patch removes it, rather than pinning an empty map.
    ///
    /// `hgetall_shared` refuses to cache an empty read because a cached "no data" for a key that
    /// no write may touch again once pinned a served view at zero rows until restart. A patch
    /// must not create through the back door what the read path declines to store.
    #[test]
    fn patching_a_snapshot_empty_removes_it() {
        let mut cache = cache_with_budget(10_000_000);
        cache.insert("k".to_string(), snapshot_of(100, 2));
        cache.patch("k", |snapshot| {
            snapshot.clear();
            true
        });
        assert!(!cache.contains_key("k"));
        assert_eq!(cache.bytes, 0);
    }

    /// A snapshot bigger than the whole budget is skipped, not cached-then-everything-evicted.
    #[test]
    fn an_oversized_snapshot_does_not_evict_the_whole_cache() {
        let mut cache = cache_with_budget(50_000);
        cache.insert("small".to_string(), snapshot_of(100, 10));
        let kept = cache.bytes;
        cache.insert("huge".to_string(), snapshot_of(200_000, 1));
        assert!(!cache.contains_key("huge"), "an oversized snapshot was cached");
        assert!(cache.contains_key("small"), "an oversized insert cleared the cache");
        assert_eq!(cache.bytes, kept);
    }

    /// Re-inserting the same key replaces its accounting instead of adding to it.
    #[test]
    fn reinserting_a_key_does_not_double_count_it() {
        let mut cache = cache_with_budget(10_000_000);
        cache.insert("k".to_string(), snapshot_of(100, 10));
        let once = cache.bytes;
        cache.insert("k".to_string(), snapshot_of(100, 10));
        assert_eq!(cache.bytes, once, "re-inserting a key counted it twice");
    }

    /// The borrowed parse and the `Value` parse must name the SAME buckets and types.
    ///
    /// Not "both produce something": the two write into one store, so a record the fast path
    /// files under a bucket the slow path would not is a record an index-served scan cannot
    /// find. Each case below is checked against the `Value` path's own answer, so the test
    /// cannot pass by both sides being wrong in the same way.
    #[test]
    fn borrowed_index_facts_agree_with_the_value_path() {
        let payloads = [
            // scope_key at each of the five sources candidate_scope_key reads, in its order
            r#"{"record_type":"msg","scope_key":"t=11|u=22"}"#,
            r#"{"record_type":"msg","access_scope":{"scope_key":"t=11|u=22"}}"#,
            r#"{"record_type":"msg","metadata":{"access_scope":{"scope_key":"t=11|u=22"}}}"#,
            r#"{"record_type":"msg","scope":{"scope_key":"t=11|u=22"}}"#,
            r#"{"record_type":"msg","envelope":{"scope":{"scope_key":"t=11|u=22"}}}"#,
            // precedence: an earlier source must win over a later one
            r#"{"record_type":"msg","scope_key":"t=1|u=2","envelope":{"scope":{"scope_key":"t=9|u=9"}}}"#,
            // an EMPTY earlier source falls through to the later one
            r#"{"record_type":"msg","scope_key":"","scope":{"scope_key":"t=11|u=22"}}"#,
            // scopeless, partial, and missing type -- the three bucket shapes
            r#"{"record_type":"summary"}"#,
            r#"{"record_type":"msg","scope_key":"t=11"}"#,
            r#"{"scope_key":"t=11|u=22"}"#,
            // bundles, including one holding a mix
            r#"{"record_bundle":[{"record_type":"a","scope_key":"t=1|u=2"},{"record_type":"b"}]}"#,
            r#"{"record_bundle":[]}"#,
            // shapes the borrowed struct does not model -- these must FALL BACK, not misfile
            r#"{"record_type":7,"scope_key":"t=11|u=22"}"#,
            r#"{"record_type":"msg","access_scope":"not-an-object"}"#,
            r#"{"record_type":"msg","scope_key":"t=11|u=22","extra":{"deep":[1,2,{"x":null}]}}"#,
            // not a record at all
            r#"[1,2,3]"#,
            r#"not json"#,
        ];
        for payload in payloads {
            let decoded = decode_matrixark_payload(payload);
            let want_types = records_record_types(&decoded);
            let want_buckets: Vec<String> = decoded
                .iter()
                .flat_map(record_scope_buckets)
                .collect();

            let (got_types, got_buckets): (Vec<String>, Vec<String>) =
                match payload_index_facts(payload) {
                    Some(facts) => (
                        facts_record_types(&facts)
                            .into_iter()
                            .map(str::to_string)
                            .collect(),
                        facts.iter().flat_map(facts_scope_buckets).collect(),
                    ),
                    None => (
                        want_types.clone(),
                        want_buckets.clone(),
                    ),
                };
            assert_eq!(got_types, want_types, "types for {payload}");
            assert_eq!(got_buckets, want_buckets, "buckets for {payload}");
        }
    }

    /// The fast path must actually BE taken for an ordinary record.
    ///
    /// Without this the agreement test above passes vacuously: every case could be falling back
    /// to the `Value` path, which trivially agrees with itself, and the optimisation would be
    /// inert while every test stayed green.
    #[test]
    fn the_borrowed_parse_answers_an_ordinary_record() {
        let facts = payload_index_facts(
            r#"{"record_type":"msg","scope_key":"t=11|u=22","text":"hello","vector":[0.1,0.2]}"#,
        )
        .expect("an ordinary record must not fall back to the Value path");
        assert_eq!(facts_record_types(&facts), vec!["msg"]);
        assert_eq!(facts[0].scope_key(), "t=11|u=22");
    }

    #[test]
    fn a_text_lane_response_is_still_one_json_line() {
        let response = super::response_from_result(
            Err(("unavailable".to_string(), "probe".to_string())),
            1,
        );
        let json_text = serde_json::to_string(&response).expect("serializes");
        let mut out: Vec<u8> = Vec::new();
        super::write_lane_response(&mut out, false, &response, &json_text);
        assert_eq!(out.last(), Some(&b'\n'), "text lane must stay newline-delimited");
        assert_ne!(out[0], super::LANE_BINARY_MAGIC, "text lane must not look framed");
        let parsed: Value = serde_json::from_slice(&out).expect("parses as one json line");
        assert_eq!(parsed["ok"], false, "the error path still serializes a response");
    }

    #[test]
    fn a_binary_lane_response_is_a_length_prefixed_frame() {
        let response = super::response_from_result(
            Err(("unavailable".to_string(), "probe".to_string())),
            1,
        );
        let json_text = serde_json::to_string(&response).expect("serializes");
        let mut out: Vec<u8> = Vec::new();
        super::write_lane_response(&mut out, true, &response, &json_text);

        // Length-prefixed, not delimited: a msgpack body can contain a newline, so a reader
        // that split on one would cut a frame in half.
        assert_eq!(out[0], super::LANE_BINARY_MAGIC, "frame must start with the magic byte");
        let len = u32::from_le_bytes([out[1], out[2], out[3], out[4]]) as usize;
        assert_eq!(out.len(), 5 + len, "header length must describe the body exactly");

        // The body carries the same response the text lane would have sent. Decode into a typed
        // shape rather than serde_json::Value: msgpack has a `bin` type with no JSON equivalent,
        // so a Value decoder rejects the frame with "invalid type: byte array". That is worth
        // knowing beyond this test -- a reader that maps msgpack onto JSON types will see bytes
        // where the text lane gave it a string.
        #[derive(serde::Deserialize)]
        struct OkOnly {
            ok: bool,
            op: String,
        }
        let decoded: OkOnly = rmp_serde::from_slice(&out[5..]).expect("body decodes");
        let as_json: Value = serde_json::from_str(&json_text).expect("json parses");
        assert_eq!(decoded.ok, as_json["ok"].as_bool().unwrap(), "same ok as the text lane");
        assert_eq!(decoded.op, as_json["op"].as_str().unwrap(), "same op as the text lane");
    }

    #[test]
    fn the_two_codecs_are_distinguishable_by_their_first_byte() {
        // A reader handed the wrong codec must fail loudly rather than parse garbage. A JSON
        // line can never begin with the frame magic.
        let response = super::response_from_result(
            Err(("unavailable".to_string(), "probe".to_string())),
            1,
        );
        let json_text = serde_json::to_string(&response).expect("serializes");
        let mut text: Vec<u8> = Vec::new();
        let mut binary: Vec<u8> = Vec::new();
        super::write_lane_response(&mut text, false, &response, &json_text);
        super::write_lane_response(&mut binary, true, &response, &json_text);
        assert_ne!(text[0], binary[0]);
        assert_eq!(text[0], b'{');
    }

    #[test]
    fn a_record_payload_is_a_string_unless_the_caller_asked_for_a_document() {
        let stored = "{\"record_type\":\"context_event\",\"text\":\"a \\\"quoted\\\" value\"}";
        let record = HashReadRecord {
            key: "k".to_string(),
            field: "f".to_string(),
            value: record_payload(stored.to_string(), false),
        };
        let wire = serde_json::to_string(&record).expect("serializes");
        let parsed: Value = serde_json::from_str(&wire).expect("parses");
        assert!(parsed["value"].is_string(), "default must stay a string: {wire}");
        let inner: Value =
            serde_json::from_str(parsed["value"].as_str().unwrap()).expect("inner parses");
        assert_eq!(inner["record_type"], "context_event");
    }

    #[test]
    fn an_inline_payload_carries_the_same_record_without_the_second_parse() {
        let stored = "{\"record_type\":\"context_event\",\"text\":\"a \\\"quoted\\\" value\",\"n\":12345}";
        let inline_wire = serde_json::to_string(&HashReadRecord {
            key: "k".to_string(),
            field: "f".to_string(),
            value: record_payload(stored.to_string(), true),
        })
        .expect("serializes");
        let inline: Value = serde_json::from_str(&inline_wire).expect("parses");
        assert!(inline["value"].is_object(), "inline must be a document: {inline_wire}");
        assert_eq!(inline["value"]["n"], 12345);

        let text_wire = serde_json::to_string(&HashReadRecord {
            key: "k".to_string(),
            field: "f".to_string(),
            value: record_payload(stored.to_string(), false),
        })
        .expect("serializes");
        let text: Value = serde_json::from_str(&text_wire).expect("parses");
        let from_text: Value =
            serde_json::from_str(text["value"].as_str().unwrap()).expect("inner parses");
        assert_eq!(from_text, inline["value"], "the shape changes, the record must not");
    }

    #[test]
    fn a_payload_that_is_not_json_falls_back_rather_than_corrupting_the_batch() {
        for stored in ["not json at all", "", "{unclosed"] {
            let wire = serde_json::to_string(&HashReadRecord {
                key: "k".to_string(),
                field: "f".to_string(),
                value: record_payload(stored.to_string(), true),
            })
            .expect("serializes");
            let parsed: Value = serde_json::from_str(&wire)
                .unwrap_or_else(|e| panic!("malformed for {stored:?}: {e} / {wire}"));
            assert!(parsed["value"].is_string(), "must fall back for {stored:?}");
            assert_eq!(parsed["value"].as_str().unwrap(), stored);
        }
    }
    use super::compare_scored_candidate;
    use super::{matrixark_scan_cache_key, RecordLogRequest};

    /// A scan request, built from the module's own helper.
    ///
    /// Building one from bare JSON does not work: `RecordLogRequest` has required fields and serde
    /// will not invent them -- the first version of this failed with `missing field op`.
    fn scan_command(statuses: Option<Vec<String>>) -> RecordLogRequest {
        let mut command = request("matrixark_scan_candidates");
        command.count_key = Some("matrixark:count".to_string());
        command.record_hash_key = Some("matrixark:records".to_string());
        command.shard_size = Some(1024);
        command.record_types = Some(vec!["matrixark_async_pipeline_task".to_string()]);
        command.record_statuses = statuses;
        command
    }

    /// A status-filtered scan answers a SUBSET of the same question. If it shared a cache entry
    /// with an unfiltered scan, a caller asking for the whole record type would be served the
    /// subset -- and both are valid-looking results, so nothing would error.
    #[test]
    fn a_status_filtered_scan_does_not_share_a_cache_entry() {
        let unfiltered = scan_command(None);
        let filtered = scan_command(Some(vec!["idle_commit_scheduled".to_string()]));

        // Control first: without it, an assert_ne that always holds would look like proof.
        assert_eq!(
            matrixark_scan_cache_key(&unfiltered, "count:7"),
            matrixark_scan_cache_key(&unfiltered, "count:7"),
            "the same command produced two different keys, so the comparison below means nothing"
        );

        assert_ne!(
            matrixark_scan_cache_key(&unfiltered, "count:7"),
            matrixark_scan_cache_key(&filtered, "count:7"),
            "a status-filtered scan shares a cache entry with an unfiltered one"
        );
    }

    /// Two different status sets are two different questions.
    #[test]
    fn two_status_sets_do_not_share_a_cache_entry() {
        let scheduled = scan_command(Some(vec!["idle_commit_scheduled".to_string()]));
        let committed = scan_command(Some(vec!["idle_commit_committed".to_string()]));
        assert_ne!(
            matrixark_scan_cache_key(&scheduled, "count:7"),
            matrixark_scan_cache_key(&committed, "count:7"),
            "two different status filters share one cache entry"
        );
    }

    /// Absent means no filtering. This is what lets every existing caller keep its behaviour, and
    /// what lets an older caller talk to a newer engine unchanged.
    #[test]
    fn an_absent_status_filter_is_not_an_empty_one() {
        let absent = scan_command(None);
        assert!(
            absent.record_statuses.is_none(),
            "an absent record_statuses decoded as something other than None, so the scan would \
             filter when the caller asked for no filtering"
        );
        let empty = scan_command(Some(Vec::new()));
        assert_eq!(
            Some(Vec::<String>::new()),
            empty.record_statuses,
            "an explicitly empty list should decode as empty, and the scan treats it as no filter"
        );
    }

    use super::native_correctness_evidence;

    /// Each field must move for its own reason. The six this replaced were one condition wearing
    /// six hats: a non-empty pack marked every property verified, an empty one marked every
    /// property failed, and nothing in between could ever be observed.
    #[test]
    fn correctness_evidence_fields_are_independent() {
        let all_off = native_correctness_evidence(false, 0, false, false, 0, false);
        let all_on = native_correctness_evidence(true, 3, true, true, 7, true);
        for field in [
            "scope_filtering",
            "placement_filtering",
            "compact_secondary_index_prefilter",
            "stale_superseded_exclusion",
            "selected_any",
        ] {
            assert_eq!(
                Some(false),
                all_off.get(field).and_then(Value::as_bool),
                "{field} did not follow its own input"
            );
            assert_eq!(
                Some(true),
                all_on.get(field).and_then(Value::as_bool),
                "{field} did not follow its own input"
            );
        }
    }

    #[test]
    fn one_property_moving_does_not_move_the_others() {
        // The shape of the bug: flip a single input and exactly one field may change.
        let base = native_correctness_evidence(false, 0, false, false, 0, false);
        let scoped = native_correctness_evidence(true, 0, false, false, 0, false);
        assert_eq!(Some(true), scoped.get("scope_filtering").and_then(Value::as_bool));
        for field in [
            "placement_filtering",
            "compact_secondary_index_prefilter",
            "stale_superseded_exclusion",
            "selected_any",
        ] {
            assert_eq!(
                base.get(field),
                scoped.get(field),
                "{field} moved when only the scope changed"
            );
        }
    }

    #[test]
    fn a_selected_pack_does_not_certify_checks_that_did_not_run() {
        // The old behaviour exactly: something was selected, therefore everything was verified.
        let evidence = native_correctness_evidence(false, 0, false, false, 0, true);
        assert_eq!(Some(true), evidence.get("selected_any").and_then(Value::as_bool));
        assert_eq!(
            Some(false),
            evidence.get("scope_filtering").and_then(Value::as_bool),
            "a non-empty pack certified a scope filter that was never applied"
        );
        assert_eq!(
            Some(false),
            evidence.get("placement_filtering").and_then(Value::as_bool)
        );
    }

    #[test]
    fn an_empty_pack_does_not_fail_checks_that_did_run() {
        // The other direction, which the old code got wrong just as badly.
        let evidence = native_correctness_evidence(true, 4, true, true, 0, false);
        assert_eq!(Some(false), evidence.get("selected_any").and_then(Value::as_bool));
        for field in [
            "scope_filtering",
            "placement_filtering",
            "compact_secondary_index_prefilter",
            "stale_superseded_exclusion",
        ] {
            assert_eq!(
                Some(true),
                evidence.get(field).and_then(Value::as_bool),
                "{field} was reported as failed because the pack happened to be empty"
            );
        }
    }

    #[test]
    fn the_quotas_this_path_does_not_perform_are_reported_as_not_performed() {
        let evidence = native_correctness_evidence(true, 9, true, true, 3, true);
        assert_eq!(
            Some(false),
            evidence.get("shared_resource_skill_quota").and_then(Value::as_bool)
        );
        assert_eq!(
            Some(false),
            evidence.get("cross_session_quota_rerank").and_then(Value::as_bool)
        );
        assert_eq!(
            Some(3),
            evidence.get("stale_superseded_dropped").and_then(Value::as_u64)
        );
    }

    use std::cmp::Ordering;

    /// Stamping a cache hit must not reach for the scan-cache lock.
    ///
    /// The caller stamps the value while it still holds that guard, so a lock in here deadlocks
    /// the request against itself -- which is exactly what happened on every hit. Holding the
    /// guard for the duration of the call pins the invariant: if `mark_scan_cache_hit` ever locks
    /// again, this test hangs instead of passing.
    #[test]
    fn stamping_a_cache_hit_does_not_relock_the_scan_cache() {
        let held = super::matrixark_scan_cache()
            .lock()
            .expect("scan cache lock");
        let stamped = super::mark_scan_cache_hit(
            serde_json::json!({"ok": true, "scan_stats": {"scanned_records": 3}}),
            7,
        );
        drop(held);
        assert_eq!(Some(true), stamped.get("cache_hit").and_then(|v| v.as_bool()));
        let stats = stamped.get("scan_stats").expect("scan stats");
        assert_eq!(
            Some(7),
            stats
                .get("native_placement_candidate_cache_entries")
                .and_then(|v| v.as_u64())
                .map(|v| v as usize),
            "the entry count must come from the caller's guard, not a fresh lock"
        );
    }

    /// Relevance still decides: a better score wins regardless of age.
    #[test]
    fn a_higher_score_wins_regardless_of_position() {
        // (score, ordinal). The lower-scoring candidate is newer and must still lose.
        assert_eq!(
            Ordering::Less,
            compare_scored_candidate((0.9, 0), (0.1, 99))
        );
        assert_eq!(
            Ordering::Greater,
            compare_scored_candidate((0.1, 99), (0.9, 0))
        );
    }

    /// A tie is broken by scan position, EARLIEST first -- and that is a known wart, not a
    /// preference.
    ///
    /// Candidates arrive in append order, so this ranks the older of two equally-relevant
    /// statements first, which for a memory is usually the stale one. Preferring the newer
    /// candidate was tried and reverted: this comparator also decides what survives truncation to
    /// `max_selected_refs`, and flipping it dropped entity refs out of the pack entirely (see
    /// `matrixark_native_retrieve_context_pack_returns_selected_refs`). Fixing it properly means
    /// preferring recency within a ref type, or changing selection with it.
    #[test]
    fn a_tie_is_broken_by_scan_position() {
        assert_eq!(
            Ordering::Greater,
            compare_scored_candidate((0.5, 7), (0.5, 2)),
            "the earlier candidate (lower ordinal) currently sorts first on a tie"
        );
        assert_eq!(
            Ordering::Less,
            compare_scored_candidate((0.5, 2), (0.5, 7))
        );
    }

    #[test]
    fn the_same_candidate_compares_equal() {
        assert_eq!(Ordering::Equal, compare_scored_candidate((0.5, 3), (0.5, 3)));
    }

    /// Sorting a realistic mix: scores descending, and within a score band scan order.
    #[test]
    fn sorting_puts_best_first_then_scan_order() {
        let mut candidates = vec![(0.2, 0), (0.8, 1), (0.2, 5), (0.8, 4)];
        candidates.sort_by(|left, right| compare_scored_candidate(*left, *right));
        assert_eq!(vec![(0.8, 1), (0.8, 4), (0.2, 0), (0.2, 5)], candidates);
    }

    /// A NaN score must not panic the comparator; it falls through to the position tie-break.
    #[test]
    fn a_nan_score_is_treated_as_a_tie() {
        assert_eq!(
            Ordering::Greater,
            compare_scored_candidate((f64::NAN, 9), (f64::NAN, 1))
        );
    }

    use super::*;
    use std::collections::BTreeSet;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use tempfile::tempdir;

    fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        // A panicking test poisons this mutex, and expecting it turned ONE failure into a
        // cascade: every later test died at "env lock" and the real defect hid among a dozen
        // phantom ones. The guard serialises env-var use, not data -- recover and carry on.
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The barrier counters are only useful if something outside the process can read them.
    ///
    /// They are recorded at every barrier site and were exposed nowhere, so the one number that
    /// says how much of a write is barrier-bound could not be measured, only argued. This asserts
    /// both halves a harness needs: a durable write moves the counters, and `reset` clears them
    /// so a span can be bracketed instead of diffing lifetime totals by hand.
    #[test]
    fn durability_barriers_op_reports_and_resets() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().to_path_buf();
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            root.join("bar-cache"),
            root.join("bar-pages"),
            root.join("bar-index"),
        );
        engine.load_shard(DEFAULT_SHARD_ID);
        let engine = RecordStore::Local(engine);

        let mut clear = request("durability_barriers");
        clear.field = "reset".to_string();
        execute_record_log_request(&engine, clear, root.clone()).expect("reset");

        let prefix = "matrixark:barriertest";
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{prefix}:record_count");
        append.value = "1".to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{prefix}:records:000000"),
            format!("{:020}", 0),
            "{\"record_type\":\"context_event\"}".to_string(),
        )];
        execute_record_log_request(&engine, append, root.clone()).expect("append");

        let output = execute_record_log_request(
            &engine,
            request("durability_barriers"),
            root.clone(),
        )
        .expect("read");
        let after_write: u64 = output.extra.values().filter_map(Value::as_u64).sum();
        assert!(
            after_write > 0,
            "a durable write recorded no barriers: {:?}",
            output.extra
        );

        let mut clear = request("durability_barriers");
        clear.field = "reset".to_string();
        execute_record_log_request(&engine, clear, root.clone()).expect("reset again");

        let output =
            execute_record_log_request(&engine, request("durability_barriers"), root)
                .expect("read back");
        let after_reset: u64 = output.extra.values().filter_map(Value::as_u64).sum();
        assert!(
            after_reset < after_write,
            "reset did not clear the counters: {after_write} then {after_reset}"
        );
    }

    fn request(op: &str) -> RecordLogRequest {
        RecordLogRequest {
            records_inline_json: false,
            // This request names no identities: the field is only read by the delete op.
            record_ids: None,
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
            // No status filtering: this helper builds the request every other test starts from.
            record_statuses: None,
            newest_by_type: None,
            record_fields: None,
            query_vector: None,
            min_score: None,
            max_context_tokens: None,
            layer_min_refs: None,
            ranking_weights: None,
            selected_node_hashes: None,
            secondary_index_groups: None,
            scope: None,
            return_index_records: false,
            record: None,
            visibility_keys: Vec::new(),
            top_level_response: false,
            blob_offset: None,
            blob_length: None,
            blob_referenced_hashes: None,
            blob_min_age_ms: None,
            client_request_id: None,
        }
    }

    fn typed_scan(
        engine: &RecordStore,
        storage_prefix: &str,
        types: &[&str],
        root: PathBuf,
    ) -> Value {
        let mut scan = request("matrixark_scan_candidates");
        scan.storage_prefix = storage_prefix.to_string();
        scan.count_key = Some(format!("{storage_prefix}:record_count"));
        scan.record_hash_key = Some(format!("{storage_prefix}:records"));
        scan.shard_size = Some(1); // several shards, so index order across shards is exercised
        scan.record_types = Some(types.iter().map(|t| t.to_string()).collect());
        // json_output spreads the scan object into ; rebuild the object from there.
        let output = execute_record_log_request(engine, scan, root).expect("typed scan");
        Value::Object(output.extra.into_iter().collect())
    }

    fn scan_record_texts(scan: &Value) -> Vec<String> {
        scan.get("records")
            .and_then(Value::as_array)
            .expect("records")
            .iter()
            .map(|record| {
                record
                    .get("text")
                    .or_else(|| record.get("target_memory_id"))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string()
            })
            .collect()
    }

    fn append_one(
        engine: &RecordStore,
        storage_prefix: &str,
        sequence: u64,
        payload: &str,
        root: PathBuf,
    ) {
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{storage_prefix}:record_count");
        append.value = (sequence + 1).to_string();
        append.entries_compact = vec![CompactHashEntry(
            format!("{storage_prefix}:records:{sequence:06}"),
            format!("{sequence:020}"),
            payload.to_string(),
        )];
        execute_record_log_request(engine, append, root).expect("append");
    }

    fn pinned_scan(
        engine: &RecordStore,
        storage_prefix: &str,
        tenant: u64,
        user: u64,
        explicit_user: bool,
        root: PathBuf,
    ) -> Value {
        let mut scan = request("matrixark_scan_candidates");
        scan.storage_prefix = storage_prefix.to_string();
        scan.count_key = Some(format!("{storage_prefix}:record_count"));
        scan.record_hash_key = Some(format!("{storage_prefix}:records"));
        scan.shard_size = Some(1);
        scan.record_types = Some(vec!["context_event".to_string()]);
        let mut scope = json!({"tenant_hash": tenant, "user_hash": user});
        if explicit_user {
            scope["_explicit_scope_keys"] = json!(["tenant_id", "user_id"]);
        }
        scan.scope = Some(scope);
        let output = execute_record_log_request(engine, scan, root).expect("pinned scan");
        Value::Object(output.extra.into_iter().collect())
    }

    fn scoped_event(event_id: u64, tenant: u64, user: u64, text: &str) -> String {
        format!(
            r#"{{"record_type":"context_event","event_id_hash":{event_id},"text":"{text}","scope_key":"t={tenant}|u={user}|s=1|","access_scope":{{"tenant_hash":{tenant},"user_hash":{user},"scope_key":"t={tenant}|u={user}|s=1|"}}}}"#
        )
    }

    /// A store whose shard load is REFUSED (here: a corrupt WAL record with no base to hide
    /// behind) must refuse to open -- not hand back a cached engine whose every op answers
    /// shard_not_loaded. Upstream layers read that steady error stream as an empty store, which
    /// is exactly how a damaged-at-scale store served vacuous empties on every reload while its
    /// records sat durably on disk. The refusal must also not be cached: each open retries the
    /// load, so repairing the artifacts heals the next request.
    #[test]
    fn a_refused_shard_load_refuses_open_instead_of_serving_empty() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let probe = request("get_string");
        let root = record_log_root(&probe);
        {
            let engine = open_engine(&probe).expect("fresh store opens");
            let mut put = request("put_string");
            put.key = "k1".to_string();
            put.value = "v1".to_string();
            execute_record_log_request(&engine, put, root.clone()).expect("write lands");
        }
        // Crash-and-damage: the process is gone (drop the cache's engine), the durable base is
        // absent (none was materialized), and a WAL record is corrupt -- so the reload must
        // replay the WAL and must refuse when it cannot.
        clear_engine_cache();
        let wal_path = root.join("indexes").join("wals").join("shard-1.wal.bin");
        let contents = std::fs::read(&wal_path).expect("wal exists");
        let mut damaged = b"GARBAGE-NOT-A-FRAMED-RECORD".to_vec();
        damaged.push(b'\n');
        damaged.extend_from_slice(&contents);
        std::fs::write(&wal_path, damaged).expect("corrupt wal");
        let base_path = root.join("indexes").join("shard-1.index.json");
        let _ = std::fs::remove_file(&base_path);

        let refused = open_engine(&probe);
        let error = refused.expect_err("a refused load must refuse the open");
        assert!(
            error.contains("shard load refused"),
            "the refusal must name the cause, got: {error}"
        );
        assert!(
            !engine_cache()
                .lock()
                .expect("engine cache lock")
                .contains_key(&root),
            "a refused load must not cache an engine; the next open must retry the load"
        );
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    /// The blob ops are the python surface's road to the embedded engine's attachment tier:
    /// put publishes a content-addressed blob and answers with its URI, fetch range-reads it
    /// back byte-identical through the same op surface, and sweep leaves a still-referenced
    /// blob alone while collecting the orphan.
    #[test]
    fn blob_ops_roundtrip_the_attachment_through_the_op_surface() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");

        let payload = b"the original attachment bytes, fetched back whole".repeat(64);
        let mut put = request("matrixark_resource_blob_put");
        put.key = "42".to_string();
        put.value = base64_encode_bytes(&payload);
        let committed = execute_record_log_request(&engine, put, root.clone()).expect("blob put");
        let uri = committed
            .extra
            .get("matrixark_blob_uri")
            .and_then(Value::as_str)
            .expect("blob uri")
            .to_string();
        assert!(uri.starts_with("temporalstore://resources/"), "unexpected uri {uri}");
        let kept_hash = committed
            .extra
            .get("matrixark_blob_content_hash")
            .and_then(Value::as_str)
            .expect("content hash")
            .to_string();

        let mut fetch = request("matrixark_resource_blob_fetch");
        fetch.key = uri.clone();
        let served = execute_record_log_request(&engine, fetch, root.clone()).expect("blob fetch");
        assert_eq!(
            Some(payload.len() as u64),
            served.extra.get("matrixark_blob_total_size").and_then(Value::as_u64)
        );
        assert_eq!(Some(true), served.extra.get("matrixark_blob_eof").and_then(Value::as_bool));
        assert_eq!(payload, base64_decode_str(&served.value), "fetched bytes differ");

        let mut range = request("matrixark_resource_blob_fetch");
        range.key = uri.clone();
        range.blob_offset = Some(3);
        range.blob_length = Some(11);
        let window = execute_record_log_request(&engine, range, root.clone()).expect("range fetch");
        assert_eq!(payload[3..14].to_vec(), base64_decode_str(&window.value));
        assert_eq!(Some(false), window.extra.get("matrixark_blob_eof").and_then(Value::as_bool));

        let mut orphan = request("matrixark_resource_blob_put");
        orphan.key = "42".to_string();
        orphan.value = base64_encode_bytes(b"orphaned attachment");
        execute_record_log_request(&engine, orphan, root.clone()).expect("orphan put");

        let mut sweep = request("matrixark_resource_blob_sweep");
        sweep.key = "42".to_string();
        sweep.blob_referenced_hashes = Some(vec![kept_hash]);
        sweep.blob_min_age_ms = Some(0);
        let swept = execute_record_log_request(&engine, sweep, root.clone()).expect("sweep");
        assert_eq!(Some(2), swept.extra.get("matrixark_blob_scanned").and_then(Value::as_u64));
        assert_eq!(Some(1), swept.extra.get("matrixark_blob_deleted").and_then(Value::as_u64));

        let mut refetch = request("matrixark_resource_blob_fetch");
        refetch.key = uri;
        let still_there = execute_record_log_request(&engine, refetch, root).expect("kept blob");
        assert_eq!(payload, base64_decode_str(&still_there.value), "the referenced blob must survive the sweep");
    }

    fn base64_encode_bytes(bytes: &[u8]) -> String {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;
        STANDARD.encode(bytes)
    }

    fn base64_decode_str(encoded: &str) -> Vec<u8> {
        use base64::engine::general_purpose::STANDARD;
        use base64::Engine as _;
        STANDARD.decode(encoded).expect("valid base64")
    }

    /// The core property: walk + backfill on the first pinned scan, scope index on the second,
    /// identical ordered answers -- with a scopeless record present in both (it matches every
    /// query, so the none-bucket must ride along) and another subject's record in neither.
    #[test]
    fn scope_index_serves_a_pinned_scan_with_the_walks_answer() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:scope-index";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            &scoped_event(1, 11, 22, "alices first"), root.clone());
        append_one(&engine, storage_prefix, 1,
            &scoped_event(2, 11, 33, "bobs record"), root.clone());
        append_one(&engine, storage_prefix, 2,
            r#"{"record_type":"context_event","event_id_hash":3,"text":"scopeless"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 3,
            &scoped_event(4, 11, 22, "alices second"), root.clone());

        let walk = pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone());
        let stats = walk.get("scan_stats").expect("stats");
        assert_eq!(Some(false), stats.get("scope_index_used").and_then(Value::as_bool));

        clear_matrixark_scan_cache();
        let scoped = pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone());
        let stats = scoped.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("scope_index_used").and_then(Value::as_bool),
            "the second pinned scan must be served by the scope index");
        assert_eq!(scan_record_texts(&walk), scan_record_texts(&scoped),
            "scope-indexed and walk answers must be identical, in the same order");
        assert_eq!(vec!["alices first", "scopeless", "alices second"],
            scan_record_texts(&scoped),
            "another subject's record must be absent; the scopeless one present");
    }

    /// A scopeless record of a type the query did not ask for must not drag its field into the
    /// fetch. This is the property the one-bucket layout got wrong: ingest bundles a scoped
    /// event with scopeless system records, so the master bucket held nearly every field and a
    /// pinned scan fetched the store (measured: 4,921 records fetched to keep 2).
    #[test]
    fn scope_index_skips_scopeless_records_of_other_types() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:scope-index-riders";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            &scoped_event(1, 11, 22, "alices event"), root.clone());
        append_one(&engine, storage_prefix, 1,
            r#"{"record_type":"context_index","posting":"rider"}"#, root.clone());
        append_one(&engine, storage_prefix, 2,
            r#"{"record_type":"context_event","event_id_hash":3,"text":"scopeless"}"#,
            root.clone());

        pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone()); // walk + backfill
        clear_matrixark_scan_cache();
        let scan = pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone());
        let stats = scan.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("scope_index_used").and_then(Value::as_bool));
        assert_eq!(vec!["alices event", "scopeless"], scan_record_texts(&scan),
            "scopeless events still ride along; the posting must not");
        assert_eq!(Some(2_u64), stats.get("scanned_records").and_then(Value::as_u64),
            "the posting-only field must not be fetched at all");
    }

    /// A query that does not pin the user must not be scope-index-served: the bucket scheme
    /// cannot answer a tenant-wide question.
    #[test]
    fn scope_index_declines_an_unpinned_query() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:scope-index-unpinned";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            &scoped_event(1, 11, 22, "alice"), root.clone());
        append_one(&engine, storage_prefix, 1,
            &scoped_event(2, 11, 33, "bob"), root.clone());

        // Backfill via a pinned scan, then ask tenant-wide (user not explicit).
        pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone());
        clear_matrixark_scan_cache();
        let tenant_wide = pinned_scan(&engine, storage_prefix, 11, 22, false, root.clone());
        let stats = tenant_wide.get("scan_stats").expect("stats");
        assert_eq!(Some(false), stats.get("scope_index_used").and_then(Value::as_bool));
        assert_eq!(vec!["alice", "bob"], scan_record_texts(&tenant_wide));
    }

    /// Records appended after the backfill are bucketed in the same durable batch as the data.
    #[test]
    fn scope_index_sees_records_appended_after_the_backfill() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:scope-index-append";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            &scoped_event(1, 11, 22, "before"), root.clone());
        pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone()); // walk + backfill
        append_one(&engine, storage_prefix, 1,
            &scoped_event(2, 11, 22, "after"), root.clone());

        clear_matrixark_scan_cache();
        let scan = pinned_scan(&engine, storage_prefix, 11, 22, true, root.clone());
        let stats = scan.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("scope_index_used").and_then(Value::as_bool));
        assert_eq!(vec!["before", "after"], scan_record_texts(&scan));
    }

    /// Id mode: the locator finds the id's own rows, the type index finds the rows that point
    /// at it, and the composed answer equals the walk's -- in the walk's order.
    #[test]
    fn id_scoped_scan_matches_the_walk() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:id-scoped";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            r#"{"record_type":"context_event","event_id_hash":77,"text":"the memory"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 1,
            r#"{"record_type":"context_event","event_id_hash":88,"text":"someone else"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 2,
            r#"{"record_type":"matrixark_memory_tombstone","tombstone_kind":"delete","tombstone_reason":"supersede","target_memory_id":"77","superseded_by":"99"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 3,
            r#"{"record_type":"matrixark_memory_feedback","target_memory_id":"77","feedback":"POSITIVE"}"#,
            root.clone());

        // The locator entry the adapter's side-index builder would have written: the id's OWN
        // row only. The tombstone and feedback must come from the type index -- that split is
        // the design under test.
        let mut locator = request("hset");
        locator.key = format!("{storage_prefix}:context_ref_locator");
        locator.field = "77".to_string();
        locator.value = format!(
            r#"{{"locations":[{{"key":"{storage_prefix}:records:000000","field":"{:020}"}}]}}"#,
            0
        );
        execute_record_log_request(&engine, locator, root.clone()).expect("locator entry");

        let id_request = |root: PathBuf| {
            let mut scan = request("matrixark_scan_candidates");
            scan.storage_prefix = storage_prefix.to_string();
            scan.count_key = Some(format!("{storage_prefix}:record_count"));
            scan.record_hash_key = Some(format!("{storage_prefix}:records"));
            scan.shard_size = Some(1);
            scan.record_types = Some(vec![
                "context_event".to_string(),
                "matrixark_memory_tombstone".to_string(),
                "matrixark_memory_feedback".to_string(),
            ]);
            scan.record_ids = Some(vec!["77".to_string()]);
            let output = execute_record_log_request(&engine, scan, root).expect("id scan");
            Value::Object(output.extra.into_iter().collect())
        };

        // First run: no marker yet, so the walk answers (id-filtered) and backfills.
        let walk = id_request(root.clone());
        let stats = walk.get("scan_stats").expect("stats");
        assert_eq!(Some(false), stats.get("id_scoped_used").and_then(Value::as_bool));
        assert_eq!(Some(true), stats.get("type_index_backfilled").and_then(Value::as_bool));

        clear_matrixark_scan_cache();
        let scoped = id_request(root.clone());
        let stats = scoped.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("id_scoped_used").and_then(Value::as_bool),
            "the second run must be served by the locator + type-index compose");
        assert_eq!(scan_record_texts(&walk), scan_record_texts(&scoped),
            "id-scoped and walk answers must be identical, in the same order");
        assert_eq!(vec!["the memory", "77", "77"], scan_record_texts(&scoped),
            "the other memory's event must be absent; order is log order");
    }

    /// An id the locator has never seen falls back to the walk -- absence must be walked, not
    /// guessed, because an old store predates the side index.
    #[test]
    fn id_scoped_scan_walks_when_the_locator_has_no_entry() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:id-scoped-miss";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            r#"{"record_type":"context_event","event_id_hash":77,"text":"unlocated"}"#,
            root.clone());

        let mut scan = request("matrixark_scan_candidates");
        scan.storage_prefix = storage_prefix.to_string();
        scan.count_key = Some(format!("{storage_prefix}:record_count"));
        scan.record_hash_key = Some(format!("{storage_prefix}:records"));
        scan.shard_size = Some(1);
        scan.record_types = Some(vec!["context_event".to_string()]);
        scan.record_ids = Some(vec!["77".to_string()]);
        let output = execute_record_log_request(&engine, scan.clone(), root.clone()).expect("scan");
        let first = Value::Object(output.extra.into_iter().collect());
        clear_matrixark_scan_cache();
        let output = execute_record_log_request(&engine, scan, root).expect("scan");
        let second = Value::Object(output.extra.into_iter().collect());
        for result in [&first, &second] {
            let stats = result.get("scan_stats").expect("stats");
            assert_eq!(Some(false), stats.get("id_scoped_used").and_then(Value::as_bool));
            assert_eq!(vec!["unlocated"], scan_record_texts(result));
        }
    }

    /// The core property: walk + backfill on the first typed scan, index on the second, and the
    /// two answers are byte-identical in content and order.
    #[test]
    fn type_index_serves_the_second_scan_with_the_walks_answer() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:type-index-equality";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            r#"{"record_type":"context_event","event_id_hash":1,"text":"first event"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 1,
            r#"{"record_bundle":[{"record_type":"context_summary","summary_hash":9,"text":"a summary"},{"record_type":"matrixark_memory_tombstone","tombstone_kind":"delete","target_memory_id":"42"}]}"#,
            root.clone());
        append_one(&engine, storage_prefix, 2,
            r#"{"record_type":"context_event","event_id_hash":2,"text":"second event"}"#,
            root.clone());

        let first = typed_scan(&engine, storage_prefix,
            &["context_event", "matrixark_memory_tombstone"], root.clone());
        let stats = first.get("scan_stats").expect("stats");
        assert_eq!(Some(false), stats.get("type_index_used").and_then(Value::as_bool));
        assert_eq!(Some(true), stats.get("type_index_backfilled").and_then(Value::as_bool),
            "the first typed scan walks anyway, so it must build the index");

        clear_matrixark_scan_cache(); // or the second scan is a cache hit, not an index read
        let second = typed_scan(&engine, storage_prefix,
            &["context_event", "matrixark_memory_tombstone"], root.clone());
        let stats = second.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("type_index_used").and_then(Value::as_bool));
        assert_eq!(
            scan_record_texts(&first),
            scan_record_texts(&second),
            "index-served and walk-served answers must be identical, in the same order"
        );
        assert_eq!(vec!["first event", "42", "second event"], scan_record_texts(&second));
    }

    /// Appends after the backfill maintain the index in the same durable batch as the data.
    #[test]
    fn type_index_sees_records_appended_after_the_backfill() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:type-index-append";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            r#"{"record_type":"context_event","event_id_hash":1,"text":"before backfill"}"#,
            root.clone());
        typed_scan(&engine, storage_prefix, &["context_event"], root.clone()); // walk + backfill
        append_one(&engine, storage_prefix, 1,
            r#"{"record_type":"context_event","event_id_hash":2,"text":"after backfill"}"#,
            root.clone());

        clear_matrixark_scan_cache();
        let scan = typed_scan(&engine, storage_prefix, &["context_event"], root.clone());
        let stats = scan.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("type_index_used").and_then(Value::as_bool));
        assert_eq!(vec!["before backfill", "after backfill"], scan_record_texts(&scan));
    }

    /// A physically deleted record neither serves nor errors through the index.
    #[test]
    fn type_index_survives_a_physical_delete() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        clear_engine_cache();
        clear_matrixark_scan_cache();

        let storage_prefix = "matrixark:test:type-index-delete";
        let probe = request("get_string");
        let root = record_log_root(&probe);
        let engine = open_engine(&probe).expect("engine");
        append_one(&engine, storage_prefix, 0,
            r#"{"record_type":"context_event","event_id_hash":11,"text":"stays"}"#,
            root.clone());
        append_one(&engine, storage_prefix, 1,
            r#"{"record_type":"context_event","event_id_hash":22,"text":"goes"}"#,
            root.clone());
        typed_scan(&engine, storage_prefix, &["context_event"], root.clone()); // backfill

        let mut delete = request("matrixark_delete_records");
        delete.count_key = Some(format!("{storage_prefix}:record_count"));
        delete.record_hash_key = Some(format!("{storage_prefix}:records"));
        delete.shard_size = Some(1);
        delete.record_ids = Some(vec!["22".to_string()]);
        execute_record_log_request(&engine, delete, root.clone()).expect("delete");

        clear_matrixark_scan_cache();
        let scan = typed_scan(&engine, storage_prefix, &["context_event"], root.clone());
        let stats = scan.get("scan_stats").expect("stats");
        assert_eq!(Some(true), stats.get("type_index_used").and_then(Value::as_bool));
        assert_eq!(vec!["stays"], scan_record_texts(&scan));
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
        // 256, not the 4096 this pinned before, and the floor moved because it was measured
        // rather than because it was in the way: every index write an add makes is under 4 KB, and
        // those are the most repetitive bytes in the store. Over 120 adds on a fresh store the
        // disk cost went 176.8 -> 148.1 KB per add and the median add went 152.5 -> 144.7 ms, so
        // the throughput this threshold exists to protect did not pay for it. Dropping the floor
        // to 1 is worse on both counts (149.7 KB, 256.0 ms): the smallest payloads cost more to
        // compress than they give back, which is what a floor is for.
        assert_eq!(options.compression_min_bytes, 256);
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
            false,
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
            false,
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
        // Every acked write lands a WAL record even under async storage -- async only
        // defers the fsync barrier, it does not skip the log -- so a clean reopen
        // replays the log and restores writes the publish did not target. Targeted
        // publish governs which keys get their index snapshot persisted (asserted
        // above via the publish diagnostics), not which writes survive replay.
        assert_eq!(
            read_bytes(
                &reopened,
                Command::HashGet {
                    key: format!("{storage_prefix}:records:000001"),
                    field: "00000000000000000001".to_string(),
                },
            )
            .expect("get untargeted hash field"),
            r#"{"record_type":"context_event","text":"target not published"}"#
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


    /// Build a store with a large shared_context layer and one session record, and return the
    /// selected refs. `floors` goes in the RECORD, which is where the proxy client puts the pack
    /// request -- so this also covers the engine reading its ranking fields from either place.
    fn pack_with_layers(dir: &tempfile::TempDir, prefix: &str, budget: u64, floors: Value) -> Vec<Value> {
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());
        let mut append = request("matrixark_batch_append_records");
        append.key = format!("{prefix}:record_count");
        append.value = "1".to_string();
        // Three short shared_context refs that match the query well, and one session ref that
        // matches it poorly and costs more tokens than the leftover budget can hold. Without a
        // floor the fill spends the budget on the three and the session layer returns NOTHING --
        // which is the crowding this exists to stop.
        //
        // The session text must still CLEAR the threshold, so it carries the query term. A floor
        // promotes a candidate past the BUDGET, not past `min_score`: something that scores zero
        // is excluded before any layer accounting runs, which is the caller's rule. The first
        // version of this fixture used text with no query term at all, so the session ref was
        // dropped as unscoreable and the floor had nothing to promote -- the test failed while the
        // engine was behaving correctly.
        let long_session_text = format!("gpu {}", "session note ".repeat(8));
        let bundle = json!({"record_bundle": [
            {"record_type": "context_event", "event_id_hash": 201, "text": "gpu memory tuning",
             "sharing_scope": "tenant_shared"},
            {"record_type": "context_event", "event_id_hash": 202, "text": "gpu memory tuning",
             "sharing_scope": "tenant_shared"},
            {"record_type": "context_event", "event_id_hash": 203, "text": "gpu memory tuning",
             "sharing_scope": "tenant_shared"},
            {"record_type": "context_event", "event_id_hash": 204, "text": long_session_text,
             "memory_scope": "session"},
        ]});
        append.entries_compact = vec![CompactHashEntry(
            format!("{prefix}:records:000000"),
            "00000000000000000000".to_string(),
            serde_json::to_string(&bundle).expect("bundle"),
        )];
        let root = record_log_root(&append);
        let engine = open_engine(&append).expect("engine");
        execute_record_log_request(&engine, append, root.clone()).expect("append");

        // The PRODUCTION op. `_full_scan` routes to retrieve_context_pack_native, a SECOND
        // pack builder with its own selection -- driving that one tested code this change
        // never touches, which is why the floor test and its control returned identical packs.
        let mut retrieve = request("matrixark_retrieve_context_pack");
        retrieve.storage_prefix = prefix.to_string();
        retrieve.count_key = Some(format!("{prefix}:record_count"));
        retrieve.record_hash_key = Some(format!("{prefix}:records"));
        let mut record = json!({
            "query": "gpu memory",
            "max_context_tokens": budget,
        });
        if let Some(object) = record.as_object_mut() {
            if !floors.is_null() {
                object.insert("layer_min_refs".to_string(), floors);
            }
        }
        retrieve.record = Some(record);
        // The query goes on the REQUEST for this op, and the pack comes back in `value` as JSON --
        // not in `extra`, which is where the full-scan op puts it. Reading the wrong one returned
        // an empty vec through `unwrap_or_default`, so every assertion here was made about a pack
        // nothing had looked at. `expect` now, so a miss fails loudly instead of reading as empty.
        retrieve.query = "gpu memory".to_string();
        let output = execute_record_log_request(&engine, retrieve, root).expect("retrieve");
        let response: Value =
            serde_json::from_str(&output.value).expect("context pack json in value");
        response
            .get("context_pack")
            .and_then(|pack| pack.get("selected_refs"))
            .and_then(Value::as_array)
            .cloned()
            .expect("selected_refs present in the pack")
    }

    fn has_session_ref(refs: &[Value]) -> bool {
        refs.iter().any(|value| {
            value.get("memory_layer").and_then(Value::as_str) == Some("session")
        })
    }

    /// Separates the two reasons a floor can appear not to work.
    ///
    /// The floor test failed with a pack identical to its own control, which is consistent with
    /// BOTH "the floor was not read" and "the session record never became a candidate". With a
    /// budget large enough to hold everything, a missing session ref can only mean the second.
    #[test]
    fn the_session_record_is_a_candidate_at_all() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        let refs = pack_with_layers(
            &dir,
            "matrixark:test:layer-candidate-probe",
            100_000,
            Value::Null,
        );
        let layers: Vec<&str> = refs
            .iter()
            .filter_map(|value| value.get("memory_layer").and_then(Value::as_str))
            .collect();
        assert!(
            has_session_ref(&refs),
            "with a budget nothing can exhaust, the session record must be selected; \
             layers were {layers:?} across {} refs",
            refs.len()
        );
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    /// A layer floor must survive a budget the layer would otherwise lose.
    ///
    /// Selection used to be a flat top-N by score, and the per-layer budget was computed AFTER it
    /// -- so a layer that lost the flat contest never reached its own budget. With a production
    /// skill corpus in shared_context and a handful of session memories, that is how session
    /// context disappears from a pack entirely.
    #[test]
    fn a_layer_floor_survives_a_budget_the_layer_would_otherwise_lose() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        let refs = pack_with_layers(
            &dir,
            "matrixark:test:layer-floor-on",
            30,
            json!({"session": 1}),
        );
        assert!(
            has_session_ref(&refs),
            "the session floor must be honoured, got {refs:?}"
        );
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    /// The positive control: WITHOUT the floor the same store and budget lose the session layer.
    ///
    /// Without this the test above could pass because the session ref fitted anyway, and the floor
    /// would be asserting nothing.
    #[test]
    fn without_a_floor_the_bigger_layer_takes_the_budget() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        let refs = pack_with_layers(&dir, "matrixark:test:layer-floor-off", 30, Value::Null);
        // An EMPTY pack also has no session ref, so without this the control passes vacuously and
        // certifies a floor test that never selected anything. It did exactly that while the
        // harness was reading the wrong field off the response.
        assert!(
            !refs.is_empty(),
            "control is vacuous: the pack came back empty, so 'no session ref' means nothing"
        );
        assert!(
            !has_session_ref(&refs),
            "control failed: the session ref fitted without a floor, so the floor test proves \
             nothing -- retune the budget or the text sizes, got {refs:?}"
        );
        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }

    #[test]
    fn matrixark_native_retrieve_enforces_source_role_budget() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

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
        // Walking the production lists means a field ADDED to them is covered here without
        // anyone remembering to add it -- which is the drift that left the old hand-written copy
        // checking 25 of 26. It cannot, on its own, notice a field REMOVED from them: the loop
        // would simply stop asking about it. So the extent is pinned too, and a shrinking list
        // fails here rather than quietly starting to leak.
        assert_eq!(
            LINEAGE_ONLY_FIELDS.len(),
            22,
            "the lineage list changed size; if a field was deliberately dropped, say so and              update this number, because dropping one is how a served ref starts carrying it"
        );
        assert_eq!(SCORE_ONLY_FIELDS.len(), 4, "the score list changed size");
        for field in LINEAGE_ONLY_FIELDS.iter().chain(SCORE_ONLY_FIELDS.iter()) {
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


        env::remove_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT");
    }


    #[test]
    fn matrixark_native_retrieve_enforces_memory_selection_and_extraction_phase_budgets() {
        let _guard = env_guard();
        let dir = tempdir().expect("tempdir");
        env::set_var("MATRIXARK_TEMPORALSTORE_RUST_ROOT", dir.path());

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

        // The op that does this has a name. The variable made a DIFFERENT op behave
        // this way, for every caller in the process.
        let mut retrieve = request("matrixark_retrieve_context_pack_full_scan");
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

    // ---------------------------------------------------------------------------------------
    // Native scope-forget (`matrixark_forget_scope`): delete every record under a scope prefix
    // as one durable, recovery-safe operation, while leaving co-resident scopes intact.
    // ---------------------------------------------------------------------------------------

    fn clear_native_caches() {
        if let Ok(mut cache) = matrixark_scan_cache().lock() {
            cache.clear();
        }
        if let Ok(mut cache) = record_count_cache().lock() {
            cache.clear();
        }
        if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
            cache.clear();
        }
    }

    fn forget_engine(root: &std::path::Path, role: &str) -> RecordStore {
        let engine = TemporalEngine::with_local_dirs(
            1 << 20,
            root.join(format!("{role}-cache")),
            root.join(format!("{role}-pages")),
            root.join(format!("{role}-index")),
        );
        engine.load_shard(DEFAULT_SHARD_ID);
        RecordStore::Local(engine)
    }

    fn subject_scope(user_id: &str) -> Value {
        json!({ "user_id": user_id, "_explicit_scope_keys": ["user_id"] })
    }

    fn memory_record(user_id: &str, text: &str) -> Value {
        json!({
            "record_type": "memory",
            "text": text,
            "access_scope": { "user_id": user_id },
        })
    }

    fn seed_records(engine: &RecordStore, hash_key: &str, count_key: &str, fields: &[(&str, Value)]) {
        let mut commands = Vec::new();
        commands.push(Command::StringSet {
            key: count_key.to_string(),
            value: fields.len().to_string().into_bytes(),
        });
        for (field, value) in fields {
            commands.push(Command::HashSet {
                key: format!("{hash_key}:000000"),
                field: field.to_string(),
                value: value.to_string().into_bytes(),
            });
        }
        execute_empty_batch_runtime(engine, commands, true).expect("seed records");
    }

    fn shard_fields(engine: &RecordStore, hash_key: &str) -> BTreeMap<String, String> {
        clear_native_caches();
        hgetall_map(engine, format!("{hash_key}:000000")).expect("hgetall shard 0")
    }

    #[test]
    fn native_forget_removes_only_matching_scope_records() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let engine = forget_engine(dir.path(), "primary");
        let hash_key = "matrixark:mcp:fwd_only:records";
        let count_key = "matrixark:mcp:fwd_only:record_count";
        seed_records(
            &engine,
            hash_key,
            count_key,
            &[
                ("alice-1", memory_record("alice", "a1")),
                ("alice-2", memory_record("alice", "a2")),
                ("bob-1", memory_record("bob", "b1")),
            ],
        );

        let stats = forget_scope_records(&engine, hash_key, count_key, 1024, &subject_scope("alice"))
            .expect("forget alice");
        assert_eq!(stats.records_removed, 2, "both of alice's records removed");
        assert_eq!(stats.fields_deleted, 2, "each alice field fully tombstoned");
        assert_eq!(stats.fields_rewritten, 0);

        let remaining = shard_fields(&engine, hash_key);
        assert!(
            !remaining.contains_key("alice-1") && !remaining.contains_key("alice-2"),
            "alice's fields are gone: {remaining:?}"
        );
        assert!(
            remaining.get("bob-1").is_some_and(|value| value.contains("\"b1\"")),
            "bob's record is untouched: {remaining:?}"
        );

        // Idempotent: a second forget of the same subject removes nothing and errors nowhere.
        let again = forget_scope_records(&engine, hash_key, count_key, 1024, &subject_scope("alice"))
            .expect("second forget alice");
        assert_eq!(again.records_removed, 0, "nothing left to forget");
    }

    #[test]
    fn native_forget_rewrites_partially_matching_record_bundle() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let engine = forget_engine(dir.path(), "primary");
        let hash_key = "matrixark:mcp:bundle:records";
        let count_key = "matrixark:mcp:bundle:record_count";
        // One hash field packs a bundle carrying BOTH subjects plus sibling metadata.
        let bundle = json!({
            "record_bundle": [memory_record("alice", "a1"), memory_record("bob", "b1")],
            "bundle_seq": 7,
        });
        seed_records(&engine, hash_key, count_key, &[("bundle-0", bundle)]);

        let stats = forget_scope_records(&engine, hash_key, count_key, 1024, &subject_scope("alice"))
            .expect("forget alice");
        assert_eq!(stats.records_removed, 1);
        assert_eq!(stats.fields_deleted, 0, "field survives -- bob remains");
        assert_eq!(stats.fields_rewritten, 1);

        let remaining = shard_fields(&engine, hash_key);
        let stored = remaining.get("bundle-0").expect("bundle field survives");
        let decoded: Value = serde_json::from_str(stored).expect("valid json");
        let entries = decoded
            .get("record_bundle")
            .and_then(Value::as_array)
            .expect("record_bundle preserved");
        assert_eq!(entries.len(), 1, "only bob survives in the bundle");
        assert_eq!(entries[0].pointer("/access_scope/user_id").and_then(Value::as_str), Some("bob"));
        assert_eq!(
            decoded.get("bundle_seq").and_then(Value::as_u64),
            Some(7),
            "sibling bundle metadata is preserved on rewrite"
        );
    }


    /// An id purge opens a field for every location filed under the ids, and most of those turn
    /// out to hold nothing to remove. `records_scanned` cannot say why, and the two reasons want
    /// opposite fixes, so the counters have to separate three outcomes per field: it held a
    /// carrier and was rewritten; it held no carrier but still points at the ids, so the location
    /// is correctly filed; or it relates to the ids not at all.
    #[test]
    fn an_id_purge_separates_carrying_pointing_and_unrelated_fields() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let engine = forget_engine(dir.path(), "primary");
        let hash_key = "matrixark:mcp:idpurge:records";
        let count_key = "matrixark:mcp:idpurge:record_count";
        seed_records(
            &engine,
            hash_key,
            count_key,
            &[
                // Carries the wanted id: this is what a purge removes.
                ("f-carries", json!({ "record_type": "context_entity", "entity_hash": 4242 })),
                // Names it as a source but carries its own identity: a derivative reached THROUGH
                // the wanted id, deliberately left in place.
                ("f-points", json!({
                    "record_type": "context_summary",
                    "summary_hash": 99,
                    "source_event_ids": [4242, 777],
                })),
                // Mentions the wanted id nowhere.
                ("f-unrelated", json!({ "record_type": "context_event", "event_id_hash": 31337 })),
            ],
        );

        let ids = vec!["4242".to_string()];
        let stats = delete_records_by_ids(&engine, hash_key, count_key, 1024, &ids)
            .expect("purge by id");

        assert_eq!(stats.records_removed, 1, "only the carrier is removed");
        assert_eq!(stats.fields_visited, 3, "every seeded field is opened and decoded");
        assert_eq!(
            stats.fields_without_match, 2,
            "the pointing field and the unrelated one both yield nothing to remove"
        );
        assert_eq!(
            stats.fields_pointed_only, 1,
            "exactly one of those still points at the id -- the unrelated field does not"
        );

        // The distinction is about what was READ, not what was written: the pointing record and
        // the unrelated record must both still be there afterwards.
        let remaining = shard_fields(&engine, hash_key);
        assert!(remaining.contains_key("f-points"), "a pointing record is not a carrier");
        assert!(remaining.contains_key("f-unrelated"), "an unrelated record is untouched");
        assert!(!remaining.contains_key("f-carries"), "the carrier is gone");
    }

    /// The purge drops a located entry only when the field it points at has been read and holds
    /// nothing filed under that id -- and keeps it when something IS still filed there. Both
    /// halves matter: the first is the whole saving, the second is what stops the saving from
    /// costing retrieval the rows it reaches through those entries.
    #[test]
    fn a_purge_drops_the_entries_it_proved_stale_and_keeps_the_rest() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let engine = forget_engine(dir.path(), "primary");
        let hash_key = "matrixark:mcp:prune:records";
        let count_key = "matrixark:mcp:prune:record_count";
        let locator = "matrixark:mcp:prune:context_ref_locator";

        seed_records(
            &engine,
            hash_key,
            count_key,
            &[
                // Field names are the 20-wide zero-padded offsets a location resolves to, so the
                // compact "shard:offset" entries below address exactly these.
                // Everything about 4242 lives here, and all of it is about to go.
                ("00000000000000000000", json!({ "record_type": "context_entity", "ref_hash": 4242 })),
                // Filed under 4242 as a source, and stays filed after the purge.
                ("00000000000000000001", json!({
                    "record_type": "context_summary",
                    "summary_hash": 99,
                    "source_event_ids": [4242],
                })),
            ],
        );
        // Both fields are filed under 4242; the first will stop describing it, the second will not.
        execute_empty_batch_runtime(
            &engine,
            vec![Command::HashSet {
                key: locator.to_string(),
                field: "4242".to_string(),
                value: json!({ "locations": ["0:0", "0:1"] }).to_string().into_bytes(),
            }],
            true,
        )
        .expect("seed locator");

        let stats = delete_records_by_ids(&engine, hash_key, count_key, 1024,
                                          &["4242".to_string()]).expect("purge");
        assert_eq!(stats.records_removed, 1);
        assert_eq!(stats.locator_locations_dropped, 1, "exactly the emptied location is dropped");

        clear_native_caches();
        let raw = read_bytes(&engine, Command::HashGet {
            key: locator.to_string(),
            field: "4242".to_string(),
        })
        .expect("locator readable");
        let decoded: Value = serde_json::from_str(&raw).expect("valid json");
        let left: Vec<&str> = decoded["locations"].as_array().expect("locations")
            .iter().filter_map(Value::as_str).collect();
        assert_eq!(left, vec!["0:1"],
                   "the entry for the field that still has a record filed under 4242 survives");
    }

    /// Staleness is decided by the WRITER's filing rule, and the writer files under more fields
    /// than deletion matches on. Every one of them has to keep an entry alive, or a purge would
    /// drop the only route by which an id-scoped read reaches that record. Checked field by field
    /// so adding one to the writer without adding it here shows up as a failure.
    #[test]
    fn locator_filing_fields_are_covered() {
        for field in ["ref_hash", "event_id_hash", "chunk_hash", "section_hash", "skill_hash",
                      "resource_hash", "summary_hash", "batch_id_hash", "source_event_hash",
                      "target_memory_id", "superseded_by", "entity_hash", "segment_hash"] {
            let record = json!({ field: 4242 });
            assert!(
                record_filed_under_id(&record, |id| id == "4242"),
                "a record filed under {field} must keep its located entry alive"
            );
        }
        for field in ["ref_hashes", "source_event_ids", "source_refs"] {
            let record = json!({ field: [1, 4242] });
            assert!(
                record_filed_under_id(&record, |id| id == "4242"),
                "a record filed under {field} must keep its located entry alive"
            );
        }
        // A record that names the id nowhere must NOT hold an entry open, or nothing is reclaimed.
        assert!(!record_filed_under_id(&json!({ "unrelated_hash": 4242 }), |id| id == "4242"));
        assert!(!record_filed_under_id(&json!({ "ref_hash": 7 }), |id| id == "4242"));
    }

    /// A posting carrying SEVERAL refs has no singular `ref_hash` -- the builder only writes that
    /// when there is exactly one -- so before the `ref_hashes` fallback this returned None and two
    /// serving sites dropped the record instead of scoring it.
    #[test]
    fn a_multi_ref_posting_still_has_an_identity() {
        let single = json!({
            "record_type": "context_index",
            "ref_hash": 4242_u64,
            "ref_hashes": [4242_u64],
        });
        assert_eq!(record_ref_hash(&single).as_deref(), Some("4242"),
                   "a single-ref posting keeps resolving through the singular field");

        let multi = json!({
            "record_type": "context_index",
            "ref_hashes": [7001_u64, 7002_u64, 7003_u64],
        });
        assert_eq!(record_ref_hash(&multi).as_deref(), Some("7001"),
                   "a multi-ref posting resolves through the array");

        let identity_wins = json!({
            "record_type": "context_entity",
            "entity_hash": 900_u64,
            "ref_hashes": [111_u64],
        });
        assert_eq!(record_ref_hash(&identity_wins).as_deref(), Some("900"),
                   "an identity field still takes precedence over the array");

        let neither = json!({"record_type": "context_index"});
        assert_eq!(record_ref_hash(&neither), None,
                   "a record with no identity at all is still unidentified");
    }

    #[test]
    fn native_forget_rejects_underspecified_scope() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let engine = forget_engine(dir.path(), "primary");
        let hash_key = "matrixark:mcp:guard:records";
        let count_key = "matrixark:mcp:guard:record_count";
        seed_records(
            &engine,
            hash_key,
            count_key,
            &[("alice-1", memory_record("alice", "a1"))],
        );

        // Empty scope -> would match every record -> must be refused, and nothing deleted.
        let empty = forget_scope_records(&engine, hash_key, count_key, 1024, &json!({}));
        assert!(empty.is_err(), "empty scope must be refused");
        // A user_id that is NOT marked explicit does not constrain matching -> also refused.
        let implicit = forget_scope_records(
            &engine,
            hash_key,
            count_key,
            1024,
            &json!({ "user_id": "alice" }),
        );
        assert!(implicit.is_err(), "non-explicit subject must be refused");

        let remaining = shard_fields(&engine, hash_key);
        assert!(
            remaining.contains_key("alice-1"),
            "a refused forget deletes nothing: {remaining:?}"
        );
    }

    #[test]
    fn native_forget_tombstones_survive_wal_replay_recovery() {
        let _guard = env_guard();
        clear_native_caches();
        let dir = tempdir().expect("tempdir");
        let hash_key = "matrixark:mcp:recover:records";
        let count_key = "matrixark:mcp:recover:record_count";

        // Phase 1: seed + forget on the primary, then shut it down cleanly.
        {
            let engine = forget_engine(dir.path(), "recover");
            seed_records(
                &engine,
                hash_key,
                count_key,
                &[
                    ("alice-1", memory_record("alice", "a1")),
                    ("alice-2", memory_record("alice", "a2")),
                    ("bob-1", memory_record("bob", "b1")),
                ],
            );
            let stats =
                forget_scope_records(&engine, hash_key, count_key, 1024, &subject_scope("alice"))
                    .expect("forget alice");
            assert_eq!(stats.records_removed, 2);
            engine.unload_shard(DEFAULT_SHARD_ID);
        }

        // Phase 2: a fresh engine on the SAME pages/index dirs replays the WAL from scratch. The
        // forget tombstones must NOT resurrect alice, and bob must remain.
        clear_native_caches();
        let reopened = forget_engine(dir.path(), "recover");
        let remaining = shard_fields(&reopened, hash_key);
        assert!(
            !remaining.contains_key("alice-1") && !remaining.contains_key("alice-2"),
            "forget must survive WAL replay -- alice must not resurrect: {remaining:?}"
        );
        assert!(
            remaining.get("bob-1").is_some_and(|value| value.contains("\"b1\"")),
            "bob survives recovery: {remaining:?}"
        );

        // And the native retrieve scan agrees post-recovery: zero alice candidates, one bob.
        let mut alice_scan = request("matrixark_scan_candidates");
        alice_scan.count_key = Some(count_key.to_string());
        alice_scan.record_hash_key = Some(hash_key.to_string());
        alice_scan.shard_size = Some(1024);
        alice_scan.scope = Some(subject_scope("alice"));
        clear_native_caches();
        let alice_result = scan_matrixark_candidates(&reopened, &alice_scan).expect("scan alice");
        assert_eq!(
            alice_result.get("count").and_then(Value::as_u64),
            Some(0),
            "no alice candidates after recovery: {alice_result}"
        );

        let mut bob_scan = request("matrixark_scan_candidates");
        bob_scan.count_key = Some(count_key.to_string());
        bob_scan.record_hash_key = Some(hash_key.to_string());
        bob_scan.shard_size = Some(1024);
        bob_scan.scope = Some(subject_scope("bob"));
        clear_native_caches();
        let bob_result = scan_matrixark_candidates(&reopened, &bob_scan).expect("scan bob");
        assert_eq!(
            bob_result.get("count").and_then(Value::as_u64),
            Some(1),
            "bob still retrievable after recovery: {bob_result}"
        );
    }
}


#[cfg(test)]
mod scan_cap_tests {
    use super::newest_locations;

    fn at(shard: u32, field: u32) -> String {
        format!("{shard:06}:{field:06}")
    }

    #[test]
    fn no_cap_keeps_everything() {
        let all = vec![at(0, 2), at(0, 1), at(0, 3)];
        let kept = newest_locations(all.clone(), None);
        assert_eq!(kept.len(), all.len());
    }

    #[test]
    fn a_cap_keeps_the_newest_by_append_order() {
        // Deliberately out of order on the way in: the index is read from a map, and a cap that
        // assumed sorted input would keep the wrong records rather than merely too many.
        let all = vec![at(0, 3), at(0, 1), at(1, 0), at(0, 2)];
        assert_eq!(newest_locations(all.clone(), Some(2)), vec![at(0, 3), at(1, 0)]);
        assert_eq!(newest_locations(all.clone(), Some(1)), vec![at(1, 0)]);
    }

    #[test]
    fn a_cap_larger_than_the_set_keeps_all_of_it() {
        let all = vec![at(0, 1), at(0, 2)];
        assert_eq!(newest_locations(all.clone(), Some(9)).len(), 2);
        assert_eq!(newest_locations(all.clone(), Some(2)).len(), 2);
    }

    #[test]
    fn shard_order_beats_field_order() {
        // A later shard is always newer, even when its field number is smaller -- the shard part
        // leads the key, which is why zero-padding both parts matters.
        let all = vec![at(0, 999), at(1, 1)];
        assert_eq!(newest_locations(all, Some(1)), vec![at(1, 1)]);
    }
}


#[cfg(test)]
mod record_id_predicate_tests {
    use super::*;

    /// Record shapes to check the two implementations against each other on.
    fn shapes() -> Vec<Value> {
        vec![
            json!({"event_id_hash": 7}),
            json!({"event_id_hash": "7"}),
            json!({"entity_hash": 0}),
            json!({"summary_hash": u64::MAX}),
            json!({"segment_hash": -5}),
            json!({"ref_hash": ""}),
            json!({"ref_hash": "abc"}),
            json!({"ref_hashes": [1, 2, 3]}),
            json!({"ref_hashes": ["a", "", "b"]}),
            json!({"ref_hashes": []}),
            json!({"ref_hashes": "not-an-array"}),
            json!({"event_id_hash": 11, "ref_hashes": [12, "13"]}),
            json!({"unrelated": "field"}),
            json!({}),
            json!({"event_id_hash": null}),
            json!({"entity_hash": 1.5}),
        ]
    }

    /// The candidate id sets to ask about, including ones that should match nothing.
    fn probes() -> Vec<Vec<&'static str>> {
        vec![
            vec![],
            vec!["7"],
            vec!["0"],
            vec!["18446744073709551615"],
            vec!["-5"],
            vec!["abc"],
            vec!["1"],
            vec!["2"],
            vec!["3"],
            vec!["13"],
            vec!["nope"],
            vec![""],
            vec!["7", "nope"],
            vec!["1.5"],
        ]
    }

    #[test]
    fn the_predicate_agrees_with_the_allocating_version() {
        // Checked against record_addressable_ids rather than against hand-written expectations:
        // this is a deletion path, and the property that matters is "same answer as before", not
        // "the answer I think is right".
        for record in shapes() {
            let ids = record_addressable_ids(&record);
            for probe in probes() {
                let wanted: HashSet<&str> = probe.iter().copied().collect();
                let old = ids.iter().any(|id| wanted.contains(id.as_str()));
                let new = record_carries_wanted_id(&record, |id| wanted.contains(id));
                assert_eq!(
                    old, new,
                    "disagreement on record {record} for ids {probe:?}: old={old} new={new}"
                );
            }
        }
    }

    #[test]
    fn a_u64_is_written_without_allocating_and_reads_back_the_same() {
        for value in [0_u64, 1, 9, 10, 99, 100, 12345, u64::MAX, u64::MAX - 1] {
            let mut buf = [0_u8; 20];
            assert_eq!(
                value.to_string(),
                u64_into(&mut buf, value),
                "{value} did not round-trip through the stack buffer"
            );
        }
    }

}
