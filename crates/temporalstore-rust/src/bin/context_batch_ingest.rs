// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Load-once batch ingester for local-context backfill (Claude + Codex).
//!
//! The per-event hook binary (`codex_context_hook`) loads the engine and reloads
//! the growing shard index on every process start, so bulk backfill is O(n) per
//! item -> O(n^2) overall (~45-120s/item once the store is warm). This binary
//! loads `TemporalEngine` ONCE and ingests a whole stream, ingest-only (no
//! retrieval -- retrieval is what we are warming the store *for*). It shares the
//! exact `extract_context` path, body format, source-id scheme, and session
//! index file as the live hook, so records are identical to live ingestion.
//! By default it also enables the live hook's L1 gate, so thin prompt/tool rows
//! store raw evidence plus L0 without paying the richer L1 fanout cost.
//! Set MATRIXARK_BACKFILL_RAW_FIRST=1 for a faster first pass that stores every
//! row as a raw ContextNode/ContextEvent/source index and defers summaries and
//! embeddings to a later extraction pass.
//!
//! Input: one JSON object per stdin line:
//!   {"session_id":"<sid>","event":"UserPromptSubmit","ts_ms":1780000000000,"text":"..."}
//! `event` selects role/source-kind exactly like the live hook (UserPromptSubmit
//! -> user/Chat, Stop -> assistant/UserEvent, PostToolUse -> tool/Code). `ts_ms`
//! is optional (falls back to now) and preserves temporal ordering for decay.
//!
//! Identity/scope/root come from env, mirroring the hooks:
//!   MATRIXARK_AGENT_NAME, MATRIXARK_ACCOUNT_ID, MATRIXARK_TENANT_ID,
//!   MATRIXARK_USER_ID, TEMPORALSTORE_RUST_CODEX_HOOK_ROOT
//!
//! Run once per agent (each agent has its own root), e.g.:
//!   MATRIXARK_AGENT_NAME=claude TEMPORALSTORE_RUST_CODEX_HOOK_ROOT=/tmp/...claude-hook \
//!     context_batch_ingest --agent-name claude < backfill.claude.jsonl

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temporalstore_rust::{
    ingest_extract_context, BatchExecuteRequest, Command, ContextEvent, ContextExtractRequest,
    ContextExtractedEventIndexes, ContextIngestExtractRequest, ContextModelProviderConfig,
    ContextNode, ContextSourceKind, ContextSummaryDirtyMarker, InternalContextIndex,
    TemporalEngine,
};

// Reason code stamped on embedding-dirty markers written by the raw-first bulk
// path, distinguishing "embedding deferred at bulk ingest" from a live-path embed
// failure. Purely diagnostic; the drainer treats any pending marker identically.
const EMBEDDING_DIRTY_REASON_BULK_DEFERRED: u32 = 2;
// Raw-first emits three engine commands per source: node upsert, combined
// event+source-index write, and an in-memory embedding-dirty marker. The marker
// is transient and is not WAL persisted, so non-exclusive conservative anchoring
// counts only the two durable commands.
const RAW_FIRST_DURABLE_WAL_COMMANDS_PER_SOURCE: u64 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

fn main() {
    let process_started = Instant::now();
    // Bulk backfill: defer per-record index persistence to flush_shard_index()
    // (see MATRIXARK_BULK_INGEST gate in the engine). Turns O(n^2) into O(n).
    std::env::set_var("MATRIXARK_BULK_INGEST", "1");
    // Bulk backfill writes many WAL records into an already-large live hook store.
    // Trust the WAL's verified in-process sequence cache so each append does not
    // rescan the full WAL file to find the tail.
    if std::env::var("TS_PHASE1_FLAT").is_err() {
        std::env::set_var("TS_PHASE1_FLAT", "1");
    }
    let eager_cache_warm_on_load_overridden =
        std::env::var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD").is_ok();
    if std::env::var("MATRIXARK_CONTEXT_INGEST_GATE_L1").is_err() {
        std::env::set_var("MATRIXARK_CONTEXT_INGEST_GATE_L1", "1");
    }
    let raw_first = env_bool("MATRIXARK_BACKFILL_RAW_FIRST");
    let checkpoint_only = env_bool("MATRIXARK_BACKFILL_CHECKPOINT_ONLY")
        || std::env::args().any(|arg| arg == "--checkpoint-only");
    if !eager_cache_warm_on_load_overridden {
        std::env::set_var(
            "MATRIXARK_EAGER_CACHE_WARM_ON_LOAD",
            if checkpoint_only { "1" } else { "0" },
        );
    }
    let skip_covered_sessions = env_bool("MATRIXARK_BACKFILL_SKIP_COVERED_SESSIONS");
    let exclusive_writer = std::env::var("MATRIXARK_BACKFILL_EXCLUSIVE_WRITER")
        .ok()
        .map(|value| env_value_bool(&value))
        .unwrap_or(raw_first);
    let cache_bytes: usize = std::env::var("MATRIXARK_BACKFILL_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(256 * 1024 * 1024);
    let flush_every_accepted: u64 = std::env::var("MATRIXARK_BACKFILL_FLUSH_EVERY_ACCEPTED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(if raw_first { 100_000_000 } else { 10_000 });
    let sub_batch: usize = std::env::var("MATRIXARK_BACKFILL_SUB_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v| *v > 0)
        .unwrap_or(if raw_first { 4_000 } else { 250 });
    let max_body: usize = std::env::var("MATRIXARK_BACKFILL_MAX_BODY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000);
    let agent_name = std::env::args()
        .skip(1)
        .scan(false, |flag, a| {
            if *flag {
                *flag = false;
                Some(Some(a))
            } else if a == "--agent-name" || a == "--agent" {
                *flag = true;
                Some(None)
            } else {
                Some(None)
            }
        })
        .flatten()
        .next()
        .or_else(|| std::env::var("MATRIXARK_AGENT_NAME").ok())
        .or_else(|| std::env::var("TEMPORALSTORE_AGENT_NAME").ok())
        .unwrap_or_else(|| "codex".to_string());

    let account_id =
        std::env::var("MATRIXARK_ACCOUNT_ID").unwrap_or_else(|_| "acct_codex".to_string());
    let tenant_id =
        std::env::var("MATRIXARK_TENANT_ID").unwrap_or_else(|_| "tenant_codex".to_string());
    let user_id = std::env::var("MATRIXARK_USER_ID")
        .or_else(|_| std::env::var("USER"))
        .unwrap_or_else(|_| "codex_user".to_string());
    let root = PathBuf::from(
        std::env::var("TEMPORALSTORE_RUST_CODEX_HOOK_ROOT")
            .unwrap_or_else(|_| "/tmp/temporalstore-rust-codex-hook".to_string()),
    );

    fs::create_dir_all(&root).expect("hook root should be creatable");
    // Load the engine ONCE for the whole stream.
    let load_started = Instant::now();
    let engine = TemporalEngine::with_local_dirs(
        cache_bytes,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(1);
    let load_seconds = load_started.elapsed().as_secs_f64();
    let replay_from_sequence = engine.write_ahead_log_store().stats(1).last_sequence;
    std::env::set_var(
        "MATRIXARK_BULK_INGEST_REPLAY_FROM_SEQUENCE",
        replay_from_sequence.to_string(),
    );

    // Load existing session index once; accumulate in memory; write once at the end.
    let index_path = session_index_path(&root, &agent_name);
    let mut index: SessionIndex = fs::read_to_string(&index_path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    if checkpoint_only {
        let flush_started = Instant::now();
        engine.flush_shard_index(1);
        let report = json!({
            "status": "ok",
            "mode": "checkpoint_only",
            "agent_name": agent_name,
            "root": root.display().to_string(),
            "index_path": index_path.display().to_string(),
            "seconds": process_started.elapsed().as_secs_f64(),
            "load_seconds": load_seconds,
            "flush_seconds": flush_started.elapsed().as_secs_f64(),
            "cache_bytes": cache_bytes,
            "cache_stats": cache_stats_json(&engine),
            "session_count": index.sessions.len(),
            "total_nodes": index.sessions.values().map(Vec::len).sum::<usize>(),
            "replay_from_sequence": replay_from_sequence,
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&report).expect("report should serialize")
        );
        let _ = io::stdout().flush();
        return;
    }
    let indexed_nodes_by_session: BTreeMap<String, BTreeSet<u64>> = index
        .sessions
        .iter()
        .map(|(session, nodes)| (session.clone(), nodes.iter().copied().collect()))
        .collect();

    let mut scanned = 0u64;
    let mut accepted = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
    let mut skipped_covered = 0u64;
    let mut checkpoint_flushes = 0u64;
    let mut checkpoint_flush_seconds = 0.0f64;
    let mut next_checkpoint_accepted = flush_every_accepted.max(1);
    let started = Instant::now();
    let end_time_ms = now_ms().saturating_add(1);

    // Read the whole stream and group records by session. Batched ingestion via
    // `ingest_extract_context` persists the index ONCE per call (per session),
    // turning N per-record index rewrites into ~num_sessions -> O(n) not O(n^2).
    let mut groups: BTreeMap<String, Vec<ContextExtractRequest>> = BTreeMap::new();
    // Optional global scope: mirror records tagged for cross-session recall into a
    // stable global tenant so retrieval can surface them from any session.
    let global_session = std::env::var("MATRIXARK_BACKFILL_GLOBAL_SESSION")
        .ok()
        .filter(|s| !s.trim().is_empty());
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(_) => break,
        };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        scanned += 1;
        let rec: Value = match serde_json::from_str(line) {
            Ok(v) => v,
            Err(_) => {
                failed += 1;
                continue;
            }
        };
        let text = rec
            .get("text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_string();
        if text.is_empty() {
            skipped += 1;
            continue;
        }
        let event = rec
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("UserPromptSubmit");
        let session_id = rec
            .get("session_id")
            .and_then(Value::as_str)
            .unwrap_or("backfill_session")
            .to_string();
        let ts_ms = rec
            .get("ts_ms")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0)
            .unwrap_or(end_time_ms);
        let source_id = format!(
            "{}:{}:{}:{:016x}",
            agent_name,
            session_id,
            event,
            stable_hash64(&text)
        );
        // Cap body length for bulk speed: summary/extract cost scales with body size.
        let body_text: String = if text.chars().count() > max_body {
            text.chars().take(max_body).collect()
        } else {
            text.clone()
        };
        let body = format!(
            "agent={}; role={}: {}",
            agent_name,
            role_for_event(event),
            body_text
        );
        let mk = |sid: &str| ContextExtractRequest {
            shard_id: 1,
            tenant_hash: stable_hash64(&format!(
                "{}:{}:{}:{}",
                account_id, tenant_id, user_id, sid
            )),
            source_kind: source_kind_for_event(event),
            source_id: source_id.clone(),
            title: format!("{} {} {}", agent_name, event, sid),
            body: body.clone(),
            timestamp_ms: ts_ms,
            provider: ContextModelProviderConfig::default(),
        };
        groups
            .entry(session_id.clone())
            .or_default()
            .push(mk(&session_id));
        // Cross-session/global mirror: resources + explicitly-global sessions.
        if let Some(gs) = &global_session {
            let is_global = session_id == "_resources" || session_id.starts_with("_global");
            if is_global && gs != &session_id {
                groups.entry(gs.clone()).or_default().push(mk(gs));
            }
        }
    }
    let read_seconds = started.elapsed().as_secs_f64();

    let flush_every_session = env_bool("MATRIXARK_BACKFILL_FLUSH_EVERY_SESSION");
    let ingest_started = Instant::now();
    if raw_first {
        let mut batch = Vec::with_capacity(sub_batch);
        let mut raw_batches = 0u64;
        for (session_id, sources) in groups.into_iter() {
            if sources.is_empty() {
                continue;
            }
            let tenant_hash =
                tenant_hash_for_session(&account_id, &tenant_id, &user_id, &session_id);
            let mut session_seen = 0u64;
            let mut session_skipped_covered = 0u64;
            for source in sources {
                let expected_hash = expected_node_hash(tenant_hash, &source);
                if skip_covered_sessions
                    && indexed_nodes_by_session
                        .get(&session_id)
                        .map(|covered| covered.contains(&expected_hash))
                        .unwrap_or(false)
                {
                    skipped_covered = skipped_covered.saturating_add(1);
                    session_skipped_covered = session_skipped_covered.saturating_add(1);
                    continue;
                }
                batch.push(RawBackfillItem {
                    session_id: session_id.clone(),
                    tenant_hash,
                    source,
                });
                session_seen = session_seen.saturating_add(1);
                if batch.len() >= sub_batch {
                    let (chunk_accepted, chunk_failed, chunk_node_hashes) =
                        write_raw_sources_mixed(&engine, &batch);
                    accepted = accepted.saturating_add(chunk_accepted);
                    failed = failed.saturating_add(chunk_failed);
                    raw_batches = raw_batches.saturating_add(1);
                    extend_session_index(&mut index, chunk_node_hashes);
                    maybe_checkpoint(
                        &engine,
                        accepted,
                        failed,
                        true,
                        exclusive_writer,
                        replay_from_sequence,
                        flush_every_accepted,
                        &mut next_checkpoint_accepted,
                        &mut checkpoint_flushes,
                        &mut checkpoint_flush_seconds,
                    );
                    batch.clear();
                }
            }
            eprintln!(
                "  session {} -> queued {} skipped_covered {} ({:.0} rec/s cumulative)",
                session_id,
                session_seen,
                session_skipped_covered,
                accepted as f64 / ingest_started.elapsed().as_secs_f64().max(0.001)
            );
        }
        if !batch.is_empty() {
            let (chunk_accepted, chunk_failed, chunk_node_hashes) =
                write_raw_sources_mixed(&engine, &batch);
            accepted = accepted.saturating_add(chunk_accepted);
            failed = failed.saturating_add(chunk_failed);
            raw_batches = raw_batches.saturating_add(1);
            extend_session_index(&mut index, chunk_node_hashes);
            maybe_checkpoint(
                &engine,
                accepted,
                failed,
                true,
                exclusive_writer,
                replay_from_sequence,
                flush_every_accepted,
                &mut next_checkpoint_accepted,
                &mut checkpoint_flushes,
                &mut checkpoint_flush_seconds,
            );
        }
        eprintln!(
            "  raw-first mixed batches {} ({:.0} rec/s cumulative)",
            raw_batches,
            accepted as f64 / ingest_started.elapsed().as_secs_f64().max(0.001)
        );
    } else {
        for (session_id, sources) in groups.into_iter() {
            if sources.is_empty() {
                continue;
            }
            let tenant_hash =
                tenant_hash_for_session(&account_id, &tenant_id, &user_id, &session_id);
            let mut session_accepted = 0u64;
            let mut session_failed = 0u64;
            let mut session_skipped_covered = 0u64;
            let mut node_hashes = Vec::new();
            for chunk in sources.chunks(sub_batch) {
                let expected_hashes = expected_node_hashes(tenant_hash, chunk);
                if skip_covered_sessions
                    && !expected_hashes.is_empty()
                    && indexed_nodes_by_session
                        .get(&session_id)
                        .map(|covered| expected_hashes.iter().all(|hash| covered.contains(hash)))
                        .unwrap_or(false)
                {
                    let rows = chunk.len() as u64;
                    skipped_covered = skipped_covered.saturating_add(rows);
                    session_skipped_covered = session_skipped_covered.saturating_add(rows);
                    continue;
                }
                let report = ingest_extract_context(
                    &engine,
                    ContextIngestExtractRequest {
                        shard_id: 1,
                        tenant_hash,
                        sources: chunk.to_vec(),
                        query: String::new(), // ingest-only: skip retrieval work during backfill
                        start_time_ms: 0,
                        end_time_ms,
                        max_events: 1,
                        provider: ContextModelProviderConfig::default(),
                    },
                );
                let (chunk_accepted, chunk_failed, chunk_node_hashes) = (
                    report.accepted as u64,
                    report.failed as u64,
                    report.node_hashes,
                );
                session_accepted += chunk_accepted;
                session_failed += chunk_failed;
                node_hashes.extend(chunk_node_hashes);
                maybe_checkpoint(
                    &engine,
                    accepted.saturating_add(session_accepted),
                    failed.saturating_add(session_failed),
                    false,
                    exclusive_writer,
                    replay_from_sequence,
                    flush_every_accepted,
                    &mut next_checkpoint_accepted,
                    &mut checkpoint_flushes,
                    &mut checkpoint_flush_seconds,
                );
            }
            node_hashes.sort_unstable();
            node_hashes.dedup();
            accepted += session_accepted;
            failed += session_failed;
            if !node_hashes.is_empty() {
                index
                    .sessions
                    .entry(session_id.clone())
                    .or_default()
                    .extend(node_hashes.iter().copied());
            }
            // Bulk persistence normally flushes once after all session groups below.
            // Keep an escape hatch for diagnosis, but do not rewrite the growing
            // shard index after every session in the normal fresh-start backfill.
            if flush_every_session {
                engine.flush_shard_index(1);
            }
            eprintln!(
                "  session {} -> accepted {} failed {} skipped_covered {} ({:.0} rec/s cumulative)",
                session_id,
                session_accepted,
                session_failed,
                session_skipped_covered,
                accepted as f64 / ingest_started.elapsed().as_secs_f64().max(0.001)
            );
        }
    }
    let ingest_seconds = ingest_started.elapsed().as_secs_f64();

    // Persist the TemporalStore shard index once per process/chunk. This is the
    // main latency win for fresh-start backfill: the daemon already chunks and
    // resumes, so we keep durability at each chunk boundary without rewriting
    // the full index once per session.
    let flush_started = Instant::now();
    if raw_first && failed == 0 {
        set_bulk_ingest_expected_wal_commands(
            &engine,
            1,
            replay_from_sequence,
            accepted,
            exclusive_writer,
        );
    }
    engine.flush_shard_index(1);
    let flush_seconds = flush_started.elapsed().as_secs_f64();

    // Dedup and persist the session index once.
    for nodes in index.sessions.values_mut() {
        nodes.sort_unstable();
        nodes.dedup();
    }
    let _ = fs::write(
        &index_path,
        serde_json::to_vec_pretty(&index).expect("session index should serialize"),
    );

    let report = json!({
        "status": "ok",
        "agent_name": agent_name,
        "root": root.display().to_string(),
        "index_path": index_path.display().to_string(),
        "scanned": scanned,
        "accepted": accepted,
        "failed": failed,
        "skipped": skipped,
        "skipped_covered": skipped_covered,
        "seconds": process_started.elapsed().as_secs_f64(),
        "load_seconds": load_seconds,
        "read_seconds": read_seconds,
        "ingest_seconds": ingest_seconds,
        "flush_seconds": flush_seconds,
        "checkpoint_flushes": checkpoint_flushes,
        "checkpoint_flush_seconds": checkpoint_flush_seconds,
        "session_count": index.sessions.len(),
        "total_nodes": index.sessions.values().map(Vec::len).sum::<usize>(),
        "replay_from_sequence": replay_from_sequence,
        "cache_bytes": cache_bytes,
        "cache_stats": cache_stats_json(&engine),
        "flush_every_accepted": flush_every_accepted,
        "skip_covered_sessions": skip_covered_sessions,
        "gate_l1": env_bool("MATRIXARK_CONTEXT_INGEST_GATE_L1"),
        "raw_first": raw_first,
        "raw_first_mixed_batching": raw_first,
        "exclusive_writer": exclusive_writer,
        "sub_batch": sub_batch,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report should serialize")
    );
    let _ = io::stdout().flush();
}

fn cache_stats_json(engine: &TemporalEngine) -> Value {
    let stats = engine.cache().stats();
    json!({
        "memory_bytes": stats.memory_bytes,
        "disk_bytes": stats.disk_bytes,
        "memory_hits": stats.memory_hits,
        "disk_hits": stats.disk_hits,
        "misses": stats.misses,
        "puts": stats.puts,
        "memory_fills": stats.memory_fills,
        "disk_fills": stats.disk_fills,
        "memory_evictions": stats.memory_evictions,
        "pinned_entries": stats.pinned_entries,
        "pinned_bytes": stats.pinned_bytes,
    })
}

fn expected_node_hashes(tenant_hash: u64, sources: &[ContextExtractRequest]) -> Vec<u64> {
    let mut hashes = sources
        .iter()
        .map(|source| {
            stable_hash64(&format!(
                "{}:{}:{}",
                tenant_hash, source.source_kind as u8, source.source_id
            ))
        })
        .collect::<Vec<_>>();
    hashes.sort_unstable();
    hashes.dedup();
    hashes
}

#[derive(Debug)]
struct RawBackfillItem {
    session_id: String,
    tenant_hash: u64,
    source: ContextExtractRequest,
}

fn tenant_hash_for_session(
    account_id: &str,
    tenant_id: &str,
    user_id: &str,
    session_id: &str,
) -> u64 {
    stable_hash64(&format!(
        "{}:{}:{}:{}",
        account_id, tenant_id, user_id, session_id
    ))
}

fn expected_node_hash(tenant_hash: u64, source: &ContextExtractRequest) -> u64 {
    stable_hash64(&format!(
        "{}:{}:{}",
        tenant_hash, source.source_kind as u8, source.source_id
    ))
}

fn extend_session_index(index: &mut SessionIndex, node_hashes: Vec<(String, u64)>) {
    if node_hashes.is_empty() {
        return;
    }
    for (session_id, node_hash) in node_hashes {
        index
            .sessions
            .entry(session_id)
            .or_default()
            .push(node_hash);
    }
}

fn maybe_checkpoint(
    engine: &TemporalEngine,
    accepted: u64,
    failed: u64,
    raw_first: bool,
    exclusive_writer: bool,
    replay_from_sequence: u64,
    flush_every_accepted: u64,
    next_checkpoint_accepted: &mut u64,
    checkpoint_flushes: &mut u64,
    checkpoint_flush_seconds: &mut f64,
) {
    if flush_every_accepted == 0 || accepted < *next_checkpoint_accepted {
        return;
    }
    let checkpoint_started = Instant::now();
    if raw_first && failed == 0 {
        set_bulk_ingest_expected_wal_commands(
            &engine,
            1,
            replay_from_sequence,
            accepted,
            exclusive_writer,
        );
    }
    engine.flush_shard_index(1);
    let elapsed = checkpoint_started.elapsed().as_secs_f64();
    *checkpoint_flushes = checkpoint_flushes.saturating_add(1);
    *checkpoint_flush_seconds += elapsed;
    while *next_checkpoint_accepted <= accepted {
        *next_checkpoint_accepted =
            next_checkpoint_accepted.saturating_add(flush_every_accepted.max(1));
    }
    eprintln!(
        "  checkpoint accepted {} flush_seconds {:.3}",
        accepted, elapsed
    );
}

fn set_bulk_ingest_expected_wal_commands(
    engine: &TemporalEngine,
    shard_id: u64,
    replay_from_sequence: u64,
    accepted: u64,
    exclusive_writer: bool,
) {
    let expected = if exclusive_writer {
        engine
            .write_ahead_log_store()
            .stats(shard_id)
            .last_sequence
            .saturating_sub(replay_from_sequence)
    } else {
        accepted.saturating_mul(RAW_FIRST_DURABLE_WAL_COMMANDS_PER_SOURCE)
    };
    std::env::set_var(
        "MATRIXARK_BULK_INGEST_EXPECTED_WAL_COMMANDS",
        expected.to_string(),
    );
}

fn write_raw_sources_mixed(
    engine: &TemporalEngine,
    items: &[RawBackfillItem],
) -> (u64, u64, Vec<(String, u64)>) {
    if items.is_empty() {
        return (0, 0, Vec::new());
    }
    let shard_id = items[0].source.shard_id;
    let mut commands = Vec::with_capacity(items.len().saturating_mul(3));
    let mut node_hashes = Vec::new();
    for item in items {
        let source = &item.source;
        let tenant_hash = item.tenant_hash;
        let node_hash = expected_node_hash(tenant_hash, source);
        let event_id_hash = stable_hash64(&format!("event:{}:{}", source.source_id, source.body));
        let timestamp_ms = source.timestamp_ms.max(1);
        let node = ContextNode {
            node_hash,
            parent_hash: 0,
            kind: source_kind_code(source.source_kind),
            canonical_name: source.title.clone(),
            l0: truncate_words(&source.body, 18),
            status: 1,
            last_event_time_ms: timestamp_ms,
            summary_dirty: true,
            l1_ref: String::new(),
            raw_metadata_ref: source.source_id.clone(),
        };
        let event = ContextEvent {
            event_id_hash,
            event_time_ms: timestamp_ms,
            ingestion_time_ms: timestamp_ms,
            kind: source_kind_code(source.source_kind),
            event_type: 1,
            actor_hash: stable_hash64(&source.source_id),
            status: 1,
            valid_until_ms: 0,
            confidence: 1.0,
            importance: 1.0,
            text: source.body.clone(),
            source_ref: source.source_id.clone(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
        };
        commands.push(Command::ContextUpsertNode { tenant_hash, node });
        commands.push(Command::ContextWriteExtractedEvent {
            tenant_hash,
            node_hash,
            event,
            indexes: ContextExtractedEventIndexes {
                source_hash: stable_hash64(&source.source_id),
                disabled_indexes: vec![
                    InternalContextIndex::EventKind,
                    InternalContextIndex::Status,
                    InternalContextIndex::EventTimeBucket,
                    InternalContextIndex::Entity,
                ],
                ..ContextExtractedEventIndexes::default()
            },
            first_write_only: true,
            cold_storage: false,
        });
        commands.push(Command::ContextMarkEmbeddingDirty {
            tenant_hash,
            marker: ContextSummaryDirtyMarker {
                node_hash,
                event_time_ms: timestamp_ms,
                reason: EMBEDDING_DIRTY_REASON_BULK_DEFERRED,
                propagate_depth: 0,
            },
            clear: false,
        });
        node_hashes.push((item.session_id.clone(), node_hash));
    }
    let response = engine.batch_execute(BatchExecuteRequest { shard_id, commands });
    if !response.status.ok {
        return (0, items.len() as u64, Vec::new());
    }
    let failed_commands = response
        .responses
        .iter()
        .filter(|entry| !entry.status.ok)
        .count();
    if failed_commands > 0 {
        let failed_rows = failed_commands.div_ceil(3);
        let accepted = items.len().saturating_sub(failed_rows) as u64;
        return (accepted, failed_rows as u64, node_hashes);
    }
    node_hashes.sort_unstable();
    node_hashes.dedup();
    (items.len() as u64, 0, node_hashes)
}

fn session_index_path(root: &PathBuf, agent_name: &str) -> PathBuf {
    root.join(format!(
        "{}-session-index.json",
        sanitize_agent_name(agent_name)
    ))
}

fn sanitize_agent_name(agent_name: &str) -> String {
    let value = agent_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let trimmed = value.trim_matches('-');
    if trimmed.is_empty() {
        "agent".to_string()
    } else {
        trimmed.to_string()
    }
}

fn source_kind_for_event(event: &str) -> ContextSourceKind {
    match event {
        "PostToolUse" | "PreToolUse" | "PermissionRequest" | "ToolCall" | "ToolResult"
        | "cursor.tool" | "claude.tool" => ContextSourceKind::Code,
        "Stop" | "PostCompact" | "SubagentStop" | "SessionEnd" | "ConversationStop"
        | "cursor.stop" | "claude.stop" => ContextSourceKind::UserEvent,
        _ => ContextSourceKind::Chat,
    }
}

fn role_for_event(event: &str) -> &'static str {
    match event {
        "PostToolUse" | "PreToolUse" | "PermissionRequest" | "ToolCall" | "ToolResult"
        | "cursor.tool" | "claude.tool" => "tool",
        "Stop" | "PostCompact" | "SubagentStop" | "SessionEnd" | "ConversationStop"
        | "cursor.stop" | "claude.stop" => "assistant",
        _ => "user",
    }
}

fn source_kind_code(kind: ContextSourceKind) -> u32 {
    match kind {
        ContextSourceKind::Document => 1,
        ContextSourceKind::Chat => 2,
        ContextSourceKind::Ticket => 3,
        ContextSourceKind::Code => 4,
        ContextSourceKind::Incident => 5,
        ContextSourceKind::UserEvent => 6,
    }
}

fn truncate_words(text: &str, max_words: usize) -> String {
    let mut words = text.split_whitespace();
    let mut out = words.by_ref().take(max_words).collect::<Vec<_>>().join(" ");
    if words.next().is_some() {
        out.push_str("...");
    }
    out
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| env_value_bool(&value))
        .unwrap_or(false)
}

fn env_value_bool(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
