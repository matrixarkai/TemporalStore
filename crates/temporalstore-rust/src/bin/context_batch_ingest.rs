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
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use temporalstore_rust::{
    ingest_extract_context, ContextExtractRequest, ContextIngestExtractRequest,
    ContextModelProviderConfig, ContextSourceKind, TemporalEngine,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

fn main() {
    // Bulk backfill: defer per-record index persistence to flush_shard_index()
    // (see MATRIXARK_BULK_INGEST gate in the engine). Turns O(n^2) into O(n).
    std::env::set_var("MATRIXARK_BULK_INGEST", "1");
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
    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(1);
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

    let mut scanned = 0u64;
    let mut accepted = 0u64;
    let mut failed = 0u64;
    let mut skipped = 0u64;
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

    let flush_every_session = env_bool("MATRIXARK_BACKFILL_FLUSH_EVERY_SESSION");
    for (session_id, sources) in groups.into_iter() {
        if sources.is_empty() {
            continue;
        }
        let tenant_hash = stable_hash64(&format!(
            "{}:{}:{}:{}",
            account_id, tenant_id, user_id, session_id
        ));
        let report = ingest_extract_context(
            &engine,
            ContextIngestExtractRequest {
                shard_id: 1,
                tenant_hash,
                sources,
                query: String::new(), // ingest-only: skip retrieval work during backfill
                start_time_ms: 0,
                end_time_ms,
                max_events: 1,
                provider: ContextModelProviderConfig::default(),
            },
        );
        accepted += report.accepted as u64;
        failed += report.failed as u64;
        if !report.node_hashes.is_empty() {
            index
                .sessions
                .entry(session_id.clone())
                .or_default()
                .extend(report.node_hashes.iter().copied());
        }
        // Bulk persistence normally flushes once after all session groups below.
        // Keep an escape hatch for diagnosis, but do not rewrite the growing
        // shard index after every session in the normal fresh-start backfill.
        if flush_every_session {
            engine.flush_shard_index(1);
        }
        eprintln!(
            "  session {} -> accepted {} failed {} ({:.0} rec/s cumulative)",
            session_id,
            report.accepted,
            report.failed,
            accepted as f64 / started.elapsed().as_secs_f64().max(0.001)
        );
    }

    // Persist the TemporalStore shard index once per process/chunk. This is the
    // main latency win for fresh-start backfill: the daemon already chunks and
    // resumes, so we keep durability at each chunk boundary without rewriting
    // the full index once per session.
    engine.flush_shard_index(1);

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
        "seconds": started.elapsed().as_secs_f64(),
        "session_count": index.sessions.len(),
        "total_nodes": index.sessions.values().map(Vec::len).sum::<usize>(),
        "replay_from_sequence": replay_from_sequence,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report should serialize")
    );
    let _ = io::stdout().flush();
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
