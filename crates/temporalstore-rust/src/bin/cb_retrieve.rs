// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Experiment-only: retrieve a REAL ContextPack from a context_batch_ingest store.
//!
//! Loads the embedded TemporalEngine at --root (same with_local_dirs layout the
//! bulk backfill bin writes), loads node_hashes for --session-id from the agent
//! session index, and runs the SAME `retrieve_context` the live hook uses. Use
//! --max-nodes to bound candidate fanout for restart/cache promotion probes.
//! Prints JSON: {query, blocks, tokens, text}.
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::Instant;

use serde::Deserialize;
use serde_json::json;
use temporalstore_rust::{
    retrieve_context, ContextModelProviderConfig, ContextProviderKind, ContextRetrieveRequest,
    ContextSourceKind, ContextTier, TemporalEngine,
};

#[derive(Debug, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

fn stable_hash64(value: &str) -> u64 {
    let mut h = DefaultHasher::new();
    value.hash(&mut h);
    h.finish()
}

fn arg(flag: &str, default: &str) -> String {
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        if a == flag {
            return it.next().unwrap_or_else(|| default.to_string());
        }
    }
    default.to_string()
}

fn main() {
    let root = PathBuf::from(arg("--root", "/tmp/ts-cb/store"));
    let agent = arg("--agent-name", "claude");
    let session = arg("--session-id", "s000");
    let account = arg("--account-id", "acct_cb");
    let tenant = arg("--tenant-id", "tenant_cb");
    let user = arg("--user-id", "cb_user");
    let query = arg("--query", "");
    let max_nodes: usize = arg("--max-nodes", "4000").parse().unwrap_or(4000);
    let default_max_blocks = if max_nodes == 0 { 24 } else { max_nodes };
    let max_blocks: usize = arg("--max-blocks", "")
        .parse()
        .unwrap_or(default_max_blocks)
        .max(1);
    let source_jsonl = PathBuf::from(arg("--source-jsonl", ""));
    // When --embed-base-url is set, rank by REAL model embeddings (query vector
    // comes from the same OpenAI-compatible provider used for the stored node
    // vectors). Without it, the default mock/deterministic provider is used
    // (16-dim hash vectors) which is the lexical/recency-only baseline.
    let embed_base_url = arg("--embed-base-url", "");
    let embedding_model = arg("--embedding-model", "all-MiniLM-L6-v2");
    let provider = if embed_base_url.trim().is_empty() {
        ContextModelProviderConfig::default()
    } else {
        let mut provider = ContextModelProviderConfig::default();
        provider.provider_name = "minilm-local".to_string();
        provider.provider_kind = ContextProviderKind::OpenAiCompatible;
        provider.base_url = embed_base_url.clone();
        provider.api_key_env = String::new();
        provider.embedding_model = embedding_model.clone();
        provider.mock_mode = false;
        provider
    };

    let engine = TemporalEngine::with_local_dirs(
        4 * 1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    let load_started = Instant::now();
    engine.load_shard(1);
    let shard_load_seconds = load_started.elapsed().as_secs_f64();

    let index_path = root.join(format!("{}-session-index.json", agent));
    let index: SessionIndex = fs::read_to_string(&index_path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    let full_node_hashes: Vec<u64> = index.sessions.get(&session).cloned().unwrap_or_default();
    let fallback_node_hashes: Vec<u64> = if max_nodes == 0 {
        full_node_hashes.clone()
    } else {
        full_node_hashes.iter().take(max_nodes).copied().collect()
    };

    let tenant_hash = stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
    let emit_blocks = arg("--emit-blocks", "0") == "1";

    // Optional one-shot embedding-lookup probe (diagnostic): query node_l0 refs
    // directly against this engine/tenant to isolate reload vs retrieve-path.
    if arg("--probe-embeddings", "0") == "1" {
        use temporalstore_rust::{
            context_embedding_ref_hash, Command, CommandResponse, ExecuteRequest,
        };
        eprintln!(
            "probe tenant_hash={} node_count={} first_node={:?} first_ref(FNV)={:?}",
            tenant_hash,
            fallback_node_hashes.len(),
            fallback_node_hashes.first().copied(),
            fallback_node_hashes
                .first()
                .map(|nh| context_embedding_ref_hash(tenant_hash, *nh, "node_l0")),
        );
        // (a) node_l0 only, small/large; (b) retrieve's EXACT interleaved shape.
        for take in [500usize, 900, 1000, 1001, 1100, 2000, 3700] {
            let refs: Vec<u64> = fallback_node_hashes
                .iter()
                .take(take)
                .map(|nh| context_embedding_ref_hash(tenant_hash, *nh, "node_l0"))
                .collect();
            let n = refs.len();
            for lim in [n, 1000usize] {
                let resp = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextQueryEmbeddings {
                        tenant_hash,
                        ref_hashes: refs.clone(),
                        limit: Some(lim.max(1)),
                    },
                });
                let found = match resp.response {
                    CommandResponse::ContextEmbeddings { embeddings } => embeddings.len(),
                    _ => 0,
                };
                eprintln!(
                    "probe node_l0-only refs={} limit={} found={}",
                    n, lim, found
                );
            }
        }
        // Exact retrieve shape: interleaved node_l0/node_l1, limit = len*2.
        let mut interleaved = Vec::new();
        for nh in &fallback_node_hashes {
            for label in ["node_l0", "node_l1"] {
                interleaved.push(context_embedding_ref_hash(tenant_hash, *nh, label));
            }
        }
        let lim = fallback_node_hashes.len().saturating_mul(2).max(1);
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEmbeddings {
                tenant_hash,
                ref_hashes: interleaved.clone(),
                limit: Some(lim),
            },
        });
        let found = match resp.response {
            CommandResponse::ContextEmbeddings { embeddings } => embeddings.len(),
            _ => 0,
        };
        eprintln!(
            "probe interleaved refs={} limit={} found={}",
            interleaved.len(),
            lim,
            found
        );
        return;
    }

    // Run a single retrieve at a given fanout cap and serialize the result.
    let run_one = |query: &str, cap: usize| -> serde_json::Value {
        let mut prefilter_debug = json!({
            "enabled": false,
            "source_jsonl": source_jsonl.display().to_string(),
            "matched_source_rows": 0,
        });
        let mut node_hashes = fallback_node_hashes.clone();
        if !source_jsonl.as_os_str().is_empty() && source_jsonl.exists() && max_nodes > 0 {
            let (prefiltered, debug) = prefilter_nodes_from_source_jsonl(
                &source_jsonl,
                &agent,
                &session,
                tenant_hash,
                query,
                &full_node_hashes,
                max_nodes,
            );
            prefilter_debug = debug;
            if !prefiltered.is_empty() {
                node_hashes = prefiltered;
            }
        }
        let started = Instant::now();
        let report = retrieve_context(
            &engine,
            ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash,
                node_hashes: node_hashes.clone(),
                query: query.to_string(),
                start_time_ms: 0,
                end_time_ms: u64::MAX,
                max_events: cap,
                min_confidence: 0.0,
                min_importance: 0.0,
                tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
                max_summary_nodes: cap,
                max_event_nodes: cap,
                prefer_current_agent: false,
                current_agent_scope_key: format!("agent:{}", agent),
                provider: provider.clone(),
            },
        );
        let mut text = String::new();
        let mut tokens: u64 = 0;
        let mut block_list = Vec::new();
        for b in &report.blocks {
            tokens += b.estimated_tokens as u64;
            text.push_str(&b.text);
            text.push_str("\n\n");
            block_list.push(json!({
                "node_hash": b.node_hash,
                "tokens": b.estimated_tokens,
                "source_ref": b.source_ref,
                "text": b.text,
            }));
        }
        let tts = &report.query_understanding_debug.tree_traversal_summary;
        json!({
            "query": query,
            "cap": cap,
            "session": session,
            "session_node_count": full_node_hashes.len(),
            "candidate_node_count": node_hashes.len(),
            "candidate_node_limit": max_nodes,
            "candidate_node_truncated": full_node_hashes.len() > node_hashes.len(),
            "max_blocks": cap,
            "shard_load_seconds": shard_load_seconds,
            "candidate_prefilter": prefilter_debug,
            "ok": report.status.ok,
            "status_code": report.status.code,
            "blocks": report.blocks.len(),
            "tokens": tokens,
            "query_embedding_dimension": tts.query_embedding_dimension,
            "summary_embedding_candidate_count": tts.summary_embedding_candidate_count,
            "summary_embedding_selected_count": tts.summary_embedding_selected_count,
            "retrieve_seconds": started.elapsed().as_secs_f64(),
            "text": if emit_blocks { String::new() } else { text },
            "block_list": if emit_blocks { serde_json::Value::Array(block_list) } else { serde_json::Value::Null },
        })
    };

    // Batch mode: load the engine ONCE and answer many "<cap>\t<query>" lines
    // from stdin, printing one JSON object per line. This amortizes the ~17s
    // shard-load cost across a whole sweep. Otherwise run the single --query.
    if arg("--batch-stdin", "0") == "1" {
        use std::io::{BufRead, Write};
        let stdin = std::io::stdin();
        let stdout = std::io::stdout();
        let mut out = stdout.lock();
        for line in stdin.lock().lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => break,
            };
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (cap_str, q) = line.split_once('\t').unwrap_or(("8", line));
            let cap: usize = cap_str.trim().parse().unwrap_or(8);
            let value = run_one(q, cap);
            let _ = writeln!(out, "{}", value);
            let _ = out.flush();
        }
    } else {
        println!("{}", run_one(&query, max_blocks));
    }
}

fn prefilter_nodes_from_source_jsonl(
    path: &PathBuf,
    agent: &str,
    session: &str,
    tenant_hash: u64,
    query: &str,
    full_node_hashes: &[u64],
    max_nodes: usize,
) -> (Vec<u64>, serde_json::Value) {
    let allowed: HashSet<u64> = full_node_hashes.iter().copied().collect();
    let query_terms = text_terms(query);
    let Ok(text) = fs::read_to_string(path) else {
        return (
            Vec::new(),
            json!({"enabled": false, "source_jsonl": path.display().to_string(), "error": "read_failed"}),
        );
    };
    let mut candidates: Vec<(usize, u64, usize, u64)> = Vec::new();
    let mut matched_source_rows = 0usize;
    let mut intersected_node_rows = 0usize;
    for (ordinal, line) in text.lines().enumerate() {
        let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if row.get("session_id").and_then(|v| v.as_str()) != Some(session) {
            continue;
        }
        let event = row
            .get("event")
            .and_then(|v| v.as_str())
            .unwrap_or("UserPromptSubmit");
        let body = row.get("text").and_then(|v| v.as_str()).unwrap_or("");
        if body.trim().is_empty() {
            continue;
        }
        matched_source_rows += 1;
        let source_id = format!(
            "{}:{}:{}:{:016x}",
            agent,
            session,
            event,
            stable_hash64(body)
        );
        let node_hash = stable_hash64(&format!(
            "{}:{}:{}",
            tenant_hash,
            source_kind_for_event(event) as u8,
            source_id
        ));
        if !allowed.contains(&node_hash) {
            continue;
        }
        intersected_node_rows += 1;
        let terms = text_terms(body);
        let overlap = query_terms.intersection(&terms).count();
        let ts_ms = row.get("ts_ms").and_then(|v| v.as_u64()).unwrap_or(0);
        candidates.push((overlap, ts_ms, ordinal, node_hash));
    }
    candidates.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| right.1.cmp(&left.1))
            .then_with(|| left.2.cmp(&right.2))
    });
    let mut selected = Vec::new();
    let mut seen = HashSet::new();
    let positive_overlap = candidates.iter().filter(|item| item.0 > 0).count();
    for (_overlap, _ts, _ordinal, node_hash) in candidates {
        if seen.insert(node_hash) {
            selected.push(node_hash);
            if selected.len() >= max_nodes {
                break;
            }
        }
    }
    let debug = json!({
        "enabled": true,
        "source_jsonl": path.display().to_string(),
        "matched_source_rows": matched_source_rows,
        "intersected_node_rows": intersected_node_rows,
        "positive_overlap_rows": positive_overlap,
        "selected_node_count": selected.len(),
    });
    (selected, debug)
}

fn text_terms(text: &str) -> HashSet<String> {
    text.split(|ch: char| !ch.is_ascii_alphanumeric())
        .map(|term| term.trim().to_ascii_lowercase())
        .filter(|term| term.len() > 2)
        .collect()
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
