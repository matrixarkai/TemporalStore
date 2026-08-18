// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Experiment-only: retrieve a REAL ContextPack from a context_batch_ingest store.
//!
//! Loads the embedded TemporalEngine at --root (same with_local_dirs layout the
//! bulk backfill bin writes), loads all node_hashes for --session-id from the
//! agent session index, and runs the SAME `retrieve_context` the live hook uses,
//! with high fanout caps so recall reflects the whole store (not the hook's
//! 8-event injection cap). Prints JSON: {query, blocks, tokens, text}.
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::json;
use temporalstore_rust::{
    retrieve_context, ContextModelProviderConfig, ContextProviderKind, ContextRetrieveRequest,
    ContextTier, TemporalEngine,
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
    engine.load_shard(1);

    let index_path = root.join(format!("{}-session-index.json", agent));
    let index: SessionIndex = fs::read_to_string(&index_path)
        .ok()
        .and_then(|t| serde_json::from_str(&t).ok())
        .unwrap_or_default();
    let node_hashes: Vec<u64> = index.sessions.get(&session).cloned().unwrap_or_default();

    let tenant_hash = stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1);

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
            node_hashes.len(),
            node_hashes.first().copied(),
            node_hashes
                .first()
                .map(|nh| context_embedding_ref_hash(tenant_hash, *nh, "node_l0")),
        );
        // (a) node_l0 only, small/large; (b) retrieve's EXACT interleaved shape.
        for take in [500usize, 900, 1000, 1001, 1100, 2000, 3700] {
            let refs: Vec<u64> = node_hashes
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
        for nh in &node_hashes {
            for label in ["node_l0", "node_l1"] {
                interleaved.push(context_embedding_ref_hash(tenant_hash, *nh, label));
            }
        }
        let lim = node_hashes.len().saturating_mul(2).max(1);
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
        let started = Instant::now();
        let report = retrieve_context(
            &engine,
            ContextRetrieveRequest {
                shard_id: 1,
                tenant_hash,
                node_hashes: node_hashes.clone(),
                query: query.to_string(),
                start_time_ms: 0,
                end_time_ms: now_ms.saturating_add(1),
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
            "session_node_count": node_hashes.len(),
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
        println!("{}", run_one(&query, max_nodes));
    }
}
