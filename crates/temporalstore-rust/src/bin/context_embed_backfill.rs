// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI
//! Additive embed-backfill for `raw_first` bulk-ingested context stores.
//!
//! `context_batch_ingest` with `MATRIXARK_BACKFILL_RAW_FIRST=1` stores raw
//! ContextNode/ContextEvent/source-index rows fast but **defers embeddings** --
//! and no runnable extraction pass existed to attach them. Without embeddings the
//! retrieve path (`retrieve_context`) has nothing to rank by cosine, so focused
//! semantic recall on a large bulk store degrades to lexical/recency fallback
//! (~0% needle recall at scale).
//!
//! This bin closes that gap WITHOUT touching live ingest: it opens an existing
//! `with_local_dirs` store, enumerates the context nodes recorded in the agent
//! session index, reads each node's stored event text, batches those texts to a
//! real OpenAI-compatible embedding provider (e.g. a local MiniLM
//! `/v1/embeddings` server), and persists the `ctx:embedding:{tenant}:{ref}`
//! entries under the exact `node_l0` ref-hash the retrieve path reads
//! (`context_embedding_ref_hash`). It reuses the engine's own provider path
//! (`context_backfill_embeddings`) so batching, `MATRIXARK_REQUIRE_MODEL_EMBEDDINGS`
//! enforcement, and response validation are identical to live extraction.
//!
//! Identity flags mirror `cb_retrieve`/the live hook so the derived tenant_hash
//! matches what retrieval uses. Example:
//!   MATRIXARK_REQUIRE_MODEL_EMBEDDINGS=1 context_embed_backfill \
//!     --root /tmp/ts-cb-lin/1350000/store --agent-name claude \
//!     --account-id acct_cb --tenant-id tenant_cb --user-id cb_user \
//!     --embed-base-url http://127.0.0.1:18099/v1 \
//!     --embedding-model all-MiniLM-L6-v2
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::json;
use temporalstore_rust::{
    context_backfill_embeddings, context_embedding_ref_hash, BatchExecuteRequest, Command,
    CommandResponse, ContextEmbedding, ContextModelProviderConfig, ContextProviderKind,
    ExecuteRequest, TemporalEngine,
};

#[derive(Debug, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

fn load_node_l0_texts(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hashes: &[u64],
) -> HashMap<u64, String> {
    let mut out = HashMap::new();
    for chunk in node_hashes.chunks(512) {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNodes {
                tenant_hash,
                node_hashes: chunk.to_vec(),
            },
        });
        if let CommandResponse::ContextNodes { nodes } = resp.response {
            for node in nodes {
                let text = if node.l0.trim().is_empty() {
                    node.canonical_name.clone()
                } else {
                    node.l0.clone()
                };
                if !text.trim().is_empty() {
                    out.insert(node.node_hash, text);
                }
            }
        }
    }
    out
}

fn stable_hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

fn main() {
    let root = PathBuf::from(arg("--root", "/tmp/ts-cb/store"));
    let agent = arg("--agent-name", "claude");
    let account = arg("--account-id", "acct_cb");
    let tenant = arg("--tenant-id", "tenant_cb");
    let user = arg("--user-id", "cb_user");
    let only_session = arg("--session-id", "");
    let base_url = arg("--embed-base-url", "http://127.0.0.1:18099/v1");
    let embedding_model = arg("--embedding-model", "all-MiniLM-L6-v2");
    let batch: usize = arg("--batch", "64").parse().unwrap_or(64);
    let max_events: usize = arg("--max-events", "4").parse().unwrap_or(4);
    let prefer_events = arg("--prefer-events", "0") == "1";
    let verify_only = arg("--verify", "0") == "1";

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

    let mut provider = ContextModelProviderConfig::default();
    provider.provider_name = "minilm-local".to_string();
    provider.provider_kind = ContextProviderKind::OpenAiCompatible;
    provider.base_url = base_url.clone();
    provider.api_key_env = String::new();
    provider.embedding_model = embedding_model.clone();
    provider.mock_mode = false;

    let sessions: Vec<String> = if only_session.trim().is_empty() {
        index.sessions.keys().cloned().collect()
    } else {
        vec![only_session.clone()]
    };

    if verify_only {
        // Fresh-load verification: for the first N nodes of each session, query
        // the node_l0 embedding straight from the reloaded store and count hits.
        let n_probe = 20usize;
        for session in &sessions {
            let node_hashes = index.sessions.get(session).cloned().unwrap_or_default();
            let tenant_hash =
                stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
            let probe: Vec<u64> = node_hashes.iter().take(n_probe).copied().collect();
            let ref_hashes: Vec<u64> = probe
                .iter()
                .map(|nh| context_embedding_ref_hash(tenant_hash, *nh, "node_l0"))
                .collect();
            let resp = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextQueryEmbeddings {
                    tenant_hash,
                    ref_hashes: ref_hashes.clone(),
                    limit: Some(ref_hashes.len().max(1)),
                },
            });
            let (found, dim) =
                if let CommandResponse::ContextEmbeddings { embeddings } = resp.response {
                    let d = embeddings.first().map(|e| e.vector.len()).unwrap_or(0);
                    (embeddings.len(), d)
                } else {
                    (0, 0)
                };
            println!(
                "{}",
                json!({
                    "verify": true,
                    "session": session,
                    "tenant_hash": tenant_hash,
                    "probed": probe.len(),
                    "found": found,
                    "vector_dim": dim,
                    "first_node_hash": probe.first().copied(),
                    "first_ref_hash": ref_hashes.first().copied(),
                })
            );
        }
        return;
    }

    let started = Instant::now();
    let mut total_nodes = 0u64;
    let mut embedded = 0u64;
    let mut empty_text = 0u64;
    let mut node_l0_text_used = 0u64;
    let mut failed = 0u64;
    let updated_at_ms = now_ms();

    for session in &sessions {
        let node_hashes = index.sessions.get(session).cloned().unwrap_or_default();
        if node_hashes.is_empty() {
            continue;
        }
        let tenant_hash = stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
        let end_time_ms = u64::MAX;
        let node_texts = load_node_l0_texts(&engine, tenant_hash, &node_hashes);

        // Collect (node_hash, text) by reading each node's stored events.
        // If event bytes are unavailable in an old/raw-first store, fall back to
        // the ContextNode L0 text so every context node can receive a model vector.
        let mut pending: Vec<(u64, String)> = Vec::with_capacity(node_hashes.len());
        for node_hash in &node_hashes {
            total_nodes += 1;
            let mut text = String::new();
            if prefer_events {
                let resp = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command: Command::ContextQueryEvents {
                        tenant_hash,
                        node_hash: *node_hash,
                        start_time_ms: 0,
                        end_time_ms,
                        limit: Some(max_events.max(1)),
                        current_valid_only: false,
                        as_of_ms: 0,
                        kinds: Vec::new(),
                        statuses: Vec::new(),
                        min_confidence: 0.0,
                        min_importance: 0.0,
                    },
                });
                if let CommandResponse::ContextEvents { events, .. } = resp.response {
                    // Use the richest (longest) event text for this node.
                    text = events
                        .into_iter()
                        .map(|e| e.text)
                        .max_by_key(|t| t.len())
                        .unwrap_or_default();
                }
            }
            if text.trim().is_empty() {
                if let Some(node_text) = node_texts.get(node_hash) {
                    text = node_text.clone();
                    node_l0_text_used += 1;
                }
            }
            if text.trim().is_empty() {
                empty_text += 1;
                continue;
            }
            pending.push((*node_hash, text));
        }

        // Batch-embed and upsert node_l0 embeddings under the retrieve-path key.
        for chunk in pending.chunks(batch.max(1)) {
            let texts: Vec<&str> = chunk.iter().map(|(_, t)| t.as_str()).collect();
            let vectors = match context_backfill_embeddings(&provider, &texts) {
                Ok(v) => v,
                Err(status) => {
                    eprintln!(
                        "embedding batch failed (session {}): {} / {}",
                        session, status.code, status.message
                    );
                    failed += chunk.len() as u64;
                    continue;
                }
            };
            if vectors.len() != chunk.len() {
                eprintln!(
                    "embedding count mismatch (session {}): {} vectors for {} inputs",
                    session,
                    vectors.len(),
                    chunk.len()
                );
                failed += chunk.len() as u64;
                continue;
            }
            let mut commands = Vec::with_capacity(chunk.len());
            for ((node_hash, _), vector) in chunk.iter().zip(vectors.into_iter()) {
                let embedding = ContextEmbedding {
                    ref_hash: context_embedding_ref_hash(tenant_hash, *node_hash, "node_l0"),
                    level: 1,
                    model_hash: stable_hash64(&format!("embedding_model:{}", embedding_model)),
                    vector,
                    updated_at_ms,
                };
                commands.push(Command::ContextUpsertEmbedding {
                    tenant_hash,
                    embedding,
                });
            }
            let response = engine.batch_execute(BatchExecuteRequest {
                shard_id: 1,
                commands,
            });
            let ok = response.responses.iter().filter(|r| r.status.ok).count();
            let bad = response.responses.len().saturating_sub(ok);
            embedded += ok as u64;
            failed += bad as u64;
        }
        eprintln!(
            "  session {} -> nodes {} embedded {} ({:.0} node/s cumulative)",
            session,
            node_hashes.len(),
            embedded,
            embedded as f64 / started.elapsed().as_secs_f64().max(0.001)
        );
    }

    // Persist the shard index once so the upserted embeddings survive reload.
    engine.flush_shard_index(1);

    let report = json!({
        "status": "ok",
        "root": root.display().to_string(),
        "index_path": index_path.display().to_string(),
        "base_url": base_url,
        "embedding_model": embedding_model,
        "sessions": sessions.len(),
        "total_nodes": total_nodes,
        "embedded": embedded,
        "empty_text": empty_text,
        "node_l0_text_used": node_l0_text_used,
        "prefer_events": prefer_events,
        "failed": failed,
        "seconds": started.elapsed().as_secs_f64(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report should serialize")
    );
}
