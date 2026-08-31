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
//! It reuses the engine's own provider path
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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::json;
use temporalstore_rust::{
    context_backfill_embeddings, BatchExecuteRequest, Command,
    CommandResponse, ContextModelProviderConfig, ContextProviderKind,
    ExecuteRequest, TemporalEngine,
};

#[derive(Debug, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, Vec<u64>>,
}

#[derive(Debug, Deserialize, Default)]
struct VerifyCoverageReport {
    rows: Vec<VerifyCoverageRow>,
}

#[derive(Debug, Deserialize, Default)]
struct VerifyCoverageRow {
    session: String,
    node_count: Option<usize>,
    fully_covered: Option<bool>,
}

fn load_fully_covered_sessions(path: &str) -> HashMap<String, usize> {
    if path.trim().is_empty() {
        return HashMap::new();
    }
    fs::read_to_string(path)
        .ok()
        .and_then(|text| serde_json::from_str::<VerifyCoverageReport>(&text).ok())
        .map(|report| {
            report
                .rows
                .into_iter()
                .filter_map(|row| {
                    if row.fully_covered.unwrap_or(false) {
                        Some((row.session, row.node_count.unwrap_or(0)))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

fn load_embedded_nodes(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hashes: &[u64],
) -> HashSet<u64> {
    // "Already embedded" is a property of the node record: the vector lives on the node and
    // nowhere else. A node the engine does not return, or returns without a vector, needs
    // embedding.
    let mut out = HashSet::new();
    for chunk in node_hashes.chunks(512) {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNodes {
                tenant_hash,
                node_hashes: chunk.to_vec(),
            },
        });
        if let CommandResponse::ContextNodes { nodes } = response.response {
            for node in nodes {
                if !node.vector.is_empty() {
                    out.insert(node.node_hash);
                }
            }
        }
    }
    out
}

fn embedded_node_vector_dim(
    engine: &TemporalEngine,
    tenant_hash: u64,
    node_hash: u64,
) -> usize {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ContextGetNode {
            tenant_hash,
            node_hash,
        },
    });
    match response.response {
        CommandResponse::ContextNode {
            node: Some(node), ..
        } => node.vector.len(),
        _ => 0,
    }
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

fn write_report_json(path: &str, value: &serde_json::Value) {
    if path.trim().is_empty() {
        return;
    }
    if let Some(parent) = PathBuf::from(path).parent() {
        if !parent.as_os_str().is_empty() {
            let _ = fs::create_dir_all(parent);
        }
    }
    if let Ok(text) = serde_json::to_string_pretty(value) {
        let _ = fs::write(path, text);
    }
}

fn capped_nodes(mut nodes: Vec<u64>, max_nodes: usize) -> Vec<u64> {
    if max_nodes > 0 && nodes.len() > max_nodes {
        nodes.truncate(max_nodes);
    }
    nodes
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
}

fn main() {
    // Embedding backfill is an additive bulk maintenance task. Coalesce per-node
    // persistence and persist the served index once at the end.
    std::env::set_var("MATRIXARK_BULK_INGEST", "1");
    if std::env::var("TS_PHASE1_FLAT").is_err() {
        std::env::set_var("TS_PHASE1_FLAT", "1");
    }
    if std::env::var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD").is_err() {
        std::env::set_var("MATRIXARK_EAGER_CACHE_WARM_ON_LOAD", "0");
    }

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
    let max_nodes: usize = arg("--max-nodes", "0").parse().unwrap_or(0);
    let max_new_embeddings: usize = arg("--max-new-embeddings", "0").parse().unwrap_or(0);
    let max_new_per_session: usize = arg("--max-new-per-session", "0").parse().unwrap_or(0);
    let skip_covered_sessions = arg("--skip-covered-sessions", "0") == "1";
    let skip_fully_covered_report = arg("--skip-fully-covered-report", "");
    let report_json = arg("--report-json", "");
    let verify_only = arg("--verify", "0") == "1";
    let verify_full = arg("--verify-full", "0") == "1";
    let fully_covered_sessions = load_fully_covered_sessions(&skip_fully_covered_report);

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
        // Fresh-load verification: by default sample the first N nodes of each
        // session for fast checks; --verify-full checks every indexed node so
        // bulk load jobs can prove exact missing counts before/after backfill.
        let n_probe = if verify_full { usize::MAX } else { 20usize };
        let mut verify_reports = Vec::new();
        for session in &sessions {
            let node_hashes = capped_nodes(
                index.sessions.get(session).cloned().unwrap_or_default(),
                max_nodes,
            );
            let tenant_hash =
                stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
            let probe: Vec<u64> = node_hashes.iter().take(n_probe).copied().collect();
            let embedded = load_embedded_nodes(&engine, tenant_hash, &probe);
            let found = embedded.len();
            let dim = embedded
                .iter()
                .next()
                .map(|node_hash| embedded_node_vector_dim(&engine, tenant_hash, *node_hash))
                .unwrap_or(0);
            let row = json!({
                "verify": true,
                "verify_full": verify_full,
                "session": session,
                "tenant_hash": tenant_hash,
                "node_count": node_hashes.len(),
                "probed": probe.len(),
                "found": found,
                "missing": probe.len().saturating_sub(found),
                "fully_covered": found == probe.len(),
                "vector_dim": dim,
                "max_nodes": max_nodes,
                "shard_load_seconds": shard_load_seconds,
                "first_node_hash": probe.first().copied(),
                "first_embedded_node": embedded.iter().next().copied(),
            });
            println!("{row}");
            verify_reports.push(row);
        }
        let sessions_with_embeddings = verify_reports
            .iter()
            .filter(|row| row.get("found").and_then(|value| value.as_u64()).unwrap_or(0) > 0)
            .count();
        let sessions_fully_covered = verify_reports
            .iter()
            .filter(|row| {
                let probed = row.get("probed").and_then(|value| value.as_u64()).unwrap_or(0);
                probed > 0
                    && row.get("found").and_then(|value| value.as_u64()).unwrap_or(0) == probed
            })
            .count();
        let sessions_partially_covered = verify_reports
            .iter()
            .filter(|row| {
                let probed = row.get("probed").and_then(|value| value.as_u64()).unwrap_or(0);
                let found = row.get("found").and_then(|value| value.as_u64()).unwrap_or(0);
                probed > 0 && found > 0 && found < probed
            })
            .count();
        let sessions_missing_all_probed = verify_reports
            .iter()
            .filter(|row| {
                row.get("probed").and_then(|value| value.as_u64()).unwrap_or(0) > 0
                    && row.get("found").and_then(|value| value.as_u64()).unwrap_or(0) == 0
            })
            .count();
        let total_nodes: u64 = verify_reports
            .iter()
            .map(|row| row.get("node_count").and_then(|value| value.as_u64()).unwrap_or(0))
            .sum();
        let total_probed: u64 = verify_reports
            .iter()
            .map(|row| row.get("probed").and_then(|value| value.as_u64()).unwrap_or(0))
            .sum();
        let total_found: u64 = verify_reports
            .iter()
            .map(|row| row.get("found").and_then(|value| value.as_u64()).unwrap_or(0))
            .sum();
        let report = json!({
            "status": "ok",
            "verify": true,
            "verify_full": verify_full,
            "root": root.display().to_string(),
            "index_path": index_path.display().to_string(),
            "sessions": sessions.len(),
            "sessions_with_embeddings": sessions_with_embeddings,
            "sessions_fully_covered": sessions_fully_covered,
            "sessions_partially_covered": sessions_partially_covered,
            "sessions_missing_all_probed": sessions_missing_all_probed,
            "total_nodes": total_nodes,
            "total_probed": total_probed,
            "total_found": total_found,
            "total_missing": total_probed.saturating_sub(total_found),
            "probe_limit": if verify_full { 0 } else { n_probe },
            "max_nodes": max_nodes,
            "shard_load_seconds": shard_load_seconds,
            "rows": verify_reports,
        });
        write_report_json(&report_json, &report);
        return;
    }

    let started = Instant::now();
    let mut total_nodes = 0u64;
    let mut embedded = 0u64;
    let mut empty_text = 0u64;
    let mut node_l0_text_used = 0u64;
    let mut missing_embedding_candidates = 0u64;
    let mut skipped_existing = 0u64;
    let mut skipped_covered_sessions = 0u64;
    let mut skipped_covered_session_nodes = 0u64;
    let mut skipped_report_sessions = 0u64;
    let mut skipped_report_session_nodes = 0u64;
    let mut skipped_report_stale_sessions = 0u64;
    let mut skipped_by_new_cap = 0u64;
    let mut failed = 0u64;
    let updated_at_ms = now_ms();

    for session in &sessions {
        let node_hashes = capped_nodes(
            index.sessions.get(session).cloned().unwrap_or_default(),
            max_nodes,
        );
        if node_hashes.is_empty() {
            continue;
        }
        total_nodes += node_hashes.len() as u64;
        if let Some(covered_node_count) = fully_covered_sessions.get(session) {
            if *covered_node_count == node_hashes.len() {
                skipped_report_sessions += 1;
                skipped_report_session_nodes += node_hashes.len() as u64;
                eprintln!(
                    "  session {} -> nodes {} skipped_report_fully_covered",
                    session,
                    node_hashes.len()
                );
                continue;
            }
            skipped_report_stale_sessions += 1;
        }
        let tenant_hash = stable_hash64(&format!("{}:{}:{}:{}", account, tenant, user, session));
        let end_time_ms = u64::MAX;
        let existing_started = Instant::now();
        let existing_embeddings = load_embedded_nodes(&engine, tenant_hash, &node_hashes);
        let existing_seconds = existing_started.elapsed().as_secs_f64();
        skipped_existing += existing_embeddings.len() as u64;
        if skip_covered_sessions && !existing_embeddings.is_empty() {
            skipped_covered_sessions += 1;
            skipped_covered_session_nodes += node_hashes.len() as u64;
            eprintln!(
                "  session {} -> nodes {} skipped_covered existing {} existing_seconds {:.3}",
                session,
                node_hashes.len(),
                existing_embeddings.len(),
                existing_seconds
            );
            continue;
        }

        let mut missing_nodes: Vec<u64> = node_hashes
            .iter()
            .copied()
            .filter(|node_hash| !existing_embeddings.contains(node_hash))
            .collect();
        missing_embedding_candidates += missing_nodes.len() as u64;
        if max_new_embeddings > 0 {
            let remaining = max_new_embeddings.saturating_sub(embedded as usize);
            if missing_nodes.len() > remaining {
                skipped_by_new_cap += (missing_nodes.len() - remaining) as u64;
                missing_nodes.truncate(remaining);
            }
        }
        if max_new_per_session > 0 && missing_nodes.len() > max_new_per_session {
            skipped_by_new_cap += (missing_nodes.len() - max_new_per_session) as u64;
            missing_nodes.truncate(max_new_per_session);
        }

        let node_text_started = Instant::now();
        let node_texts = load_node_l0_texts(&engine, tenant_hash, &missing_nodes);
        let node_text_seconds = node_text_started.elapsed().as_secs_f64();

        // Collect (node_hash, text) by reading each node's stored events.
        // If event bytes are unavailable in an old/raw-first store, fall back to
        // the ContextNode L0 text so every context node can receive a model vector.
        let mut pending: Vec<(u64, String)> = Vec::with_capacity(missing_nodes.len());
        for node_hash in &missing_nodes {
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
                        max_scan: None,
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
                commands.push(Command::ContextSetNodeEmbedding {
                    tenant_hash,
                    node_hash: *node_hash,
                    model_hash: stable_hash64(&format!("embedding_model:{}", embedding_model)),
                    vector,
                    updated_at_ms,
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
            "  session {} -> nodes {} missing {} embedded {} skipped_existing {} skipped_by_new_cap {} node_l0_texts {} existing_seconds {:.3} node_text_seconds {:.3} ({:.0} node/s cumulative)",
            session,
            node_hashes.len(),
            missing_nodes.len(),
            embedded,
            existing_embeddings.len(),
            skipped_by_new_cap,
            node_texts.len(),
            existing_seconds,
            node_text_seconds,
            embedded as f64 / started.elapsed().as_secs_f64().max(0.001)
        );
    }

    // Persist the shard index once so the upserted embeddings survive reload.
    let flush_started = Instant::now();
    if embedded > 0 || failed > 0 {
        engine.flush_shard_index(1);
    }
    let flush_seconds = flush_started.elapsed().as_secs_f64();

    let report = json!({
        "status": "ok",
        "root": root.display().to_string(),
        "index_path": index_path.display().to_string(),
        "base_url": base_url,
        "embedding_model": embedding_model,
        "sessions": sessions.len(),
        "max_nodes": max_nodes,
        "max_new_embeddings": max_new_embeddings,
        "max_new_per_session": max_new_per_session,
        "skip_covered_sessions": skip_covered_sessions,
        "skip_fully_covered_report": skip_fully_covered_report,
        "skip_fully_covered_report_sessions": fully_covered_sessions.len(),
        "shard_load_seconds": shard_load_seconds,
        "total_nodes": total_nodes,
        "missing_embedding_candidates": missing_embedding_candidates,
        "embedded": embedded,
        "empty_text": empty_text,
        "node_l0_text_used": node_l0_text_used,
        "skipped_existing": skipped_existing,
        "skipped_covered_sessions": skipped_covered_sessions,
        "skipped_covered_session_nodes": skipped_covered_session_nodes,
        "skipped_report_sessions": skipped_report_sessions,
        "skipped_report_session_nodes": skipped_report_session_nodes,
        "skipped_report_stale_sessions": skipped_report_stale_sessions,
        "skipped_by_new_cap": skipped_by_new_cap,
        "prefer_events": prefer_events,
        "failed": failed,
        "flush_seconds": flush_seconds,
        "seconds": started.elapsed().as_secs_f64(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("report should serialize")
    );
    write_report_json(&report_json, &report);
}
