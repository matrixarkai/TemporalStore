// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Async batched embedding drainer.
//!
//! Raw-first bulk ingest (`context_batch_ingest` with
//! `MATRIXARK_BACKFILL_RAW_FIRST=1`) and the live extraction path on embed failure
//! both store context nodes WITHOUT vectors and mark them embedding-dirty
//! (`ctx:embdirty:{tenant}:{node}`). This drainer is the background worker that
//! closes the gap: it queries only the pending embedding-dirty set (O(pending), a
//! bounded scan of the in-memory dirty index, NOT a corpus scan), reads each dirty
//! node's event text, batch-embeds it through the SAME provider path live
//! extraction uses (`context_backfill_embeddings`), upserts the `node_l0`
//! embedding under the exact ref-hash the retrieve path reads, then clears the
//! embedding-dirty marker for every node it embedded.
//!
//! It is event-driven-ish: while pending work exists it drains back-to-back with
//! no sleep, so ingest bursts clear quickly; only when the pending set is empty
//! does it sleep for a short fallback interval (an empty dirty set costs one empty
//! query). Gated OFF by default via `MATRIXARK_EMBED_DRAINER`.

use std::time::Duration;

use super::*;
use crate::types::BatchExecuteRequest;

/// A timestamp comfortably in the future but well within the context timeline's
/// valid range (`u64::MAX / CONTEXT_TIMELINE_FANOUT`), used as the query window's
/// upper bound so every pending marker overlaps.
const EMBED_DRAINER_MAX_TIME_MS: u64 = 9_000_000_000_000;

/// Configuration for a single drain pass / the drain loop.
#[derive(Debug, Clone)]
pub struct EmbedDrainerConfig {
    /// Shard to drain (single-shard local/resident engine).
    pub shard_id: ShardId,
    /// Tenant filter for the pending scan: 0 = all tenants on the shard (the
    /// normal setting, since bulk-ingest tenant hashes are per-session), or a
    /// specific tenant to scope the drain.
    pub tenant_hash: u64,
    /// Provider used to compute embeddings (mock/deterministic works offline).
    pub provider: ContextModelProviderConfig,
    /// Embedding batch size (>= 1; the task calls for >= 64 per call).
    pub batch_size: usize,
    /// Max pending nodes processed per pass (bounds one drain's work + memory).
    pub max_nodes_per_pass: usize,
    /// Max events read per node when picking the text to embed.
    pub max_events_per_node: usize,
    /// Idle fallback interval between passes when nothing was pending.
    pub interval: Duration,
}

impl Default for EmbedDrainerConfig {
    fn default() -> Self {
        Self {
            shard_id: 1,
            tenant_hash: 0,
            provider: ContextModelProviderConfig::default(),
            batch_size: 64,
            max_nodes_per_pass: 512,
            max_events_per_node: 4,
            interval: Duration::from_millis(1500),
        }
    }
}

/// Outcome of one drain pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EmbedDrainReport {
    /// Pending nodes returned by the O(pending) query this pass.
    pub pending_scanned: usize,
    /// Nodes whose embedding was upserted successfully.
    pub embedded: usize,
    /// Embedding-dirty nodes cleared (== embedded, minus any clear failures).
    pub cleared: usize,
    /// Dirty nodes skipped because they had no event text to embed. These are
    /// still cleared so the drainer does not spin on them forever.
    pub empty_text: usize,
    /// Nodes whose embedding batch failed (left dirty for a later pass).
    pub failed: usize,
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(default)
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Whether the background drainer is enabled (`MATRIXARK_EMBED_DRAINER`, default off).
pub fn embed_drainer_enabled() -> bool {
    env_bool("MATRIXARK_EMBED_DRAINER", false)
}

/// Build an [`EmbedDrainerConfig`] from environment, given the target shard and a
/// provider (identity/base-url is the caller's concern, mirroring the backfill bin).
pub fn embed_drainer_config_from_env(
    shard_id: ShardId,
    tenant_hash: u64,
    provider: ContextModelProviderConfig,
) -> EmbedDrainerConfig {
    let d = EmbedDrainerConfig::default();
    EmbedDrainerConfig {
        shard_id,
        tenant_hash,
        provider,
        batch_size: env_usize("MATRIXARK_EMBED_DRAINER_BATCH", d.batch_size).max(1),
        max_nodes_per_pass: env_usize(
            "MATRIXARK_EMBED_DRAINER_MAX_NODES_PER_PASS",
            d.max_nodes_per_pass,
        )
        .max(1),
        max_events_per_node: env_usize(
            "MATRIXARK_EMBED_DRAINER_MAX_EVENTS",
            d.max_events_per_node,
        )
        .max(1),
        interval: Duration::from_millis(env_u64(
            "MATRIXARK_EMBED_DRAINER_INTERVAL_MS",
            d.interval.as_millis() as u64,
        )),
    }
}

/// Read the text to embed for a node: the longest stored event text (matches the
/// backfill bin and gives the richest signal).
fn node_embed_text(engine: &TemporalEngine, config: &EmbedDrainerConfig, tenant_hash: u64, node_hash: u64) -> String {
    let response = engine.execute(ExecuteRequest {
        shard_id: config.shard_id,
        command: Command::ContextQueryEvents {
            tenant_hash,
            node_hash,
            start_time_ms: 0,
            end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
            limit: Some(config.max_events_per_node.max(1)),
            max_scan: None,
            current_valid_only: false,
            as_of_ms: 0,
            kinds: Vec::new(),
            statuses: Vec::new(),
            min_confidence: 0.0,
            min_importance: 0.0,
        },
    });
    match response.response {
        CommandResponse::ContextEvents { events, .. } => events
            .into_iter()
            .map(|event| event.text)
            .max_by_key(|text| text.len())
            .unwrap_or_default(),
        _ => String::new(),
    }
}

/// Clear the embedding-dirty marker for a node (write path). Returns true on ok.
fn clear_marker(
    engine: &TemporalEngine,
    config: &EmbedDrainerConfig,
    tenant_hash: u64,
    node_hash: u64,
    event_time_ms: u64,
) -> bool {
    engine
        .execute_durable(ExecuteRequest {
            shard_id: config.shard_id,
            command: Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                event_time_ms: event_time_ms.max(1),
                reason: 0,
                propagate_depth: 0,
                clear: true,
            },
        })
        .status
        .ok
}

/// Run a single drain pass: query pending -> read text -> batch embed -> upsert ->
/// clear. Pure with respect to time (no sleeping); returns what it did so it is
/// unit-testable and O(pending).
pub fn drain_embedding_dirty_once(
    engine: &TemporalEngine,
    config: &EmbedDrainerConfig,
) -> EmbedDrainReport {
    let mut report = EmbedDrainReport::default();

    // 1) O(pending) scan of the embedding-dirty index (all tenants when tenant==0).
    let query = engine.execute(ExecuteRequest {
        shard_id: config.shard_id,
        command: Command::ContextQueryEmbeddingDirty {
            tenant_hash: config.tenant_hash,
            node_hash: 0,
            start_time_ms: 0,
            end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
            limit: Some(config.max_nodes_per_pass.max(1)),
        },
    });
    let (nodes, tenant_hashes) = match query.response {
        CommandResponse::ContextEmbeddingDirtyNodes {
            nodes,
            tenant_hashes,
            ..
        } => (nodes, tenant_hashes),
        _ => return report,
    };
    report.pending_scanned = nodes.len();
    if nodes.is_empty() {
        return report;
    }

    // 2) Collect (tenant, node, event_time, text). Nodes with no text are cleared
    //    immediately so the drainer does not re-scan them every pass.
    struct Pending {
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        text: String,
    }
    let mut pending: Vec<Pending> = Vec::with_capacity(nodes.len());
    for (index, marker) in nodes.iter().enumerate() {
        let tenant_hash = tenant_hashes
            .get(index)
            .copied()
            .filter(|value| *value != 0)
            .unwrap_or(config.tenant_hash);
        if tenant_hash == 0 {
            // No tenant recoverable (shouldn't happen for embedding-dirty entries).
            report.failed += 1;
            continue;
        }
        let text = node_embed_text(engine, config, tenant_hash, marker.node_hash);
        if text.trim().is_empty() {
            report.empty_text += 1;
            if clear_marker(engine, config, tenant_hash, marker.node_hash, marker.last_event_time_ms) {
                report.cleared += 1;
            }
            continue;
        }
        pending.push(Pending {
            tenant_hash,
            node_hash: marker.node_hash,
            // The coalesced entry spans a range of event times; the drain clears against the
            // latest, which is what the mark advanced to.
            event_time_ms: marker.last_event_time_ms,
            text,
        });
    }

    // 3) Batch-embed (>= batch_size per call) and upsert node_l0 vectors, then
    //    clear the nodes for the nodes that embedded successfully.
    let model_hash = context_embedding_model_hash(&config.provider.embedding_model);
    let updated_at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(1)
        .max(1);
    for chunk in pending.chunks(config.batch_size.max(1)) {
        let texts: Vec<&str> = chunk.iter().map(|item| item.text.as_str()).collect();
        let vectors = match context_backfill_embeddings(&config.provider, &texts) {
            Ok(vectors) if vectors.len() == chunk.len() => vectors,
            _ => {
                // Leave this chunk dirty for a later pass.
                report.failed += chunk.len();
                continue;
            }
        };
        let commands: Vec<Command> = chunk
            .iter()
            .zip(vectors.into_iter())
            .map(|(item, vector)| Command::ContextSetNodeEmbedding {
                tenant_hash: item.tenant_hash,
                node_hash: item.node_hash,
                model_hash,
                vector,
                updated_at_ms,
            })
            .collect();
        let response = engine.batch_execute(BatchExecuteRequest {
            shard_id: config.shard_id,
            commands,
        });
        for (item, entry) in chunk.iter().zip(response.responses.iter()) {
            if entry.status.ok {
                report.embedded += 1;
                if clear_marker(
                    engine,
                    config,
                    item.tenant_hash,
                    item.node_hash,
                    item.event_time_ms,
                ) {
                    report.cleared += 1;
                }
            } else {
                report.failed += 1;
            }
        }
    }
    report
}

/// Run the drain loop until `should_stop` returns true. Drains back-to-back while
/// pending work exists; sleeps `config.interval` only when a pass found nothing.
pub fn run_embed_drainer_loop<F: Fn() -> bool>(
    engine: &TemporalEngine,
    config: &EmbedDrainerConfig,
    should_stop: F,
) {
    while !should_stop() {
        let report = drain_embedding_dirty_once(engine, config);
        // If the pass did real work and may have more queued, loop immediately;
        // only back off when the pending set was empty this pass.
        if report.pending_scanned == 0 {
            std::thread::sleep(config.interval);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drainer_engine() -> TemporalEngine {
        let dir = tempfile::tempdir().unwrap();
        let engine = TemporalEngine::with_local_dirs(
            1024 * 1024,
            dir.path().join("cache"),
            dir.path().join("pages"),
            dir.path().join("indexes"),
        );
        engine.load_shard(1);
        // Leak the tempdir so the store outlives the test body (engine holds paths).
        std::mem::forget(dir);
        engine
    }

    fn seed_raw_node(engine: &TemporalEngine, tenant_hash: u64, node_hash: u64, text: &str) {
        let ok = engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode {
                    tenant_hash,
                    node: Box::new(ContextNode {
                        node_hash,
                        parent_hash: 0,
                        kind: 0,
                        canonical_name: format!("node-{node_hash}"),
                        l0: text.to_string(),
                        status: 1,
                        last_event_time_ms: 1_000,
                        l1_ref: String::new(),
                        raw_metadata_ref: format!("src://{node_hash}"),
                        vector: Vec::new(),
                        embedding_model_hash: 0,
                        embedding_updated_at_ms: 0,
                        summary_vector: Vec::new(),
                        summary_vector_valid_from_ms: 0,
                        summary_vector_model_hash: 0,
                    }),
                },
            })
            .status
            .ok;
        assert!(ok);
        let ok = engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextWriteEvent {
                    tenant_hash,
                    node_hash,
                    event: Box::new(ContextEvent {
                        event_id_hash: node_hash.wrapping_mul(7).max(1),
                        event_time_ms: 1_000,
                        ingestion_time_ms: 1_000,
                        kind: 0,
                        event_type: 1,
                        actor_hash: 1,
                        status: 1,
                        valid_until_ms: 0,
                        confidence: 1.0,
                        importance: 1.0,
                        text: text.to_string(),
                        source_ref: String::new(),
                        related_node_hashes: Vec::new(),
                        compact_attrs: Vec::new(),
                        vector: Vec::new(),
                    }),
                    first_write_only: false,
                    cold_storage: false,
                },
            })
            .status
            .ok;
        assert!(ok);
        // Mark it embedding-dirty (as raw-first bulk ingest would).
        let ok = engine
            .execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextMarkEmbeddingDirty {
                    tenant_hash,
                    node_hash,
                event_time_ms: 1_000,
                reason: 2,
                propagate_depth: 0,
                    clear: false,
                },
            })
            .status
            .ok;
        assert!(ok);
    }

    fn count_pending(engine: &TemporalEngine) -> usize {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEmbeddingDirty {
                tenant_hash: 0,
                node_hash: 0,
                start_time_ms: 0,
                end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
                limit: Some(1000),
            },
        });
        match response.response {
            CommandResponse::ContextEmbeddingDirtyNodes { nodes, .. } => nodes.len(),
            _ => 0,
        }
    }

    /// The vector as carried by the node itself.
    fn inline_vector(engine: &TemporalEngine, tenant_hash: u64, node_hash: u64) -> Vec<f32> {
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextGetNode {
                tenant_hash,
                node_hash,
            },
        });
        match response.response {
            CommandResponse::ContextNode { node, .. } => {
                node.map(|node| node.vector).unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    #[test]
    fn embedding_dirty_marker_mark_query_clear_round_trip() {
        let engine = drainer_engine();
        let tenant_hash = 7;
        let node_hash = 42;
        // Independent of summary-dirty: marking embedding-dirty does not create a
        // summary-dirty marker.
        engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                event_time_ms: 500,
                reason: 2,
                propagate_depth: 0,
                clear: false,
            },
        });
        // Per-node query surfaces exactly one marker.
        let per_node = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEmbeddingDirty {
                tenant_hash,
                node_hash,
                start_time_ms: 0,
                end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
                limit: Some(10),
            },
        });
        match per_node.response {
            CommandResponse::ContextEmbeddingDirtyNodes { nodes, .. } => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(nodes[0].node_hash, node_hash);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Summary-dirty is untouched (independence).
        let summary = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQuerySummaryDirty {
                tenant_hash,
                node_hash,
                start_time_ms: 0,
                end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
                limit: Some(10),
            },
        });
        match summary.response {
            CommandResponse::ContextSummaryDirtyNodes { nodes, .. } => {
                assert!(nodes.is_empty(), "embedding-dirty must not set summary-dirty");
            }
            other => panic!("unexpected: {other:?}"),
        }
        // All-pending scan (tenant 0) returns it with its tenant hash.
        let all = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextQueryEmbeddingDirty {
                tenant_hash: 0,
                node_hash: 0,
                start_time_ms: 0,
                end_time_ms: EMBED_DRAINER_MAX_TIME_MS,
                limit: Some(10),
            },
        });
        match all.response {
            CommandResponse::ContextEmbeddingDirtyNodes {
                nodes,
                tenant_hashes,
                ..
            } => {
                assert_eq!(nodes.len(), 1);
                assert_eq!(tenant_hashes, vec![tenant_hash]);
            }
            other => panic!("unexpected: {other:?}"),
        }
        // Clear removes it.
        engine.execute_durable(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextMarkEmbeddingDirty {
                tenant_hash,
                node_hash,
                event_time_ms: 500,
                reason: 0,
                propagate_depth: 0,
                clear: true,
            },
        });
        assert_eq!(count_pending(&engine), 0);
    }

    #[test]
    fn drainer_embeds_all_pending_and_clears_markers() {
        let engine = drainer_engine();
        // Two tenants, several nodes each: exercises the all-tenant pending scan.
        let nodes: [(u64, u64, &str); 5] = [
            (100, 1, "checkout payment failed with a risk score spike"),
            (100, 2, "deployment rollout paused after latency regression"),
            (100, 3, "user updated notification preference to email"),
            (200, 4, "database migration added a new index column"),
            (200, 5, "incident timeline: outage then recovery"),
        ];
        for (tenant, node, text) in nodes {
            seed_raw_node(&engine, tenant, node, text);
        }
        assert_eq!(count_pending(&engine), 5);

        let config = EmbedDrainerConfig {
            batch_size: 64,
            ..EmbedDrainerConfig::default()
        };
        let report = drain_embedding_dirty_once(&engine, &config);
        assert_eq!(report.pending_scanned, 5);
        assert_eq!(report.embedded, 5);
        assert_eq!(report.cleared, 5);
        assert_eq!(report.failed, 0);

        // Every node now carries its vector -- the node record is the only place it lives.
        for (tenant, node, _) in nodes {
            let inline = inline_vector(&engine, tenant, node);
            assert!(!inline.is_empty(), "node {node} has no vector of its own");
        }
        assert_eq!(count_pending(&engine), 0);

        // A second pass is a no-op (empty pending set) -> O(pending) drains to zero.
        let report2 = drain_embedding_dirty_once(&engine, &config);
        assert_eq!(report2.pending_scanned, 0);
        assert_eq!(report2.embedded, 0);
    }

    #[test]
    fn drainer_pass_is_bounded_by_pending_not_corpus() {
        let engine = drainer_engine();
        // Seed nodes but only mark a couple embedding-dirty: the pass must scan
        // only the pending set, not every node.
        for node in 1..=6u64 {
            // Node + event without marking dirty.
            engine.execute_durable(ExecuteRequest {
                shard_id: 1,
                command: Command::ContextUpsertNode {
                    tenant_hash: 9,
                    node: Box::new(ContextNode {
                        node_hash: node,
                        parent_hash: 0,
                        kind: 0,
                        canonical_name: format!("n{node}"),
                        l0: format!("body text for node {node}"),
                        status: 1,
                        last_event_time_ms: 1_000,
                        l1_ref: String::new(),
                        raw_metadata_ref: String::new(),
                        vector: Vec::new(),
                        embedding_model_hash: 0,
                        embedding_updated_at_ms: 0,
                        summary_vector: Vec::new(),
                        summary_vector_valid_from_ms: 0,
                        summary_vector_model_hash: 0,
                    }),
                },
            });
        }
        // Mark just two of them dirty (with events).
        seed_raw_node(&engine, 9, 100, "only this one is pending");
        seed_raw_node(&engine, 9, 101, "and this one too");
        assert_eq!(count_pending(&engine), 2);

        let report = drain_embedding_dirty_once(&engine, &EmbedDrainerConfig::default());
        assert_eq!(report.pending_scanned, 2, "must scan only pending, not corpus");
        assert_eq!(report.embedded, 2);
        assert_eq!(count_pending(&engine), 0);
    }
}
