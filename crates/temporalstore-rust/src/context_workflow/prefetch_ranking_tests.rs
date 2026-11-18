// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn prefetched_literal_match_is_scored_before_the_summary_limit() {
    let engine = test_engine();
    let tenant_hash = 918;
    let query = "quasar invoice 00004095";
    let vector = deterministic_context_embedding(&default_embedding_model(), query);
    let mut target_vector = vector.clone();
    target_vector[0] += 0.05;
    assert!(
        context_embedding_similarity_micros(&vector, &target_vector)
            < context_embedding_similarity_micros(&vector, &vector)
    );
    for (node_hash, text, embedding) in [
        (11, "An unrelated archive document.", vector),
        (
            22,
            "quasar invoice 00004095 is the requested source.",
            target_vector,
        ),
    ] {
        upsert_raw_context_node(&engine, tenant_hash, node_hash, text, 6_000);
        let response = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ContextSetNodeEmbedding {
                tenant_hash,
                node_hash,
                model_hash: 0,
                vector: embedding,
                updated_at_ms: 6_000,
            },
        });
        assert!(response.status.ok, "{:?}", response.status);
    }
    for include_debug in [false, true] {
        for node_hashes in [vec![11, 22], vec![22, 11]] {
            let report = retrieve_context_from_nodes(
                &engine,
                ContextRetrieveRequest {
                    shard_id: 1,
                    tenant_hash,
                    node_hashes,
                    query: query.into(),
                    start_time_ms: 0,
                    end_time_ms: 10_000,
                    max_events: 1,
                    min_confidence: 0.0,
                    min_importance: 0.0,
                    tiers: vec![ContextTier::L2],
                    max_summary_nodes: 1,
                    max_event_nodes: 1,
                    prefer_current_agent: false,
                    current_agent_scope_key: String::new(),
                    provider: ContextModelProviderConfig::default(),
                    include_debug,
                },
            );
            assert!(report.status.ok, "{:?}", report.status);
            assert_eq!(report.node_count, 1);
            assert!(report.event_count <= 1);
            assert!(
                report.blocks.iter().any(|block| block.node_hash == 22),
                "literal candidate was dropped before reranking: {:?}",
                report.blocks
            );
        }
    }
}
