// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn extract(engine: &TemporalEngine, source: &str) -> ContextExtractReport {
    extract_context(
        engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 91,
            source_kind: ContextSourceKind::Chat,
            source_id: source.into(),
            title: "memory".into(),
            body: "The deployment region is west and the preferred language is Rust.".into(),
            timestamp_ms: 1000,
            provider: ContextModelProviderConfig::default(),
        },
    )
}

fn request(nodes: Vec<u64>) -> ContextRetrieveRequest {
    serde_json::from_value(serde_json::json!({
        "shard_id": 1, "tenant_hash": 91, "node_hashes": nodes,
        "query": "preferred language deployment region", "start_time_ms": 0,
        "end_time_ms": 2000, "max_events": 8, "include_debug": true
    }))
    .unwrap()
}

#[test]
fn strict_empty_candidates_do_not_expand_to_the_tenant() {
    let engine = test_engine();
    assert!(extract(&engine, "outside").status.ok);
    let regular = retrieve_context(&engine, request(Vec::new()));
    assert!(regular.status.ok);
    assert!(!regular.blocks.is_empty());
    let scoped = retrieve_context_from_nodes(&engine, request(Vec::new()));
    assert!(scoped.status.ok);
    assert!(scoped.blocks.is_empty());
    assert_eq!(scoped.fanout_plan.namespace_node_candidates, 0);
    let mut offline = request(Vec::new());
    offline.provider.provider_kind = ContextProviderKind::Ollama;
    offline.provider.mock_mode = false;
    offline.provider.base_url = "invalid-url".into();
    offline.provider.fallback_provider = None;
    offline.provider.max_retries = 0;
    assert!(retrieve_context_from_nodes(&engine, offline).status.ok);
}

#[test]
fn strict_large_candidates_do_not_reintroduce_other_hot_nodes() {
    let engine = test_engine();
    let allowed = extract(&engine, "allowed");
    let outside = extract(&engine, "outside");
    assert!(allowed.status.ok && outside.status.ok);
    assert_eq!(
        allowed.node.node_hash,
        context_source_node_hash(91, ContextSourceKind::Chat, "allowed")
    );
    let mut nodes = (1..=CONTEXT_SCOPE_NODE_CANDIDATE_LIMIT as u64).collect::<Vec<_>>();
    nodes.push(allowed.node.node_hash);
    let regular = retrieve_context(&engine, request(nodes.clone()));
    assert!(regular
        .blocks
        .iter()
        .any(|block| block.node_hash == outside.node.node_hash));
    for debug in [false, true] {
        let mut input = request(nodes.clone());
        input.include_debug = debug;
        let scoped = retrieve_context_from_nodes(&engine, input);
        assert!(scoped.status.ok);
        assert!(!scoped.blocks.is_empty(), "{scoped:?}");
        assert!(scoped
            .blocks
            .iter()
            .all(|block| block.node_hash == allowed.node.node_hash));
        if debug {
            assert_eq!(
                scoped.fanout_plan.namespace_node_candidates,
                CONTEXT_SCOPE_NODE_CANDIDATE_LIMIT + 1
            );
        }
    }
}
