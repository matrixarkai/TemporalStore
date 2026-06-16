use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use temporalstore_rust::{
    extract_context, inject_context, retrieve_context, ContextExtractRequest, ContextInjectRequest,
    ContextModelProviderConfig, ContextRetrieveRequest, ContextSourceKind, ContextTier,
    TemporalEngine,
};

#[derive(Debug, Serialize)]
struct ContextWorkflowHarnessSummary {
    root: String,
    extraction_ok: bool,
    retrieve_block_count: usize,
    selected_block_count: usize,
    blocked_block_count: usize,
    audit_selected_ref_count: usize,
    injected_prompt_contains_context: bool,
    provider_name: String,
}

fn main() {
    let root = parse_root();
    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        root.join("cache"),
        root.join("pages"),
        root.join("indexes"),
    );
    engine.load_shard(1);

    let extract = extract_context(
        &engine,
        ContextExtractRequest {
            shard_id: 1,
            tenant_hash: 20260616,
            source_kind: ContextSourceKind::Incident,
            source_id: "mock-incident-1".to_string(),
            title: "Checkout risk incident".to_string(),
            body: "Customer checkout failed. Payment risk score spiked. The proxy retried safely and support asked for root cause.".to_string(),
            timestamp_ms: 1_000,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(extract.status.ok, "{:?}", extract.status);

    let retrieve_request = ContextRetrieveRequest {
        shard_id: 1,
        tenant_hash: 20260616,
        node_hashes: vec![extract.node.node_hash],
        query: "checkout".to_string(),
        start_time_ms: 0,
        end_time_ms: 2_000,
        max_events: 8,
        min_confidence: 0.0,
        min_importance: 0.0,
        tiers: vec![ContextTier::L0, ContextTier::L1, ContextTier::L2],
    };
    let retrieve = retrieve_context(&engine, retrieve_request.clone());
    assert!(retrieve.status.ok, "{:?}", retrieve.status);
    assert!(retrieve.blocks.len() >= 2);

    let inject = inject_context(
        &engine,
        ContextInjectRequest {
            retrieve: retrieve_request,
            prompt: "Summarize the incident and explain what context matters.".to_string(),
            session_hash: 99,
            query_id: "context-harness-query".to_string(),
            max_prompt_tokens: 128,
            provider: ContextModelProviderConfig::default(),
        },
    );
    assert!(inject.status.ok, "{:?}", inject.status);
    assert!(inject.injected_prompt.contains("<context>"));
    assert!(!inject.audit.selected_refs.is_empty());

    println!(
        "{}",
        serde_json::to_string_pretty(&ContextWorkflowHarnessSummary {
            root: root.display().to_string(),
            extraction_ok: extract.status.ok,
            retrieve_block_count: retrieve.blocks.len(),
            selected_block_count: inject.selected_blocks.len(),
            blocked_block_count: inject.blocked_blocks.len(),
            audit_selected_ref_count: inject.audit.selected_refs.len(),
            injected_prompt_contains_context: inject.injected_prompt.contains("<context>"),
            provider_name: inject.provider.provider_name,
        })
        .expect("context workflow summary should serialize")
    );
}

fn parse_root() -> PathBuf {
    let mut root =
        std::env::temp_dir().join(format!("temporalstore-context-workflow-{}", now_ms()));
    let mut args = std::env::args().skip(1);
    while let Some(key) = args.next() {
        let Some(value) = args.next() else {
            usage_and_exit();
        };
        match key.as_str() {
            "--root" => root = PathBuf::from(value),
            _ => usage_and_exit(),
        }
    }
    root
}

fn usage_and_exit() -> ! {
    eprintln!("usage: context_workflow_harness [--root <path>]");
    std::process::exit(2);
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default()
}
